"""
CcwsGateAccessPipeline — raw GateAccessData SOAP → esa_middleware.ccws_gate_access.

Access codes are Fernet-encrypted with the same crypto as the legacy
scheduler pipeline (common.gate_access_crypto). Natural key: (location_code, unit_id).
"""

import logging
import time
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.config import get_engine
from sync_service.pipelines._ccws_utils import (
    NAMESPACE, build_soap_client, resolve_site_codes,
    to_int, to_bool, build_upsert_sql, parallel_fetch,
)

logger = logging.getLogger(__name__)
SOAP_ACTION = f"{NAMESPACE}/GateAccessData"

_LOC_TO_SITE_TTL = 3600  # siteinfo mappings rarely change
_loc_to_site_cache: Dict[str, int] = {}
_loc_to_site_fetched_at: float = 0.0


def _build_loc_to_site_map() -> Dict[str, int]:
    """SiteCode -> SiteID from esa_middleware.mw_siteinfo (cached 1h)."""
    global _loc_to_site_cache, _loc_to_site_fetched_at
    now = time.monotonic()
    if _loc_to_site_cache and (now - _loc_to_site_fetched_at) < _LOC_TO_SITE_TTL:
        return _loc_to_site_cache
    with get_engine('middleware').connect() as conn:
        rows = conn.execute(text(
            'SELECT "SiteCode", "SiteID" FROM mw_siteinfo '
            'WHERE "SiteCode" IS NOT NULL'
        )).fetchall()
    _loc_to_site_cache = {r[0]: r[1] for r in rows}
    _loc_to_site_fetched_at = now
    return _loc_to_site_cache


class CcwsGateAccessPipeline(BasePipeline):

    def _make_fetcher(self, soap, crypto, loc_to_site, successful_sites, failed_sites):
        def fetch(sc: str) -> List[Dict[str, Any]]:
            site_id = loc_to_site.get(sc.strip())
            if site_id is None:
                self.log.warning(f"{sc}: no SiteID mapping, skipped")
                failed_sites.add(sc.strip())
                return []
            try:
                results = soap.call(
                    operation="GateAccessData",
                    parameters={
                        "sLocationCode": sc.strip(),
                        "iMinutesSinceLastUpdate": "0",
                    },
                    soap_action=SOAP_ACTION,
                    namespace=NAMESPACE,
                    result_tag="Table",
                )
            except Exception as e:
                self.log.error(f"SOAP fetch failed for {sc}: {e}")
                failed_sites.add(sc.strip())
                return []
            # Mark site as successfully fetched (even if 0 rows came back) —
            # tombstone pass below uses this to clear ghost rows for sites
            # that delete gate-access enrollments on move-out.
            successful_sites.add(sc.strip())

            out: List[Dict[str, Any]] = []
            for r in (results or []):
                unit_id = to_int(r.get('UnitID'))
                if not unit_id:
                    continue
                ac = r.get('sAccessCode') or ''
                ac2 = r.get('sAccessCode2') or ''
                out.append({
                    'location_code': sc.strip(),
                    'site_id': site_id,
                    'unit_id': unit_id,
                    'unit_name': r.get('sUnitName') or '',
                    'is_rented': bool(to_bool(r.get('bRented'))),
                    'access_code_enc': crypto.encrypt(ac) if ac else None,
                    'access_code2_enc': crypto.encrypt(ac2) if ac2 else None,
                    'is_gate_locked': bool(to_bool(r.get('bGateLocked'))),
                    'is_overlocked': bool(to_bool(r.get('bOverlocked'))),
                    'keypad_zone': to_int(r.get('iKeypadZ')) or 0,
                })
            return out
        return fetch

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        from common.gate_access_crypto import get_gate_crypto

        site_codes = resolve_site_codes(scope)
        if not site_codes:
            return RunResult(status='failed', scope=scope, error='No site_codes resolved')

        crypto = get_gate_crypto()
        loc_to_site = _build_loc_to_site_map()
        soap = build_soap_client()
        try:
            successful_sites: set = set()
            failed_sites: set = set()
            rows, per_site = parallel_fetch(
                self._make_fetcher(
                    soap, crypto, loc_to_site, successful_sites, failed_sites,
                ),
                site_codes,
            )

            # Dedup by (location_code, unit_id)
            seen, deduped = set(), []
            for r in rows:
                k = (r['location_code'], r['unit_id'])
                if k not in seen:
                    seen.add(k); deduped.append(r)

            # Group present (location_code, unit_id) keys by site for tombstone
            present_per_site: Dict[str, set] = {}
            for r in deduped:
                present_per_site.setdefault(r['location_code'], set()).add(r['unit_id'])

            tombstoned = 0
            tombstone_skipped: List[str] = []
            cols = list(deduped[0].keys()) if deduped else []
            with get_engine('middleware').begin() as conn:
                if deduped:
                    sql = text(build_upsert_sql(
                        'ccws_gate_access', cols,
                        conflict_cols=['location_code', 'unit_id'],
                    ))
                    conn.execute(sql, deduped)

                # Tombstone: for each successfully-fetched site, delete DB
                # rows whose unit_id is no longer in the SOAP response. This
                # clears ghost enrollments at sites where SiteLink deletes
                # gate-access records on move-out (e.g. L031).
                #
                # Safety: cap each site's delete to MAX_TOMBSTONE_ROWS. If the
                # would-be delete count exceeds the cap, skip and warn — this
                # protects busy sites (e.g. L001 with ~1400 enrollments) from
                # a transient empty SOAP response wiping the table. Sparse
                # sites (e.g. L031 with 4 ghost rows) clean up normally.
                MAX_TOMBSTONE_ROWS = 50
                for sc in successful_sites:
                    present_uids = present_per_site.get(sc, set())
                    if present_uids:
                        existing = conn.execute(
                            text(
                                'SELECT COUNT(*) FROM ccws_gate_access '
                                'WHERE location_code = :loc '
                                'AND unit_id <> ALL(:uids)'
                            ),
                            {'loc': sc, 'uids': list(present_uids)},
                        ).scalar() or 0
                    else:
                        existing = conn.execute(
                            text(
                                'SELECT COUNT(*) FROM ccws_gate_access '
                                'WHERE location_code = :loc'
                            ),
                            {'loc': sc},
                        ).scalar() or 0

                    if existing > MAX_TOMBSTONE_ROWS:
                        self.log.warning(
                            f"{sc}: tombstone skipped — would delete {existing} "
                            f"rows (>{MAX_TOMBSTONE_ROWS} cap); likely transient "
                            f"empty SOAP response"
                        )
                        tombstone_skipped.append(sc)
                        continue

                    if present_uids:
                        result = conn.execute(
                            text(
                                'DELETE FROM ccws_gate_access '
                                'WHERE location_code = :loc '
                                'AND unit_id <> ALL(:uids)'
                            ),
                            {'loc': sc, 'uids': list(present_uids)},
                        )
                    else:
                        result = conn.execute(
                            text('DELETE FROM ccws_gate_access WHERE location_code = :loc'),
                            {'loc': sc},
                        )
                    tombstoned += (result.rowcount or 0)

            return RunResult(status='refreshed', records=len(deduped), scope=scope,
                             metadata={'per_site_counts': per_site,
                                       'sites_queried': len(site_codes),
                                       'tombstoned_rows': tombstoned,
                                       'tombstone_skipped_sites': tombstone_skipped,
                                       'fetch_failed_sites': sorted(failed_sites),
                                       'fetch_failed_count': len(failed_sites)})
        finally:
            try: soap.close()
            except Exception: pass
