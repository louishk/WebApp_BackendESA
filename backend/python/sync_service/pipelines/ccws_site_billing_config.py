"""
CcwsSiteBillingConfigPipeline — derive per-site proration config by calling
MoveInCostRetrieveWithDiscount_v4 on an available unit. Writes to
esa_middleware.ccws_site_billing_config, preserving manual overrides
(rows with overridden_by NOT NULL are skipped).
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.config import get_engine
from sync_service.pipelines._ccws_utils import (
    NAMESPACE, build_soap_client, resolve_site_codes,
    to_int, to_bool, parallel_fetch,
)

logger = logging.getLogger(__name__)


class CcwsSiteBillingConfigPipeline(BasePipeline):

    def _soap_call(self, client, op, params, result_tag="RT"):
        return client.call(
            operation=op, parameters=params,
            soap_action=f"{NAMESPACE}/{op}",
            namespace=NAMESPACE, result_tag=result_tag,
        )

    def _fetch_site(self, client, sc: str) -> Optional[Tuple[Optional[int], Dict[str, Any]]]:
        try:
            units = self._soap_call(client,
                "UnitsInformationAvailableUnitsOnly_v2",
                {"sLocationCode": sc}, result_tag="Table")
            if not units:
                return None
            unit = units[0]
            unit_id = unit.get("UnitID")
            if not unit_id:
                return None
            move_in = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%dT00:00:00")
            cost = self._soap_call(client,
                "MoveInCostRetrieveWithDiscount_v4", {
                    "sLocationCode": sc, "iUnitID": str(unit_id),
                    "dMoveInDate": move_in,
                    "InsuranceCoverageID": "0", "ConcessionPlanID": "0",
                    "iPromoGlobalNum": "0", "ChannelType": "0",
                    "bApplyInsuranceCredit": "false",
                    "sCreditCardNum": "",
                }, result_tag="Table")
            if not cost:
                return None
            row = cost[0]
            site_id = to_int(row.get("SiteID")) or to_int(unit.get("SiteID"))
            return site_id, {
                "b_anniv_date_leasing": bool(to_bool(row.get("bAnnivDateLeasing"))),
                "i_day_strt_prorating": to_int(row.get("iDayStrtProrating")) or 1,
                "i_day_strt_prorate_plus_next": to_int(row.get("iDayStrtProratePlusNext")) or 17,
            }
        except Exception as e:
            self.log.error(f"SOAP fetch failed for {sc}: {e}")
            return None

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        site_codes = resolve_site_codes(scope)
        if not site_codes:
            return RunResult(status='failed', scope=scope, error='No site_codes resolved')

        soap = build_soap_client()
        updated: List[str] = []
        skipped_overrides: List[str] = []
        failed: List[str] = []
        engine = get_engine('middleware')

        # Preload overridden sites
        with engine.connect() as conn:
            overrides = {
                r[0] for r in conn.execute(text(
                    "SELECT \"SiteCode\" FROM ccws_site_billing_config "
                    "WHERE overridden_by IS NOT NULL"
                )).all()
            }

        try:
            # Filter out overridden sites before fetching
            to_fetch = []
            for sc in site_codes:
                sc = sc.strip()
                if sc in overrides:
                    skipped_overrides.append(sc)
                else:
                    to_fetch.append(sc)

            # Fetch all sites in parallel; each fetch does 2 sequential SOAP calls
            def fetch_one(sc: str):
                res = self._fetch_site(soap, sc)
                if res is None:
                    return [{'SiteCode': sc, '_failed': True}]
                site_id, cfg = res
                return [{'SiteCode': sc, 'SiteID': site_id, **cfg}]

            rows, _ = parallel_fetch(fetch_one, to_fetch, max_workers=6)

            # Partition successes + prepare bulk upsert
            success_rows = []
            for r in rows:
                if r.get('_failed'):
                    failed.append(r['SiteCode'])
                else:
                    success_rows.append(r)
                    updated.append(r['SiteCode'])

            if success_rows:
                now = datetime.now(timezone.utc)
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO ccws_site_billing_config (
                            "SiteCode", "SiteID", b_anniv_date_leasing,
                            i_day_strt_prorating, i_day_strt_prorate_plus_next,
                            synced_from_soap_at, created_at, updated_at
                        ) VALUES (
                            :SiteCode, :SiteID, :b_anniv_date_leasing,
                            :i_day_strt_prorating, :i_day_strt_prorate_plus_next,
                            :now, :now, :now
                        )
                        ON CONFLICT ("SiteCode")
                        DO UPDATE SET
                            "SiteID" = EXCLUDED."SiteID",
                            b_anniv_date_leasing = EXCLUDED.b_anniv_date_leasing,
                            i_day_strt_prorating = EXCLUDED.i_day_strt_prorating,
                            i_day_strt_prorate_plus_next = EXCLUDED.i_day_strt_prorate_plus_next,
                            synced_from_soap_at = :now,
                            updated_at = :now
                        WHERE ccws_site_billing_config.overridden_by IS NULL
                    """), [
                        {**r, 'now': now} for r in success_rows
                    ])
        finally:
            try: soap.close()
            except Exception: pass

        return RunResult(
            status='refreshed',
            records=len(updated),
            scope=scope,
            metadata={
                'updated_sites': updated,
                'skipped_overrides': skipped_overrides,
                'failed_sites': failed,
                'sites_queried': len(site_codes),
            },
        )
