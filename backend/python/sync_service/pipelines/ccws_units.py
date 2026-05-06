"""
CcwsUnitsPipeline — full unit catalog from UnitsInformation_v3 → esa_middleware.ccws_units.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.config import get_engine
from sync_service.pipelines._ccws_utils import (
    NAMESPACE, build_soap_client, resolve_site_codes,
    to_int, to_decimal, to_bool, to_datetime, to_str,
    build_upsert_sql, parallel_fetch,
)

logger = logging.getLogger(__name__)
SOAP_ACTION = f"{NAMESPACE}/UnitsInformation_v3"


def _transform(r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    site_id = to_int(r.get('SiteID'))
    unit_id = to_int(r.get('UnitID'))
    if site_id is None or unit_id is None:
        return None
    return {
        'SiteID': site_id,
        'UnitID': unit_id,
        'UnitTypeID': to_int(r.get('UnitTypeID')),
        'sLocationCode': to_str(r.get('sLocationCode'), 20),
        'sUnitName': to_str(r.get('sUnitName'), 100),
        'sTypeName': to_str(r.get('sTypeName'), 100),
        'sUnitNote': to_str(r.get('sUnitNote'), 500),
        'sUnitDesc': to_str(r.get('sUnitDesc'), 500),
        'dcWidth': to_decimal(r.get('dcWidth')),
        'dcLength': to_decimal(r.get('dcLength')),
        'iFloor': to_int(r.get('iFloor')),
        'dcMapTheta': to_decimal(r.get('dcMapTheta')),
        'bMapReversWL': to_bool(r.get('bMapReversWL')),
        'iEntryLoc': to_int(r.get('iEntryLoc')),
        'iDoorType': to_int(r.get('iDoorType')),
        'iADA': to_int(r.get('iADA')),
        'bClimate': to_bool(r.get('bClimate')),
        'bPower': to_bool(r.get('bPower')),
        'bInside': to_bool(r.get('bInside')),
        'bAlarm': to_bool(r.get('bAlarm')),
        'bRentable': to_bool(r.get('bRentable')),
        'bMobile': to_bool(r.get('bMobile')),
        'bServiceRequired': to_bool(r.get('bServiceRequired')),
        'bExcludeFromWebsite': to_bool(r.get('bExcludeFromWebsite')),
        'bRented': to_bool(r.get('bRented')),
        'bWaitingListReserved': to_bool(r.get('bWaitingListReserved')),
        'bCorporate': to_bool(r.get('bCorporate')),
        'iDaysVacant': to_int(r.get('iDaysVacant')),
        'iDaysRented': to_int(r.get('iDaysRented')),
        'dMovedIn': to_datetime(r.get('dMovedIn')),
        'iDefLeaseNum': to_int(r.get('iDefLeaseNum')),
        'DefaultCoverageID': to_int(r.get('DefaultCoverageID')),
        'dcStdRate': to_decimal(r.get('dcStdRate')),
        'dcWebRate': to_decimal(r.get('dcWebRate')),
        'dcPushRate': to_decimal(r.get('dcPushRate')),
        'dcPushRate_NotRounded': to_decimal(r.get('dcPushRate_NotRounded')),
        'dcBoardRate': to_decimal(r.get('dcBoardRate')),
        'dcPreferredRate': to_decimal(r.get('dcPreferredRate')),
        'dcStdWeeklyRate': to_decimal(r.get('dcStdWeeklyRate')),
        'dcStdSecDep': to_decimal(r.get('dcStdSecDep')),
        'dcRM_RoundTo': to_decimal(r.get('dcRM_RoundTo')),
        'dcTax1Rate': to_decimal(r.get('dcTax1Rate')),
        'dcTax2Rate': to_decimal(r.get('dcTax2Rate')),
        'iPreferredChannelType': to_int(r.get('iPreferredChannelType')),
        'bPreferredIsPushRate': to_bool(r.get('bPreferredIsPushRate')),
    }


class CcwsUnitsPipeline(BasePipeline):

    def _make_fetcher(self, soap):
        def fetch(sc: str) -> List[Dict[str, Any]]:
            try:
                results = soap.call(
                    operation="UnitsInformation_v3",
                    parameters={"sLocationCode": sc.strip()},
                    soap_action=SOAP_ACTION,
                    namespace=NAMESPACE,
                    result_tag="Table",
                )
            except Exception as e:
                self.log.error(f"SOAP fetch failed for {sc}: {e}")
                return []
            out: List[Dict[str, Any]] = []
            for r in (results or []):
                t = _transform(r)
                if t:
                    out.append(t)
            return out
        return fetch

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        site_codes = resolve_site_codes(scope)
        if not site_codes:
            return RunResult(status='failed', scope=scope, error='No site_codes resolved')

        soap = build_soap_client()
        try:
            rows, per_site = parallel_fetch(self._make_fetcher(soap), site_codes)

            # Dedup by (SiteID, UnitID)
            seen, deduped = set(), []
            for r in rows:
                k = (r['SiteID'], r['UnitID'])
                if k not in seen:
                    seen.add(k); deduped.append(r)

            if not deduped:
                return RunResult(status='refreshed', records=0, scope=scope,
                                 metadata={'per_site_counts': per_site})

            cols = list(deduped[0].keys())
            sql = text(build_upsert_sql(
                'ccws_units', cols, conflict_cols=['SiteID', 'UnitID'],
            ))
            with get_engine('middleware').begin() as conn:
                conn.execute(sql, deduped)

            return RunResult(status='refreshed', records=len(deduped), scope=scope,
                             metadata={'per_site_counts': per_site,
                                       'sites_queried': len(site_codes)})
        finally:
            try: soap.close()
            except Exception: pass
