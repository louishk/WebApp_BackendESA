"""
CcwsReservationsPipeline — raw ReservationList_v3 → esa_middleware.ccws_reservations.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.config import get_engine
from sync_service.pipelines._ccws_utils import (
    NAMESPACE, build_soap_client, resolve_site_codes,
    to_int, to_decimal, to_datetime, to_str, build_upsert_sql,
    parallel_fetch,
)

logger = logging.getLogger(__name__)

SOAP_ACTION = "http://tempuri.org/CallCenterWs/CallCenterWs/ReservationList_v3"


def _transform(record: Dict[str, Any], site_code: str) -> Optional[Dict[str, Any]]:
    waiting_id = to_int(record.get('WaitingID'))
    effective_site = (record.get('sLocationCode') or site_code or '').strip()
    if not waiting_id or not effective_site:
        return None
    return {
        'SiteCode': effective_site,
        'WaitingID': waiting_id,
        'iGlobalWaitingNum': to_int(record.get('iGlobalWaitingNum')),
        'TenantID': to_int(record.get('TenantID')),
        'UnitID': to_int(record.get('UnitID')),
        'sFName': to_str(record.get('sFName'), 100),
        'sLName': to_str(record.get('sLName'), 100),
        'sEmail': to_str(record.get('sEmail'), 255),
        'sPhone': to_str(record.get('sPhone'), 50),
        'sMobile': to_str(record.get('sMobile'), 50),
        'dcRate_Quoted': to_decimal(record.get('dcRate_Quoted')),
        'ConcessionID': to_int(record.get('ConcessionID')),
        'iInquiryType': to_int(record.get('iInquiryType')),
        'QTRentalTypeID': to_int(record.get('QTRentalTypeID')),
        'dcPaidReserveFee': to_decimal(record.get('dcPaidReserveFee')),
        'iReserveFeeReceiptID': to_int(record.get('iReserveFeeReceiptID')),
        'iWaitingStatus': to_int(record.get('iWaitingStatus')),
        'dNeeded': to_datetime(record.get('dNeeded')),
        'dExpires': to_datetime(record.get('dExpires')),
        'dFollowup': to_datetime(record.get('dFollowup')),
        'dCreated': to_datetime(record.get('dCreated')),
        'dPlaced': to_datetime(record.get('dPlaced')),
        'dUpdated': to_datetime(record.get('dUpdated')),
        'dConverted_ToMoveIn': to_datetime(record.get('dConverted_ToMoveIn')),
        'sComment': to_str(record.get('sComment')),
        'sSource': to_str(record.get('sSource'), 100),
    }


class CcwsReservationsPipeline(BasePipeline):
    """Sync raw reservation records → esa_middleware.ccws_reservations."""

    def _make_fetcher(self, soap_client):
        def fetch(site_code: str) -> List[Dict[str, Any]]:
            try:
                results = soap_client.call(
                    operation="ReservationList_v3",
                    parameters={
                        "sLocationCode": site_code.strip(),
                        "iGlobalWaitingNum": "0",
                        "WaitingID": "0",
                    },
                    soap_action=SOAP_ACTION,
                    namespace=NAMESPACE,
                    result_tag="Table",
                )
            except Exception as e:
                self.log.error(f"SOAP fetch failed for {site_code}: {e}")
                return []
            out: List[Dict[str, Any]] = []
            for r in (results or []):
                t = _transform(r, site_code.strip())
                if t:
                    out.append(t)
            return out
        return fetch

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        site_codes = resolve_site_codes(scope)
        if not site_codes:
            return RunResult(status='failed', scope=scope,
                             error='No site_codes resolved')

        soap = build_soap_client()
        try:
            rows, per_site = parallel_fetch(self._make_fetcher(soap), site_codes)

            # Dedup by (SiteCode, WaitingID)
            seen = set()
            deduped: List[Dict[str, Any]] = []
            for r in rows:
                k = (r['SiteCode'], r['WaitingID'])
                if k not in seen:
                    seen.add(k)
                    deduped.append(r)

            if not deduped:
                return RunResult(status='refreshed', records=0, scope=scope,
                                 metadata={'per_site_counts': per_site})

            cols = list(deduped[0].keys())
            sql = text(build_upsert_sql(
                'ccws_reservations', cols,
                conflict_cols=['SiteCode', 'WaitingID'],
            ))
            with get_engine('middleware').begin() as conn:
                conn.execute(sql, deduped)  # executemany in one call

            return RunResult(status='refreshed', records=len(deduped), scope=scope,
                             metadata={'per_site_counts': per_site,
                                       'sites_queried': len(site_codes)})
        finally:
            try: soap.close()
            except Exception: pass
