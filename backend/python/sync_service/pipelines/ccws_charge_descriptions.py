"""
CcwsChargeDescriptionsPipeline — raw ChargeDescriptionsRetrieve → middleware.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.config import get_engine
from sync_service.pipelines._ccws_utils import (
    NAMESPACE, build_soap_client, resolve_site_codes,
    to_int, to_decimal, to_bool, to_datetime, build_upsert_sql,
    parallel_fetch,
)

logger = logging.getLogger(__name__)
SOAP_ACTION = f"{NAMESPACE}/ChargeDescriptionsRetrieve"


def _transform(record: Dict[str, Any], site_code: str) -> Optional[Dict[str, Any]]:
    chg_id = to_int(record.get('ChargeDescID'))
    site_id = to_int(record.get('SiteID'))
    if chg_id is None or site_id is None:
        return None
    return {
        'ChargeDescID': chg_id,
        'SiteID': site_id,
        'SiteCode': site_code,
        'sChgDesc': record.get('sChgDesc'),
        'sChgCategory': record.get('sChgCategory'),
        'dcPrice': to_decimal(record.get('dcPrice')),
        'dcTax1Rate': to_decimal(record.get('dcTax1Rate')),
        'dcTax2Rate': to_decimal(record.get('dcTax2Rate')),
        'bApplyAtMoveIn': to_bool(record.get('bApplyAtMoveIn')),
        'bProrateAtMoveIn': to_bool(record.get('bProrateAtMoveIn')),
        'bPermanent': to_bool(record.get('bPermanent')),
        'dDisabled': to_datetime(record.get('dDisabled')),
    }


class CcwsChargeDescriptionsPipeline(BasePipeline):

    def _make_fetcher(self, soap):
        def fetch(sc: str) -> List[Dict[str, Any]]:
            try:
                results = soap.call(
                    operation="ChargeDescriptionsRetrieve",
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
                t = _transform(r, sc.strip())
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
            seen, deduped = set(), []
            for r in rows:
                k = (r['SiteID'], r['ChargeDescID'])
                if k not in seen:
                    seen.add(k); deduped.append(r)
            if not deduped:
                return RunResult(status='refreshed', records=0, scope=scope,
                                 metadata={'per_site_counts': per_site})
            cols = list(deduped[0].keys())
            sql = text(build_upsert_sql(
                'ccws_charge_descriptions', cols,
                conflict_cols=['ChargeDescID', 'SiteID'],
            ))
            with get_engine('middleware').begin() as conn:
                conn.execute(sql, deduped)
            return RunResult(status='refreshed', records=len(deduped), scope=scope,
                             metadata={'per_site_counts': per_site,
                                       'sites_queried': len(site_codes)})
        finally:
            try: soap.close()
            except Exception: pass
