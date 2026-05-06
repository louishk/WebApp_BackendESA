"""
CcwsRentTaxRatesPipeline — raw RentTaxRatesRetrieve → middleware.

SOAP returns only dcTax1Rate + dcTax2Rate per call; we stamp SiteCode from
the request to produce one row per site.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.config import get_engine
from sync_service.pipelines._ccws_utils import (
    NAMESPACE, build_soap_client, resolve_site_codes,
    to_decimal, build_upsert_sql, parallel_fetch,
)

logger = logging.getLogger(__name__)
SOAP_ACTION = f"{NAMESPACE}/RentTaxRatesRetrieve"


class CcwsRentTaxRatesPipeline(BasePipeline):

    def _make_fetcher(self, soap):
        def fetch(sc: str) -> List[Dict[str, Any]]:
            try:
                results = soap.call(
                    operation="RentTaxRatesRetrieve",
                    parameters={"sLocationCode": sc.strip()},
                    soap_action=SOAP_ACTION,
                    namespace=NAMESPACE,
                    result_tag="Table",
                )
            except Exception as e:
                self.log.error(f"SOAP fetch failed for {sc}: {e}")
                return []
            if not results:
                return []
            r = results[0]
            return [{
                'SiteCode': sc.strip(),
                'dcTax1Rate': to_decimal(r.get('dcTax1Rate')),
                'dcTax2Rate': to_decimal(r.get('dcTax2Rate')),
            }]
        return fetch

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        site_codes = resolve_site_codes(scope)
        if not site_codes:
            return RunResult(status='failed', scope=scope, error='No site_codes resolved')
        soap = build_soap_client()
        try:
            rows, per_site = parallel_fetch(self._make_fetcher(soap), site_codes)
            if not rows:
                return RunResult(status='refreshed', records=0, scope=scope,
                                 metadata={'per_site_counts': per_site})
            cols = list(rows[0].keys())
            sql = text(build_upsert_sql(
                'ccws_rent_tax_rates', cols, conflict_cols=['SiteCode'],
            ))
            with get_engine('middleware').begin() as conn:
                conn.execute(sql, rows)
            return RunResult(status='refreshed', records=len(rows), scope=scope,
                             metadata={'per_site_counts': per_site,
                                       'sites_queried': len(site_codes)})
        finally:
            try: soap.close()
            except Exception: pass
