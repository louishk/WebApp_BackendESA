"""
CcwsDiscountPipeline — sync discount/concession plans from SiteLink
CallCenterWs (DiscountPlansRetrieveIncludingDisabled) into
esa_middleware.ccws_discount.

Scope shape:
    {"site_codes": ["L017", "L018", ...]}   # subset
    {} or None                               # fall back to default_args.location_codes

Freshness:
    freshness_table=ccws_discount, freshness_column=updated_at,
    freshness_database=middleware
"""

import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.config import get_engine

logger = logging.getLogger(__name__)

CALL_CENTER_WS_URL = "https://api.smdservers.net/CCWs_3.5/CallCenterWs.asmx"
NAMESPACE = "http://tempuri.org/CallCenterWs/CallCenterWs"
SOAP_ACTION = "http://tempuri.org/CallCenterWs/CallCenterWs/DiscountPlansRetrieveIncludingDisabled"


# ---------------------------------------------------------------------------
# Type coercion helpers (self-contained — no datalayer import)
# ---------------------------------------------------------------------------

def _to_int(v) -> Optional[int]:
    if v is None or v == '':
        return None
    try:
        return int(str(v).strip())
    except (ValueError, TypeError):
        return None


def _to_decimal(v) -> Optional[Decimal]:
    if v is None or v == '':
        return None
    try:
        return Decimal(str(v).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_bool(v) -> Optional[bool]:
    if v is None or v == '':
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ('true', '1', 'yes', 'y', 't'):
        return True
    if s in ('false', '0', 'no', 'n', 'f'):
        return False
    return None


def _to_datetime(v) -> Optional[datetime]:
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def _transform(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    concession_id = _to_int(record.get('ConcessionID'))
    site_id = _to_int(record.get('SiteID'))
    if concession_id is None or site_id is None:
        return None
    return {
        'ConcessionID': concession_id,
        'SiteID': site_id,
        'iConcessionGlobalNum': _to_int(record.get('iConcessionGlobalNum')),
        'QTTouchDiscPlanID': record.get('QTTouchDiscPlanID'),
        'PlanName_TermID': record.get('PlanName_TermID'),
        'OldPK': record.get('OldPK'),
        'sDefPlanName': record.get('sDefPlanName'),
        'sPlanName': record.get('sPlanName'),
        'sDescription': record.get('sDescription'),
        'sComment': record.get('sComment'),
        'sCouponCode': record.get('sCouponCode'),
        'dPlanStrt': _to_datetime(record.get('dPlanStrt')),
        'dPlanEnd': _to_datetime(record.get('dPlanEnd')),
        'dCreated': _to_datetime(record.get('dCreated')),
        'dUpdated': _to_datetime(record.get('dUpdated')),
        'dArchived': _to_datetime(record.get('dArchived')),
        'dDisabled': _to_datetime(record.get('dDisabled')),
        'dDeleted': _to_datetime(record.get('dDeleted')),
        'iShowOn': _to_int(record.get('iShowOn')),
        'bNeverExpires': _to_bool(record.get('bNeverExpires')),
        'iExpirMonths': _to_int(record.get('iExpirMonths')),
        'bPrepay': _to_bool(record.get('bPrepay')),
        'bOnPmt': _to_bool(record.get('bOnPmt')),
        'bManualCredit': _to_bool(record.get('bManualCredit')),
        'iPrePaidMonths': _to_int(record.get('iPrePaidMonths')),
        'iInMonth': _to_int(record.get('iInMonth')),
        'bPermanent': _to_bool(record.get('bPermanent')),
        'iAmtType': _to_int(record.get('iAmtType')),
        'dcChgAmt': _to_decimal(record.get('dcChgAmt')),
        'dcFixedDiscount': _to_decimal(record.get('dcFixedDiscount')),
        'dcPCDiscount': _to_decimal(record.get('dcPCDiscount')),
        'bRound': _to_bool(record.get('bRound')),
        'dcRoundTo': _to_decimal(record.get('dcRoundTo')),
        'dcMaxAmountOff': _to_decimal(record.get('dcMaxAmountOff')),
        'ChargeDescID': _to_int(record.get('ChargeDescID')),
        'iQty': _to_int(record.get('iQty')),
        'iOfferItemAction': _to_int(record.get('iOfferItemAction')),
        'bForCorp': _to_bool(record.get('bForCorp')),
        'dcMaxOccPct': _to_decimal(record.get('dcMaxOccPct')),
        'bForAllUnits': _to_bool(record.get('bForAllUnits')),
        'iExcludeIfLessThanUnitsTotal': _to_int(record.get('iExcludeIfLessThanUnitsTotal')),
        'dcMaxOccPctExcludeIfMoreThanUnitsTotal': _to_decimal(record.get('dcMaxOccPctExcludeIfMoreThanUnitsTotal')),
        'iExcludeIfMoreThanUnitsTotal': _to_int(record.get('iExcludeIfMoreThanUnitsTotal')),
        'iAvailableAt': _to_int(record.get('iAvailableAt')),
        'bEligibleToRemoveIfPastDue': _to_bool(record.get('bEligibleToRemoveIfPastDue')),
        'iRestrictionFlags': _to_int(record.get('iRestrictionFlags')),
        'iOccupancyPctUnitCountMethod': _to_int(record.get('iOccupancyPctUnitCountMethod')),
        'ChargeDescID1': _to_int(record.get('ChargeDescID1')),
        'SiteID1': _to_int(record.get('SiteID1')),
        'ChartOfAcctID': _to_int(record.get('ChartOfAcctID')),
        'ChgDesc_TermID': _to_int(record.get('ChgDesc_TermID')),
        'sDefChgDesc': record.get('sDefChgDesc'),
        'sChgDesc': record.get('sChgDesc'),
        'sVendor': record.get('sVendor'),
        'sVendorPhone': record.get('sVendorPhone'),
        'sReorderPartNum': record.get('sReorderPartNum'),
        'sChgCategory': record.get('sChgCategory'),
        'bApplyAtMoveIn': _to_bool(record.get('bApplyAtMoveIn')),
        'bProrateAtMoveIn': _to_bool(record.get('bProrateAtMoveIn')),
        'bPermanent1': _to_bool(record.get('bPermanent1')),
        'dcPrice': _to_decimal(record.get('dcPrice')),
        'dcTax1Rate': _to_decimal(record.get('dcTax1Rate')),
        'dcTax2Rate': _to_decimal(record.get('dcTax2Rate')),
        'dcCost': _to_decimal(record.get('dcCost')),
        'dcInStock': _to_decimal(record.get('dcInStock')),
        'dcOrderPt': _to_decimal(record.get('dcOrderPt')),
        'dChgStrt': _to_datetime(record.get('dChgStrt')),
        'dChgDisabled': _to_datetime(record.get('dChgDisabled')),
        'bUseMileageRate': _to_bool(record.get('bUseMileageRate')),
        'dcMileageRate': _to_decimal(record.get('dcMileageRate')),
        'iIncludedMiles': _to_int(record.get('iIncludedMiles')),
        'dDisabled1': _to_datetime(record.get('dDisabled1')),
        'dDeleted1': _to_datetime(record.get('dDeleted1')),
        'dUpdated1': _to_datetime(record.get('dUpdated1')),
        'OldPK1': _to_int(record.get('OldPK1')),
        'sCorpCategory': record.get('sCorpCategory'),
        'sBarCode': record.get('sBarCode'),
        'iPriceType': _to_int(record.get('iPriceType')),
        'dcPCRate': _to_decimal(record.get('dcPCRate')),
        'dcMinPriceIfPC': _to_decimal(record.get('dcMinPriceIfPC')),
        'bRound1': _to_bool(record.get('bRound1')),
        'dcRoundTo1': _to_decimal(record.get('dcRoundTo1')),
    }


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class CcwsDiscountPipeline(BasePipeline):
    """Sync CallCenterWs concession plans → esa_middleware.ccws_discount."""

    def _get_soap_client(self):
        from common.soap_client import SOAPClient
        from common.config_loader import get_config
        soap_cfg = get_config().apis.soap
        return SOAPClient(
            base_url=CALL_CENTER_WS_URL,
            corp_code=soap_cfg.corp_code,
            corp_user=soap_cfg.corp_user,
            api_key=soap_cfg.api_key_vault,
            corp_password=soap_cfg.corp_password_vault,
            timeout=120,
            retries=3,
        )

    def _resolve_site_codes(self, scope: Dict[str, Any]) -> List[str]:
        if 'site_codes' in scope:
            v = scope['site_codes']
            return list(v) if isinstance(v, (list, tuple)) else [v]
        if 'site_code' in scope:
            return [scope['site_code']]
        if 'location_codes' in scope:
            return list(scope['location_codes'])
        return []

    def _make_fetcher(self, soap_client):
        def fetch(site_code: str) -> List[Dict[str, Any]]:
            try:
                results = soap_client.call(
                    operation="DiscountPlansRetrieveIncludingDisabled",
                    parameters={"sLocationCode": site_code.strip()},
                    soap_action=SOAP_ACTION,
                    namespace=NAMESPACE,
                    result_tag="ConcessionPlans",
                )
            except Exception as e:
                self.log.error(f"SOAP fetch failed for {site_code}: {e}")
                return []
            out: List[Dict[str, Any]] = []
            for r in (results or []):
                t = _transform(r)
                if t:
                    out.append(t)
            return out
        return fetch

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        site_codes = self._resolve_site_codes(scope)
        if not site_codes:
            return RunResult(
                status='failed',
                scope=scope,
                error='No site_codes resolved (provide in scope or default_args.location_codes)',
            )

        from sync_service.pipelines._ccws_utils import parallel_fetch
        soap_client = self._get_soap_client()
        try:
            all_records, per_site_counts = parallel_fetch(
                self._make_fetcher(soap_client), site_codes,
            )

            # Dedup by (SiteID, ConcessionID)
            seen = set()
            deduped: List[Dict[str, Any]] = []
            for r in all_records:
                key = (r['SiteID'], r['ConcessionID'])
                if key not in seen:
                    seen.add(key)
                    deduped.append(r)

            if not deduped:
                return RunResult(
                    status='refreshed',
                    records=0,
                    scope=scope,
                    metadata={'per_site_counts': per_site_counts},
                )

            cols = list(deduped[0].keys())
            col_list = ', '.join(f'"{c}"' for c in cols)
            placeholders = ', '.join(f':{c}' for c in cols)
            update_list = ', '.join(
                f'"{c}" = EXCLUDED."{c}"' for c in cols
                if c not in ('SiteID', 'ConcessionID')
            )
            upsert_sql = text(f"""
                INSERT INTO ccws_discount ({col_list}, created_at, updated_at)
                VALUES ({placeholders}, NOW(), NOW())
                ON CONFLICT ("SiteID", "ConcessionID")
                DO UPDATE SET {update_list}, updated_at = NOW()
            """)

            engine = get_engine('middleware')
            with engine.begin() as conn:
                conn.execute(upsert_sql, deduped)

            return RunResult(
                status='refreshed',
                records=len(deduped),
                scope=scope,
                metadata={
                    'per_site_counts': per_site_counts,
                    'sites_queried': len(site_codes),
                },
            )

        finally:
            try:
                soap_client.close()
            except Exception:
                pass
