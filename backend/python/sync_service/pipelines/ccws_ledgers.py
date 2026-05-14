"""
CcwsLedgersPipeline — sync ccws_ledgers + ccws_charges from CallCenterWs.

Sources tenant IDs from ccws_tenants (DB roster), then fetches ledger and
charge data via CallCenterWs SOAP API:
1. ccws_tenants DB query -> Get all tenant IDs for a location
2. LedgersByTenantID_v3 -> For each tenant, get ledger(s)
3. ChargesAllByLedgerID -> For each ledger, get all charges
4. Phase C: Discover new TenantIDs from rentroll, backfill via API

Modes:
  - incremental (default): only charges for ledgers with dUpdated > threshold
  - full:                  fetch all data (initial load / historical)

Scope keys honoured (all optional):
  - mode:           'incremental' | 'full'   default 'incremental'
  - days_back:      int                      default 7
  - location_codes: list[str]                default from config
  - all_tenants:    bool                     default False
  - since:          'YYYY-MM-DD' string      overrides days_back
"""

import logging
import concurrent.futures
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import text as sa_text

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration helpers
# =============================================================================

def get_callcenter_url(reporting_url: str) -> str:
    """Convert ReportingWs URL to CallCenterWs URL."""
    return reporting_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')


def _get_pipeline_config(key: str, default=None):
    try:
        from common.config_loader import get_config
        cfg = get_config()
        pipeline_cfg = getattr(cfg.scheduler.pipelines, 'ccws_ledgers', None)
        if pipeline_cfg:
            return getattr(pipeline_cfg, key, default)
    except Exception:
        pass
    return default


# =============================================================================
# Transform functions
# =============================================================================

def transform_ccws_tenant(record: Dict[str, Any], location_code: str, extract_date: date) -> Dict[str, Any]:
    """Transform tenant API record for ccws_tenants."""
    from common import (
        convert_to_bool, convert_to_int, convert_to_decimal, convert_to_datetime,
    )
    return {
        'SiteID': convert_to_int(record.get('SiteID')),
        'TenantID': convert_to_int(record.get('TenantID')),
        'sAccessCode': record.get('sAccessCode'),
        'sAccessCode2': record.get('sAccessCode2'),
        'iAccessCode2Type': convert_to_int(record.get('iAccessCode2Type')),
        'sWebPassword': record.get('sWebPassword'),
        'bAllowedFacilityAccess': convert_to_bool(record.get('bAllowedFacilityAccess')),
        'sMrMrs': record.get('sMrMrs'),
        'sFName': record.get('sFName'),
        'sMI': record.get('sMI'),
        'sLName': record.get('sLName'),
        'sCompany': record.get('sCompany'),
        'sAddr1': record.get('sAddr1'),
        'sAddr2': record.get('sAddr2'),
        'sCity': record.get('sCity'),
        'sRegion': record.get('sRegion'),
        'sPostalCode': record.get('sPostalCode'),
        'sCountry': record.get('sCountry'),
        'sPhone': record.get('sPhone'),
        'sFax': record.get('sFax'),
        'sEmail': record.get('sEmail'),
        'sPager': record.get('sPager'),
        'sMobile': record.get('sMobile'),
        'sCountryCodeMobile': record.get('sCountryCodeMobile'),
        'sMrMrsAlt': record.get('sMrMrsAlt'),
        'sFNameAlt': record.get('sFNameAlt'),
        'sMIAlt': record.get('sMIAlt'),
        'sLNameAlt': record.get('sLNameAlt'),
        'sAddr1Alt': record.get('sAddr1Alt'),
        'sAddr2Alt': record.get('sAddr2Alt'),
        'sCityAlt': record.get('sCityAlt'),
        'sRegionAlt': record.get('sRegionAlt'),
        'sPostalCodeAlt': record.get('sPostalCodeAlt'),
        'sCountryAlt': record.get('sCountryAlt'),
        'sPhoneAlt': record.get('sPhoneAlt'),
        'sEmailAlt': record.get('sEmailAlt'),
        'sRelationshipAlt': record.get('sRelationshipAlt'),
        'sMrMrsBus': record.get('sMrMrsBus'),
        'sFNameBus': record.get('sFNameBus'),
        'sMIBus': record.get('sMIBus'),
        'sLNameBus': record.get('sLNameBus'),
        'sCompanyBus': record.get('sCompanyBus'),
        'sAddr1Bus': record.get('sAddr1Bus'),
        'sAddr2Bus': record.get('sAddr2Bus'),
        'sCityBus': record.get('sCityBus'),
        'sRegionBus': record.get('sRegionBus'),
        'sPostalCodeBus': record.get('sPostalCodeBus'),
        'sCountryBus': record.get('sCountryBus'),
        'sPhoneBus': record.get('sPhoneBus'),
        'sEmailBus': record.get('sEmailBus'),
        'sMrMrsAdd': record.get('sMrMrsAdd'),
        'sFNameAdd': record.get('sFNameAdd'),
        'sMIAdd': record.get('sMIAdd'),
        'sLNameAdd': record.get('sLNameAdd'),
        'sAddr1Add': record.get('sAddr1Add'),
        'sAddr2Add': record.get('sAddr2Add'),
        'sCityAdd': record.get('sCityAdd'),
        'sRegionAdd': record.get('sRegionAdd'),
        'sPostalCodeAdd': record.get('sPostalCodeAdd'),
        'sCountryAdd': record.get('sCountryAdd'),
        'sPhoneAdd': record.get('sPhoneAdd'),
        'sEmailAdd': record.get('sEmailAdd'),
        'sLicense': record.get('sLicense'),
        'sLicRegion': record.get('sLicRegion'),
        'sSSN': record.get('sSSN'),
        'sTaxID': record.get('sTaxID'),
        'sTaxExemptCode': record.get('sTaxExemptCode'),
        'dDOB': convert_to_datetime(record.get('dDOB')),
        'iGender': convert_to_int(record.get('iGender')),
        'sEmployer': record.get('sEmployer'),
        'bCommercial': convert_to_bool(record.get('bCommercial')),
        'bTaxExempt': convert_to_bool(record.get('bTaxExempt')),
        'bSpecial': convert_to_bool(record.get('bSpecial')),
        'bNeverLockOut': convert_to_bool(record.get('bNeverLockOut')),
        'bCompanyIsTenant': convert_to_bool(record.get('bCompanyIsTenant')),
        'bOnWaitingList': convert_to_bool(record.get('bOnWaitingList')),
        'bNoChecks': convert_to_bool(record.get('bNoChecks')),
        'bPermanent': convert_to_bool(record.get('bPermanent')),
        'bWalkInPOS': convert_to_bool(record.get('bWalkInPOS')),
        'bSpecialAlert': convert_to_bool(record.get('bSpecialAlert')),
        'bPermanentGateLockout': convert_to_bool(record.get('bPermanentGateLockout')),
        'bSMSOptIn': convert_to_bool(record.get('bSMSOptIn')),
        'bDisabledWebAccess': convert_to_bool(record.get('bDisabledWebAccess')),
        'bHasActiveLedger': convert_to_bool(record.get('bHasActiveLedger')),
        'iBlackListRating': convert_to_int(record.get('iBlackListRating')),
        'iTenEvents_OptOut': convert_to_int(record.get('iTenEvents_OptOut')),
        'MarketingID': convert_to_int(record.get('MarketingID')),
        'MktgDistanceID': convert_to_int(record.get('MktgDistanceID')),
        'MktgWhatID': convert_to_int(record.get('MktgWhatID')),
        'MktgReasonID': convert_to_int(record.get('MktgReasonID')),
        'MktgWhyID': convert_to_int(record.get('MktgWhyID')),
        'MktgTypeID': convert_to_int(record.get('MktgTypeID')),
        'iHowManyOtherStorageCosDidYouContact': convert_to_int(record.get('iHowManyOtherStorageCosDidYouContact')),
        'iUsedSelfStorageInThePast': convert_to_int(record.get('iUsedSelfStorageInThePast')),
        'iMktg_DidYouVisitWebSite': convert_to_int(record.get('iMktg_DidYouVisitWebSite')),
        'bExit_OnEmailOfferList': convert_to_bool(record.get('bExit_OnEmailOfferList')),
        'iExitSat_Cleanliness': convert_to_int(record.get('iExitSat_Cleanliness')),
        'iExitSat_Safety': convert_to_int(record.get('iExitSat_Safety')),
        'iExitSat_Services': convert_to_int(record.get('iExitSat_Services')),
        'iExitSat_Staff': convert_to_int(record.get('iExitSat_Staff')),
        'iExitSat_Price': convert_to_int(record.get('iExitSat_Price')),
        'dcLongitude': convert_to_decimal(record.get('dcLongitude')),
        'dcLatitude': convert_to_decimal(record.get('dcLatitude')),
        'sTenNote': record.get('sTenNote'),
        'sIconList': record.get('sIconList'),
        'iPrimaryPic': convert_to_int(record.get('iPrimaryPic')),
        'sPicFileN1': record.get('sPicFileN1'),
        'sPicFileN2': record.get('sPicFileN2'),
        'sPicFileN3': record.get('sPicFileN3'),
        'sPicFileN4': record.get('sPicFileN4'),
        'sPicFileN5': record.get('sPicFileN5'),
        'sPicFileN6': record.get('sPicFileN6'),
        'sPicFileN7': record.get('sPicFileN7'),
        'sPicFileN8': record.get('sPicFileN8'),
        'sPicFileN9': record.get('sPicFileN9'),
        'iGlobalNum_NationalMasterAccount': convert_to_int(record.get('iGlobalNum_NationalMasterAccount')),
        'iGlobalNum_NationalFranchiseAccount': convert_to_int(record.get('iGlobalNum_NationalFranchiseAccount')),
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'uTS': record.get('uTS'),
        'sLocationCode': location_code,
        'extract_date': extract_date,
    }


def transform_ccws_ledger(record: Dict[str, Any], tenant_id: int, extract_date: date) -> Dict[str, Any]:
    """Transform LedgersByTenantID_v3 API record for ccws_ledgers."""
    from common import (
        convert_to_bool, convert_to_int, convert_to_decimal, convert_to_datetime,
    )
    return {
        'SiteID': convert_to_int(record.get('SiteID')),
        'LedgerID': convert_to_int(record.get('LedgerID')),
        'TenantID': tenant_id,
        'EmployeeID': convert_to_int(record.get('EmployeeID')),
        'UnitID': convert_to_int(record.get('UnitID')),
        'MarketingID': convert_to_int(record.get('MarketingID')),
        'MktgDistanceID': convert_to_int(record.get('MktgDistanceID')),
        'MktgReasonID': convert_to_int(record.get('MktgReasonID')),
        'MktgTypeID': convert_to_int(record.get('MktgTypeID')),
        'MktgWhatID': convert_to_int(record.get('MktgWhatID')),
        'MktgWhyID': convert_to_int(record.get('MktgWhyID')),
        'TimeZoneID': convert_to_int(record.get('TimeZoneID')),
        'TenantName': record.get('TenantName'),
        'sUnitName': record.get('sUnitName'),
        'sMrMrs': record.get('sMrMrs'),
        'sFName': record.get('sFName'),
        'sMI': record.get('sMI'),
        'sLName': record.get('sLName'),
        'sCompany': record.get('sCompany'),
        'sAddr1': record.get('sAddr1'),
        'sAddr2': record.get('sAddr2'),
        'sCity': record.get('sCity'),
        'sRegion': record.get('sRegion'),
        'sPostalCode': record.get('sPostalCode'),
        'sCountry': record.get('sCountry'),
        'sPhone': record.get('sPhone'),
        'sFax': record.get('sFax'),
        'sEmail': record.get('sEmail'),
        'sPager': record.get('sPager'),
        'sMobile': record.get('sMobile'),
        'sCountryCodeMobile': record.get('sCountryCodeMobile'),
        'sMrMrsAlt': record.get('sMrMrsAlt'),
        'sFNameAlt': record.get('sFNameAlt'),
        'sMIAlt': record.get('sMIAlt'),
        'sLNameAlt': record.get('sLNameAlt'),
        'sAddr1Alt': record.get('sAddr1Alt'),
        'sAddr2Alt': record.get('sAddr2Alt'),
        'sCityAlt': record.get('sCityAlt'),
        'sRegionAlt': record.get('sRegionAlt'),
        'sPostalCodeAlt': record.get('sPostalCodeAlt'),
        'sCountryAlt': record.get('sCountryAlt'),
        'sPhoneAlt': record.get('sPhoneAlt'),
        'sEmailAlt': record.get('sEmailAlt'),
        'sRelationshipAlt': record.get('sRelationshipAlt'),
        'sMrMrsBus': record.get('sMrMrsBus'),
        'sFNameBus': record.get('sFNameBus'),
        'sMIBus': record.get('sMIBus'),
        'sLNameBus': record.get('sLNameBus'),
        'sCompanyBus': record.get('sCompanyBus'),
        'sAddr1Bus': record.get('sAddr1Bus'),
        'sAddr2Bus': record.get('sAddr2Bus'),
        'sCityBus': record.get('sCityBus'),
        'sRegionBus': record.get('sRegionBus'),
        'sPostalCodeBus': record.get('sPostalCodeBus'),
        'sCountryBus': record.get('sCountryBus'),
        'sPhoneBus': record.get('sPhoneBus'),
        'sEmailBus': record.get('sEmailBus'),
        'sMrMrsAdd': record.get('sMrMrsAdd'),
        'sFNameAdd': record.get('sFNameAdd'),
        'sMIAdd': record.get('sMIAdd'),
        'sLNameAdd': record.get('sLNameAdd'),
        'sAddr1Add': record.get('sAddr1Add'),
        'sAddr2Add': record.get('sAddr2Add'),
        'sCityAdd': record.get('sCityAdd'),
        'sRegionAdd': record.get('sRegionAdd'),
        'sPostalCodeAdd': record.get('sPostalCodeAdd'),
        'sCountryAdd': record.get('sCountryAdd'),
        'sPhoneAdd': record.get('sPhoneAdd'),
        'sEmailAdd': record.get('sEmailAdd'),
        'sAccessCode': record.get('sAccessCode'),
        'sAccessCode2': record.get('sAccessCode2'),
        'iAccessCode2Type': convert_to_int(record.get('iAccessCode2Type')),
        'sLicense': record.get('sLicense'),
        'sLicRegion': record.get('sLicRegion'),
        'sSSN': record.get('sSSN'),
        'sTaxID': record.get('sTaxID'),
        'sTaxExemptCode': record.get('sTaxExemptCode'),
        'dDOB': convert_to_datetime(record.get('dDOB')),
        'iGender': convert_to_int(record.get('iGender')),
        'bCommercial': convert_to_bool(record.get('bCommercial')),
        'bCompanyIsTenant': convert_to_bool(record.get('bCompanyIsTenant')),
        'bDisabledWebAccess': convert_to_bool(record.get('bDisabledWebAccess')),
        'bExcludeFromInsurance': convert_to_bool(record.get('bExcludeFromInsurance')),
        'bInvoice': convert_to_bool(record.get('bInvoice')),
        'bNeverLockOut': convert_to_bool(record.get('bNeverLockOut')),
        'bNoChecks': convert_to_bool(record.get('bNoChecks')),
        'bOnWaitingList': convert_to_bool(record.get('bOnWaitingList')),
        'bOverlocked': convert_to_bool(record.get('bOverlocked')),
        'bPermanent': convert_to_bool(record.get('bPermanent')),
        'bPermanentGateLockout': convert_to_bool(record.get('bPermanentGateLockout')),
        'bSMSOptIn': convert_to_bool(record.get('bSMSOptIn')),
        'bSpecial': convert_to_bool(record.get('bSpecial')),
        'bSpecialAlert': convert_to_bool(record.get('bSpecialAlert')),
        'bTaxExempt': convert_to_bool(record.get('bTaxExempt')),
        'bWalkInPOS': convert_to_bool(record.get('bWalkInPOS')),
        'iLeaseNum': convert_to_int(record.get('iLeaseNum')),
        'iDefLeaseNum': convert_to_int(record.get('iDefLeaseNum')),
        'dMovedIn': convert_to_datetime(record.get('dMovedIn')),
        'dPaidThru': convert_to_datetime(record.get('dPaidThru')),
        'dSchedOut': convert_to_datetime(record.get('dSchedOut')),
        'dAnniv': convert_to_datetime(record.get('dAnniv')),
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'dcRent': convert_to_decimal(record.get('dcRent')),
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),
        'dcChargeBalance': convert_to_decimal(record.get('dcChargeBalance')),
        'dcTotalDue': convert_to_decimal(record.get('dcTotalDue')),
        'dcTaxRateRent': convert_to_decimal(record.get('dcTaxRateRent')),
        'dcTaxRateInsurance': convert_to_decimal(record.get('dcTaxRateInsurance')),
        'sBillingFrequency': record.get('sBillingFrequency'),
        'iAutoBillType': convert_to_int(record.get('iAutoBillType')),
        'iInvoiceDeliveryType': convert_to_int(record.get('iInvoiceDeliveryType')),
        'iHowManyOtherStorageCosDidYouContact': convert_to_int(record.get('iHowManyOtherStorageCosDidYouContact')),
        'iUsedSelfStorageInThePast': convert_to_int(record.get('iUsedSelfStorageInThePast')),
        'iMktg_DidYouVisitWebSite': convert_to_int(record.get('iMktg_DidYouVisitWebSite')),
        'bExit_OnEmailOfferList': convert_to_bool(record.get('bExit_OnEmailOfferList')),
        'iExitSat_Cleanliness': convert_to_int(record.get('iExitSat_Cleanliness')),
        'iExitSat_Price': convert_to_int(record.get('iExitSat_Price')),
        'iExitSat_Safety': convert_to_int(record.get('iExitSat_Safety')),
        'iExitSat_Services': convert_to_int(record.get('iExitSat_Services')),
        'iExitSat_Staff': convert_to_int(record.get('iExitSat_Staff')),
        'iBlackListRating': convert_to_int(record.get('iBlackListRating')),
        'iTenEvents_OptOut': convert_to_int(record.get('iTenEvents_OptOut')),
        'dcLatitude': convert_to_decimal(record.get('dcLatitude')),
        'dcLongitude': convert_to_decimal(record.get('dcLongitude')),
        'sTenNote': record.get('sTenNote'),
        'sIconList': record.get('sIconList'),
        'iPrimaryPic': convert_to_int(record.get('iPrimaryPic')),
        'sPicFileN1': record.get('sPicFileN1'),
        'sPicFileN2': record.get('sPicFileN2'),
        'sPicFileN3': record.get('sPicFileN3'),
        'sPicFileN4': record.get('sPicFileN4'),
        'sPicFileN5': record.get('sPicFileN5'),
        'sPicFileN6': record.get('sPicFileN6'),
        'sPicFileN7': record.get('sPicFileN7'),
        'sPicFileN8': record.get('sPicFileN8'),
        'sPicFileN9': record.get('sPicFileN9'),
        'uTS': record.get('uTS'),
        'uTSbigint': convert_to_int(record.get('uTSbigint')),
        'extract_date': extract_date,
    }


def transform_ccws_charge(record: Dict[str, Any], ledger_id: int, site_id: int, extract_date: date) -> Dict[str, Any]:
    """Transform ChargesAllByLedgerID API record for ccws_charges."""
    from common import (
        convert_to_bool, convert_to_int, convert_to_decimal, convert_to_datetime,
    )
    pmt_amt = convert_to_decimal(record.get('dcPmtAmt'))
    if pmt_amt is None:
        pmt_amt = Decimal('0')
    return {
        'SiteID': site_id,
        'ChargeID': convert_to_int(record.get('ChargeID')),
        'dcPmtAmt': pmt_amt,
        'LedgerID': ledger_id,
        'ChargeDescID': convert_to_int(record.get('ChargeDescID')),
        'dcAmt': convert_to_decimal(record.get('dcAmt')),
        'dcPrice': convert_to_decimal(record.get('dcPrice')),
        'dcQty': convert_to_decimal(record.get('dcQty')),
        'dcTax1': convert_to_decimal(record.get('dcTax1')),
        'dcTax2': convert_to_decimal(record.get('dcTax2')),
        'dChgStrt': convert_to_datetime(record.get('dChgStrt')),
        'dChgEnd': convert_to_datetime(record.get('dChgEnd')),
        'bMoveIn': convert_to_bool(record.get('bMoveIn')),
        'bMoveOut': convert_to_bool(record.get('bMoveOut')),
        'sChgCategory': record.get('sChgCategory'),
        'sChgDesc': record.get('sChgDesc'),
        'sDefChgDesc': record.get('sDefChgDesc'),
        'extract_date': extract_date,
    }


# =============================================================================
# SOAP API call helpers
# =============================================================================

def _call_soap(soap_client, report_name: str, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
    from common import REPORT_REGISTRY
    config = REPORT_REGISTRY[report_name]
    return soap_client.call(
        operation=config.operation,
        parameters=parameters,
        soap_action=config.soap_action,
        namespace=config.namespace,
        result_tag=config.result_tag,
    )


def get_site_id_for_location(engine, location_code: str) -> Optional[int]:
    with engine.connect() as conn:
        row = conn.execute(sa_text(
            'SELECT "SiteID" FROM siteinfo WHERE "SiteCode" = :loc LIMIT 1'
        ), {'loc': location_code}).fetchone()
    return row[0] if row else None


def get_tenant_ids_from_db(engine, site_id: int, active_only: bool = True) -> List[int]:
    if active_only:
        query = sa_text(
            'SELECT DISTINCT "TenantID" FROM ccws_tenants '
            'WHERE "SiteID" = :sid AND "TenantID" IS NOT NULL AND "bHasActiveLedger" = true'
        )
    else:
        query = sa_text(
            'SELECT DISTINCT "TenantID" FROM ccws_tenants '
            'WHERE "SiteID" = :sid AND "TenantID" IS NOT NULL'
        )
    with engine.connect() as conn:
        rows = conn.execute(query, {'sid': site_id}).fetchall()
    return [row[0] for row in rows]


def fetch_ledgers_for_tenant(soap_client, location_code: str, tenant_id: int) -> List[Dict[str, Any]]:
    return _call_soap(soap_client, 'ledgers_by_tenant_id_v3', {
        'sLocationCode': location_code,
        'sTenantID': str(tenant_id),
    })


def fetch_charges_for_ledger(soap_client, location_code: str, ledger_id: int) -> List[Dict[str, Any]]:
    return _call_soap(soap_client, 'charges_all_by_ledger_id', {
        'sLocationCode': location_code,
        'ledgerId': ledger_id,
    })


def fetch_tenant_info(soap_client, location_code: str, tenant_id: int) -> Optional[Dict[str, Any]]:
    try:
        results = _call_soap(soap_client, 'tenant_info_by_tenant_id', {
            'sLocationCode': location_code,
            'iTenantID': tenant_id,
        })
        return results[0] if results else None
    except Exception as e:
        logger.warning("Failed to fetch tenant info for %s/TenantID=%s: %s", location_code, tenant_id, e)
        return None


# =============================================================================
# Phase C: discover new tenants from rentroll
# =============================================================================

def discover_and_backfill_new_tenants(
    soap_client,
    engine,
    location_codes: List[str],
    extract_date: date,
    chunk_size: int = 500,
) -> int:
    """Check rentroll for TenantIDs not yet in ccws_tenants and backfill."""
    from common import SessionManager, UpsertOperations, CcwsTenant, deduplicate_records

    logger.info("Phase C: Discovering new tenants from rentroll...")

    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            'SELECT "SiteID", "SiteCode" FROM siteinfo WHERE "SiteCode" IS NOT NULL'
        )).fetchall()
    site_to_location = {row[0]: row[1] for row in rows}

    with engine.connect() as conn:
        new_pairs = conn.execute(sa_text("""
            SELECT DISTINCT r."SiteID", r."TenantID"
            FROM rentroll r
            LEFT JOIN ccws_tenants ct ON r."SiteID" = ct."SiteID" AND r."TenantID" = ct."TenantID"
            WHERE ct."TenantID" IS NULL
              AND r."TenantID" IS NOT NULL
              AND r."SiteID" IS NOT NULL
        """)).fetchall()

    if not new_pairs:
        logger.info("Phase C: no new tenants found in rentroll")
        return 0

    logger.info("Phase C: found %d new (SiteID, TenantID) pairs", len(new_pairs))

    added = 0
    session_manager = SessionManager(engine)
    ccws_tenant_records = []

    for site_id, tenant_id in new_pairs:
        location_code = site_to_location.get(site_id)
        if not location_code:
            continue
        raw = fetch_tenant_info(soap_client, location_code, tenant_id)
        if raw:
            transformed = transform_ccws_tenant(raw, location_code, extract_date)
        else:
            transformed = {
                'SiteID': site_id,
                'TenantID': tenant_id,
                'sLocationCode': location_code,
                'extract_date': extract_date,
            }
        if transformed.get('TenantID'):
            ccws_tenant_records.append(transformed)
            added += 1

    if ccws_tenant_records:
        ccws_tenant_records = deduplicate_records(ccws_tenant_records, ['SiteID', 'TenantID'])
        logger.info("Phase C: upserting %d new ccws_tenants", len(ccws_tenant_records))
        with session_manager.session_scope() as session:
            upsert_ops = UpsertOperations(session, 'postgresql')
            for i in range(0, len(ccws_tenant_records), chunk_size):
                chunk = ccws_tenant_records[i:i + chunk_size]
                upsert_ops.upsert_batch(
                    model=CcwsTenant,
                    records=chunk,
                    constraint_columns=['SiteID', 'TenantID'],
                    chunk_size=chunk_size,
                )

    logger.info("Phase C complete: %d new tenants added", added)
    return added


# =============================================================================
# Core location processing
# =============================================================================

def process_location(
    soap_client,
    engine,
    location_code: str,
    site_id: int,
    extract_date: date,
    incremental_since: Optional[datetime] = None,
    active_only: bool = True,
) -> Dict[str, List[Dict]]:
    """Fetch ledgers and charges for all tenants at a single location."""
    result: Dict[str, List[Dict]] = {'ccws_ledgers': [], 'ccws_charges': []}

    tenant_ids = get_tenant_ids_from_db(engine, site_id, active_only=active_only)
    label = "active" if active_only else "all"
    logger.info("[%s] Step 1: %d %s tenants from DB roster", location_code, len(tenant_ids), label)

    if not tenant_ids:
        logger.info("[%s] No tenants in DB roster, skipping", location_code)
        return result

    logger.info("[%s] Step 2: Fetching ledgers for %d tenants", location_code, len(tenant_ids))
    ledger_to_site: Dict[int, int] = {}
    ledgers_for_charges: List[Dict] = []

    for tenant_id in tenant_ids:
        try:
            raw_ledgers = fetch_ledgers_for_tenant(soap_client, location_code, tenant_id)
        except Exception as e:
            logger.warning("[%s] Failed to fetch ledgers for tenant %s: %s", location_code, tenant_id, e)
            continue

        for raw_l in raw_ledgers:
            ccws_l = transform_ccws_ledger(raw_l, tenant_id, extract_date)
            if not ccws_l['LedgerID']:
                continue
            result['ccws_ledgers'].append(ccws_l)
            ledger_to_site[ccws_l['LedgerID']] = ccws_l['SiteID']

            if incremental_since:
                ledger_created = ccws_l.get('dCreated')
                ledger_updated = ccws_l.get('dUpdated')
                if (ledger_created and ledger_created > incremental_since) or \
                        (ledger_updated and ledger_updated > incremental_since):
                    ledgers_for_charges.append(ccws_l)
            else:
                ledgers_for_charges.append(ccws_l)

    logger.info("[%s] Found %d ledgers total", location_code, len(result['ccws_ledgers']))
    if incremental_since:
        logger.info("[%s] Ledgers updated since %s: %d",
                    location_code, incremental_since.date(), len(ledgers_for_charges))

    if not ledgers_for_charges:
        logger.info("[%s] No ledgers need charge refresh, skipping", location_code)
        return result

    ledger_ids = [l['LedgerID'] for l in ledgers_for_charges]
    logger.info("[%s] Step 3: Fetching charges for %d ledgers", location_code, len(ledger_ids))

    for ledger_id in ledger_ids:
        try:
            sid = ledger_to_site.get(ledger_id)
            raw_charges = fetch_charges_for_ledger(soap_client, location_code, ledger_id)
            for c in raw_charges:
                ccws_c = transform_ccws_charge(c, ledger_id, sid, extract_date)
                if ccws_c['ChargeID']:
                    result['ccws_charges'].append(ccws_c)
        except Exception as e:
            logger.warning("[%s] Failed to fetch charges for ledger %s: %s", location_code, ledger_id, e)

    logger.info("[%s] Found %d charges", location_code, len(result['ccws_charges']))
    return result


# =============================================================================
# Database push
# =============================================================================

def push_to_database(data: Dict[str, List[Dict]], engine, chunk_size: int = 500) -> None:
    """Upsert ledgers and charges to ccws_* tables."""
    from common import (
        Base, SessionManager, UpsertOperations, CcwsTenant, CcwsLedger, CcwsCharge,
        deduplicate_records,
    )

    Base.metadata.create_all(engine, tables=[
        CcwsTenant.__table__,
        CcwsLedger.__table__,
        CcwsCharge.__table__,
    ])

    upsert_jobs = [
        ('ccws_ledgers', 'ccws_ledgers', CcwsLedger, ['SiteID', 'LedgerID']),
        ('ccws_charges', 'ccws_charges', CcwsCharge, ['SiteID', 'ChargeID', 'dcPmtAmt']),
    ]

    session_manager = SessionManager(engine)
    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, 'postgresql')
        for label, key, model, constraints in upsert_jobs:
            records = data.get(key, [])
            if not records:
                continue
            records = deduplicate_records(records, constraints)
            logger.info("Upserting %d %s...", len(records), label)
            for i in range(0, len(records), chunk_size):
                upsert_ops.upsert_batch(
                    model=model,
                    records=records[i:i + chunk_size],
                    constraint_columns=constraints,
                    chunk_size=chunk_size,
                )


# =============================================================================
# Public orchestration entry point
# =============================================================================

def run(
    mode: str = 'incremental',
    start: str = None,
    end: str = None,
    days_back: int = 7,
    location_codes: Optional[List[str]] = None,
    all_tenants: bool = False,
    since: Optional[str] = None,
) -> Dict[str, Any]:
    """Sync ccws_ledgers + ccws_charges from CallCenterWs.

    Returns {'records': int, 'ledgers': int, 'charges': int, 'new_tenants': int}
    """
    from common import DataLayerConfig, SOAPClient
    from common.db import get_engine

    config = DataLayerConfig.from_env()
    if not config.soap:
        raise ValueError("SOAP configuration not found")

    # Location codes: argument > pipeline config > error
    if not location_codes:
        location_codes = _get_pipeline_config('location_codes', [])
    if not location_codes:
        raise ValueError("ccws_ledgers: no location_codes configured")

    chunk_size = _get_pipeline_config('sql_chunk_size', 500)
    extract_date = date.today()

    # Determine incremental threshold
    incremental_since: Optional[datetime] = None
    if mode == 'incremental':
        if since:
            incremental_since = datetime.strptime(since, '%Y-%m-%d')
        else:
            incremental_since = datetime.now() - timedelta(days=days_back)

    cc_url = get_callcenter_url(config.soap.base_url)
    soap_client = SOAPClient(
        base_url=cc_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries,
    )

    logger.info("ccws_ledgers starting mode=%s locations=%s extract_date=%s",
                mode, location_codes, extract_date)
    if incremental_since:
        logger.info("ccws_ledgers incremental_since=%s", incremental_since.strftime('%Y-%m-%d %H:%M:%S'))

    engine = get_engine('pbi')

    # Build location -> SiteID map
    site_map: Dict[str, int] = {}
    for loc in location_codes:
        sid = get_site_id_for_location(engine, loc)
        if sid:
            site_map[loc] = sid
        else:
            logger.warning("ccws_ledgers: no SiteID found for %s in siteinfo, skipping", loc)

    all_data: Dict[str, List[Dict]] = {'ccws_ledgers': [], 'ccws_charges': []}

    try:
        for location_code in location_codes:
            site_id = site_map.get(location_code)
            if not site_id:
                continue
            loc_data = process_location(
                soap_client=soap_client,
                engine=engine,
                location_code=location_code,
                site_id=site_id,
                extract_date=extract_date,
                incremental_since=incremental_since,
                active_only=not all_tenants,
            )
            for key in all_data:
                all_data[key].extend(loc_data.get(key, []))
            logger.info("[%s] summary: %d ledgers, %d charges",
                        location_code,
                        len(loc_data['ccws_ledgers']),
                        len(loc_data['ccws_charges']))

        push_to_database(all_data, engine, chunk_size)

        new_tenants = discover_and_backfill_new_tenants(
            soap_client=soap_client,
            engine=engine,
            location_codes=location_codes,
            extract_date=extract_date,
            chunk_size=chunk_size,
        )
    finally:
        soap_client.close()

    total_ledgers = len(all_data['ccws_ledgers'])
    total_charges = len(all_data['ccws_charges'])
    total = total_ledgers + total_charges

    logger.info("ccws_ledgers complete: ledgers=%d charges=%d new_tenants=%d total=%d",
                total_ledgers, total_charges, new_tenants, total)

    return {
        'records': total,
        'ledgers': total_ledgers,
        'charges': total_charges,
        'new_tenants': new_tenants,
        'mode': mode,
    }


# =============================================================================
# Pipeline class
# =============================================================================

class CcwsLedgersPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'incremental')
        days_back = scope.get('days_back', 7)
        location_codes = scope.get('location_codes')
        since = scope.get('since')
        all_tenants = scope.get('all_tenants', False)

        result = run(
            mode=mode,
            days_back=days_back,
            location_codes=location_codes,
            all_tenants=all_tenants,
            since=since,
        )

        return RunResult(
            status='refreshed',
            records=result['records'],
            scope=scope,
            metadata={
                'mode': mode,
                'ledgers': result['ledgers'],
                'charges': result['charges'],
                'new_tenants': result['new_tenants'],
            },
        )
