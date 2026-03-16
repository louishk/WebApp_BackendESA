"""
Tenant-Ledger-Charges to SQL Pipeline

Fetches tenant, ledger, and charge data using 3-step workflow:
1. TenantList -> Get all tenants for a location
2. LedgersByTenantID_v3 -> For each tenant, get ledger(s)
3. ChargesAllByLedgerID -> For each ledger, get all charges

Features:
- Uses CallCenterWs endpoints (not ReportingWs)
- Two modes: full (historical) and incremental (ongoing refresh)
- Maintains TenantID -> LedgerID -> ChargeID relationships
- Upserts to PostgreSQL

Usage:
    # Full sync - fetch all data (for initial load / historical)
    python tenant_ledger_charges_to_sql.py --mode full --location L001

    # Incremental sync - only charges for recently updated ledgers
    python tenant_ledger_charges_to_sql.py --mode incremental --location L001

    # Incremental with specific date threshold
    python tenant_ledger_charges_to_sql.py --mode incremental --location L001 --since 2026-01-01

Configuration (in .env):
    - SOAP_* : SOAP API connection settings
    - CHARGES_LOCATION_CODES: Comma-separated location codes
    - CHARGES_SQL_CHUNK_SIZE: Batch size for upsert (default: 500)
"""

import argparse
import concurrent.futures
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple, Optional
from decimal import Decimal
from decouple import config as env_config, Csv
from tqdm import tqdm

logger = logging.getLogger(__name__)

from sqlalchemy import text as sa_text

from common import (
    DataLayerConfig,
    SOAPClient,
    REPORT_REGISTRY,
    create_engine_from_config,
    SessionManager,
    UpsertOperations,
    Base,
    Tenant,
    Ledger,
    Charge,
    CcwsTenant,
    CcwsLedger,
    CcwsCharge,
    SiteInfo,
    # Data utilities
    convert_to_bool,
    convert_to_int,
    convert_to_decimal,
    convert_to_datetime,
    deduplicate_records,
)


# =============================================================================
# Configuration
# =============================================================================

# CallCenterWs service URL (derived from ReportingWs URL)
def get_callcenter_url(reporting_url: str) -> str:
    """Convert ReportingWs URL to CallCenterWs URL."""
    return reporting_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')


# =============================================================================
# Transformation Functions
# =============================================================================

def transform_tenant(record: Dict[str, Any], location_code: str) -> Dict[str, Any]:
    """Transform TenantList API record to database format."""
    return {
        'SiteID': convert_to_int(record.get('SiteID')),
        'TenantID': convert_to_int(record.get('TenantID')),
        'sLocationCode': location_code,
        'sFName': record.get('sFName'),
        'sMI': record.get('sMI'),
        'sLName': record.get('sLName'),
        'sCompany': record.get('sCompany'),
        'sAddr1': record.get('sAddr1'),
        'sAddr2': record.get('sAddr2'),
        'sCity': record.get('sCity'),
        'sRegion': record.get('sRegion'),
        'sPostalCode': record.get('sPostalCode'),
        'sPhone': record.get('sPhone'),
        'sEmail': record.get('sEmail'),
        'sMobile': record.get('sMobile'),
        'sLicense': record.get('sLicense'),
        'sAccessCode': record.get('sAccessCode'),
    }


def transform_ledger(record: Dict[str, Any], tenant_id: int, extract_date: date) -> Dict[str, Any]:
    """Transform LedgersByTenantID_v3 API record to database format (expanded fields)."""
    return {
        # Primary Key
        'SiteID': convert_to_int(record.get('SiteID')),
        'LedgerID': convert_to_int(record.get('LedgerID')),

        # Foreign Keys
        'TenantID': tenant_id,
        'unitID': convert_to_int(record.get('UnitID')),
        'EmployeeID': convert_to_int(record.get('EmployeeID')),

        # Unit Information
        'sUnitName': record.get('sUnitName'),

        # Tenant Name
        'TenantName': record.get('TenantName'),
        'sMrMrs': record.get('sMrMrs'),
        'sFName': record.get('sFName'),
        'sMI': record.get('sMI'),
        'sLName': record.get('sLName'),
        'sCompany': record.get('sCompany'),

        # Primary Address
        'sAddr1': record.get('sAddr1'),
        'sAddr2': record.get('sAddr2'),
        'sCity': record.get('sCity'),
        'sRegion': record.get('sRegion'),
        'sPostalCode': record.get('sPostalCode'),
        'sCountry': record.get('sCountry'),

        # Contact Information
        'sPhone': record.get('sPhone'),
        'sMobile': record.get('sMobile'),
        'sEmail': record.get('sEmail'),
        'sFax': record.get('sFax'),

        # Access
        'sAccessCode': record.get('sAccessCode'),
        'sAccessCode2': record.get('sAccessCode2'),

        # Financial Information
        'dcRent': convert_to_decimal(record.get('dcRent')),
        'dcChargeBalance': convert_to_decimal(record.get('dcChargeBalance')),
        'dcTotalDue': convert_to_decimal(record.get('dcTotalDue')),
        'dcTaxRateRent': convert_to_decimal(record.get('dcTaxRateRent')),
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),
        'dcTaxRateInsurance': convert_to_decimal(record.get('dcTaxRateInsurance')),

        # Dates
        'dMovedIn': convert_to_datetime(record.get('dMovedIn')),
        'dPaidThru': convert_to_datetime(record.get('dPaidThru')),
        'dAnniv': convert_to_datetime(record.get('dAnniv')),
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),

        # Billing Info
        'sBillingFrequency': record.get('sBillingFrequency'),
        'iLeaseNum': convert_to_int(record.get('iLeaseNum')),
        'iDefLeaseNum': convert_to_int(record.get('iDefLeaseNum')),
        'bInvoice': convert_to_bool(record.get('bInvoice')),
        'iAutoBillType': convert_to_int(record.get('iAutoBillType')),
        'iInvoiceDeliveryType': convert_to_int(record.get('iInvoiceDeliveryType')),

        # Status Flags
        'bOverlocked': convert_to_bool(record.get('bOverlocked')),
        'bCommercial': convert_to_bool(record.get('bCommercial')),
        'bTaxExempt': convert_to_bool(record.get('bTaxExempt')),
        'bSpecial': convert_to_bool(record.get('bSpecial')),
        'bNeverLockOut': convert_to_bool(record.get('bNeverLockOut')),
        'bCompanyIsTenant': convert_to_bool(record.get('bCompanyIsTenant')),
        'bPermanent': convert_to_bool(record.get('bPermanent')),
        'bExcludeFromInsurance': convert_to_bool(record.get('bExcludeFromInsurance')),
        'bSMSOptIn': convert_to_bool(record.get('bSMSOptIn')),

        # Marketing
        'MarketingID': convert_to_int(record.get('MarketingID')),
        'MktgDistanceID': convert_to_int(record.get('MktgDistanceID')),
        'MktgReasonID': convert_to_int(record.get('MktgReasonID')),
        'MktgTypeID': convert_to_int(record.get('MktgTypeID')),

        # License/Tax
        'sLicense': record.get('sLicense'),
        'sTaxID': record.get('sTaxID'),
        'sTaxExemptCode': record.get('sTaxExemptCode'),

        # Notes
        'sTenNote': record.get('sTenNote'),

        # Coordinates
        'dcLongitude': convert_to_decimal(record.get('dcLongitude')),
        'dcLatitude': convert_to_decimal(record.get('dcLatitude')),

        # Tracking
        'extract_date': extract_date,
    }


def transform_charge(record: Dict[str, Any], ledger_id: int, site_id: int, extract_date: date) -> Dict[str, Any]:
    """Transform ChargesAllByLedgerID API record to database format."""
    # Handle dcPmtAmt - use 0 if None to avoid PK issues
    pmt_amt = convert_to_decimal(record.get('dcPmtAmt'))
    if pmt_amt is None:
        pmt_amt = Decimal('0')

    return {
        'SiteID': site_id,
        'ChargeID': convert_to_int(record.get('ChargeID')),
        'dcPmtAmt': pmt_amt,
        'LedgerID': ledger_id,  # Injected, not from API
        'sChgCategory': record.get('sChgCategory'),
        'sChgDesc': record.get('sChgDesc'),
        'sDefChgDesc': record.get('sDefChgDesc'),
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
        'extract_date': extract_date,
    }


# =============================================================================
# CCWS (API-only) Transform Functions — map ALL fields from each endpoint
# =============================================================================

def transform_ccws_tenant(record: Dict[str, Any], location_code: str, extract_date: date) -> Dict[str, Any]:
    """Transform tenant API record for ccws_tenants (expanded fields).

    Works with both TenantList (17 fields) and GetTenantInfoByTenantID (full record).
    Missing fields from TenantList will just be None — existing data preserved by upsert.
    """
    return {
        'SiteID': convert_to_int(record.get('SiteID')),
        'TenantID': convert_to_int(record.get('TenantID')),

        # Access & Security
        'sAccessCode': record.get('sAccessCode'),
        'sAccessCode2': record.get('sAccessCode2'),
        'iAccessCode2Type': convert_to_int(record.get('iAccessCode2Type')),
        'sWebPassword': record.get('sWebPassword'),
        'bAllowedFacilityAccess': convert_to_bool(record.get('bAllowedFacilityAccess')),

        # Primary Contact
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

        # Alternate Contact
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

        # Business Contact
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

        # Additional Contact
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

        # Identification
        'sLicense': record.get('sLicense'),
        'sLicRegion': record.get('sLicRegion'),
        'sSSN': record.get('sSSN'),
        'sTaxID': record.get('sTaxID'),
        'sTaxExemptCode': record.get('sTaxExemptCode'),
        'dDOB': convert_to_datetime(record.get('dDOB')),
        'iGender': convert_to_int(record.get('iGender')),
        'sEmployer': record.get('sEmployer'),

        # Status Flags
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

        # Marketing
        'MarketingID': convert_to_int(record.get('MarketingID')),
        'MktgDistanceID': convert_to_int(record.get('MktgDistanceID')),
        'MktgWhatID': convert_to_int(record.get('MktgWhatID')),
        'MktgReasonID': convert_to_int(record.get('MktgReasonID')),
        'MktgWhyID': convert_to_int(record.get('MktgWhyID')),
        'MktgTypeID': convert_to_int(record.get('MktgTypeID')),
        'iHowManyOtherStorageCosDidYouContact': convert_to_int(record.get('iHowManyOtherStorageCosDidYouContact')),
        'iUsedSelfStorageInThePast': convert_to_int(record.get('iUsedSelfStorageInThePast')),
        'iMktg_DidYouVisitWebSite': convert_to_int(record.get('iMktg_DidYouVisitWebSite')),

        # Exit Survey
        'bExit_OnEmailOfferList': convert_to_bool(record.get('bExit_OnEmailOfferList')),
        'iExitSat_Cleanliness': convert_to_int(record.get('iExitSat_Cleanliness')),
        'iExitSat_Safety': convert_to_int(record.get('iExitSat_Safety')),
        'iExitSat_Services': convert_to_int(record.get('iExitSat_Services')),
        'iExitSat_Staff': convert_to_int(record.get('iExitSat_Staff')),
        'iExitSat_Price': convert_to_int(record.get('iExitSat_Price')),

        # Geographic
        'dcLongitude': convert_to_decimal(record.get('dcLongitude')),
        'dcLatitude': convert_to_decimal(record.get('dcLatitude')),

        # Notes & Icons
        'sTenNote': record.get('sTenNote'),
        'sIconList': record.get('sIconList'),

        # Pictures
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

        # Global/National Account
        'iGlobalNum_NationalMasterAccount': convert_to_int(record.get('iGlobalNum_NationalMasterAccount')),
        'iGlobalNum_NationalFranchiseAccount': convert_to_int(record.get('iGlobalNum_NationalFranchiseAccount')),

        # Source Timestamps
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'uTS': record.get('uTS'),

        # ETL Tracking
        'sLocationCode': location_code,
        'extract_date': extract_date,
    }


def transform_ccws_ledger(record: Dict[str, Any], tenant_id: int, extract_date: date) -> Dict[str, Any]:
    """Transform LedgersByTenantID_v3 API record for ccws_ledgers (all 141 API fields)."""
    return {
        # Primary Keys
        'SiteID': convert_to_int(record.get('SiteID')),
        'LedgerID': convert_to_int(record.get('LedgerID')),

        # Foreign Keys / IDs
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

        # Tenant Name
        'TenantName': record.get('TenantName'),
        'sUnitName': record.get('sUnitName'),

        # Primary Contact
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

        # Alternate Contact
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

        # Business Contact
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

        # Additional Contact
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

        # Access & Security
        'sAccessCode': record.get('sAccessCode'),
        'sAccessCode2': record.get('sAccessCode2'),
        'iAccessCode2Type': convert_to_int(record.get('iAccessCode2Type')),

        # Identification
        'sLicense': record.get('sLicense'),
        'sLicRegion': record.get('sLicRegion'),
        'sSSN': record.get('sSSN'),
        'sTaxID': record.get('sTaxID'),
        'sTaxExemptCode': record.get('sTaxExemptCode'),
        'dDOB': convert_to_datetime(record.get('dDOB')),
        'iGender': convert_to_int(record.get('iGender')),

        # Status Flags
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

        # Lease & Dates
        'iLeaseNum': convert_to_int(record.get('iLeaseNum')),
        'iDefLeaseNum': convert_to_int(record.get('iDefLeaseNum')),
        'dMovedIn': convert_to_datetime(record.get('dMovedIn')),
        'dPaidThru': convert_to_datetime(record.get('dPaidThru')),
        'dSchedOut': convert_to_datetime(record.get('dSchedOut')),
        'dAnniv': convert_to_datetime(record.get('dAnniv')),
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),

        # Financial
        'dcRent': convert_to_decimal(record.get('dcRent')),
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),
        'dcChargeBalance': convert_to_decimal(record.get('dcChargeBalance')),
        'dcTotalDue': convert_to_decimal(record.get('dcTotalDue')),
        'dcTaxRateRent': convert_to_decimal(record.get('dcTaxRateRent')),
        'dcTaxRateInsurance': convert_to_decimal(record.get('dcTaxRateInsurance')),
        'sBillingFrequency': record.get('sBillingFrequency'),

        # Billing
        'iAutoBillType': convert_to_int(record.get('iAutoBillType')),
        'iInvoiceDeliveryType': convert_to_int(record.get('iInvoiceDeliveryType')),

        # Marketing
        'iHowManyOtherStorageCosDidYouContact': convert_to_int(record.get('iHowManyOtherStorageCosDidYouContact')),
        'iUsedSelfStorageInThePast': convert_to_int(record.get('iUsedSelfStorageInThePast')),
        'iMktg_DidYouVisitWebSite': convert_to_int(record.get('iMktg_DidYouVisitWebSite')),

        # Exit Survey
        'bExit_OnEmailOfferList': convert_to_bool(record.get('bExit_OnEmailOfferList')),
        'iExitSat_Cleanliness': convert_to_int(record.get('iExitSat_Cleanliness')),
        'iExitSat_Price': convert_to_int(record.get('iExitSat_Price')),
        'iExitSat_Safety': convert_to_int(record.get('iExitSat_Safety')),
        'iExitSat_Services': convert_to_int(record.get('iExitSat_Services')),
        'iExitSat_Staff': convert_to_int(record.get('iExitSat_Staff')),

        # Blacklist / Events
        'iBlackListRating': convert_to_int(record.get('iBlackListRating')),
        'iTenEvents_OptOut': convert_to_int(record.get('iTenEvents_OptOut')),

        # Geographic
        'dcLatitude': convert_to_decimal(record.get('dcLatitude')),
        'dcLongitude': convert_to_decimal(record.get('dcLongitude')),

        # Notes & Icons
        'sTenNote': record.get('sTenNote'),
        'sIconList': record.get('sIconList'),

        # Pictures
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

        # Source Timestamps
        'uTS': record.get('uTS'),
        'uTSbigint': convert_to_int(record.get('uTSbigint')),

        # Tracking
        'extract_date': extract_date,
    }


def transform_ccws_charge(record: Dict[str, Any], ledger_id: int, site_id: int, extract_date: date) -> Dict[str, Any]:
    """Transform ChargesAllByLedgerID API record for ccws_charges (all 16 API fields)."""
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
# API Call Functions
# =============================================================================

def call_soap_endpoint(
    soap_client: SOAPClient,
    report_name: str,
    parameters: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Make a SOAP call using the registered endpoint configuration."""
    config = REPORT_REGISTRY[report_name]
    return soap_client.call(
        operation=config.operation,
        parameters=parameters,
        soap_action=config.soap_action,
        namespace=config.namespace,
        result_tag=config.result_tag
    )


def fetch_tenants(
    soap_client: SOAPClient,
    location_code: str
) -> List[Dict[str, Any]]:
    """Fetch all tenants for a location."""
    return call_soap_endpoint(
        soap_client,
        'tenant_list',
        {
            'sLocationCode': location_code,
            'sTenantFirstName': '',  # Empty = all tenants
            'sTenantLastName': ''    # Empty = all tenants
        }
    )


def fetch_ledgers_for_tenant(
    soap_client: SOAPClient,
    location_code: str,
    tenant_id: int
) -> List[Dict[str, Any]]:
    """Fetch ledgers for a specific tenant."""
    return call_soap_endpoint(
        soap_client,
        'ledgers_by_tenant_id_v3',
        {
            'sLocationCode': location_code,
            'sTenantID': str(tenant_id)
        }
    )


def fetch_charges_for_ledger(
    soap_client: SOAPClient,
    location_code: str,
    ledger_id: int
) -> List[Dict[str, Any]]:
    """Fetch all charges for a specific ledger."""
    return call_soap_endpoint(
        soap_client,
        'charges_all_by_ledger_id',
        {
            'sLocationCode': location_code,
            'ledgerId': ledger_id
        }
    )


def fetch_tenant_info(
    soap_client: SOAPClient,
    location_code: str,
    tenant_id: int
) -> Optional[Dict[str, Any]]:
    """Fetch full tenant info by TenantID. Returns first record or None."""
    try:
        results = call_soap_endpoint(
            soap_client,
            'tenant_info_by_tenant_id',
            {
                'sLocationCode': location_code,
                'iTenantID': tenant_id
            }
        )
        if results:
            return results[0]
        return None
    except Exception as e:
        logger.warning(f"Failed to fetch tenant info for {location_code}/TenantID={tenant_id}: {e}")
        return None


# =============================================================================
# Phase C: Discover new tenants from rentroll not yet in ccws_tenants
# =============================================================================

def discover_and_backfill_new_tenants(
    soap_client: SOAPClient,
    engine,
    location_codes: List[str],
    extract_date: date,
    chunk_size: int = 500
) -> int:
    """
    Check rentroll for TenantIDs not yet in ccws_tenants.
    INSERT new IDs, then call GetTenantInfoByTenantID to fill details.
    Returns count of new tenants added.
    """
    logger.info("Phase C: Discovering new tenants from rentroll...")

    # Build SiteID → location_code map
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            'SELECT "SiteID", "SiteCode" FROM siteinfo WHERE "SiteCode" IS NOT NULL'
        )).fetchall()
    site_to_location = {row[0]: row[1] for row in rows}

    # Find TenantIDs in rentroll that don't exist in ccws_tenants
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
        logger.info("  No new tenants found in rentroll.")
        return 0

    logger.info(f"  Found {len(new_pairs)} new (SiteID, TenantID) pairs")

    # Insert skeleton rows + call GetTenantInfoByTenantID for each
    added = 0
    session_manager = SessionManager(engine)

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, 'postgresql')

        ccws_tenant_records = []
        with tqdm(total=len(new_pairs), desc="  New tenants", unit="t") as pbar:
            for site_id, tenant_id in new_pairs:
                location_code = site_to_location.get(site_id)
                if not location_code:
                    pbar.update(1)
                    continue

                # Try to get full data via API
                raw = fetch_tenant_info(soap_client, location_code, tenant_id)
                if raw:
                    transformed = transform_ccws_tenant(raw, location_code, extract_date)
                else:
                    # Insert skeleton with just IDs
                    transformed = {
                        'SiteID': site_id,
                        'TenantID': tenant_id,
                        'sLocationCode': location_code,
                        'extract_date': extract_date,
                    }

                if transformed.get('TenantID'):
                    ccws_tenant_records.append(transformed)
                    added += 1

                pbar.update(1)

        # Upsert all at once
        if ccws_tenant_records:
            ccws_tenant_records = deduplicate_records(ccws_tenant_records, ['SiteID', 'TenantID'])
            logger.info(f"  Upserting {len(ccws_tenant_records)} new ccws_tenants...")
            for i in range(0, len(ccws_tenant_records), chunk_size):
                chunk = ccws_tenant_records[i:i + chunk_size]
                upsert_ops.upsert_batch(
                    model=CcwsTenant,
                    records=chunk,
                    constraint_columns=['SiteID', 'TenantID'],
                    chunk_size=chunk_size
                )

    logger.info(f"  Phase C complete: {added} new tenants added")
    return added


# =============================================================================
# Data Pipeline Functions
# =============================================================================

def process_location(
    soap_client: SOAPClient,
    location_code: str,
    extract_date: date,
    incremental_since: Optional[datetime] = None,
    max_workers: int = 10
) -> Dict[str, List[Dict]]:
    """
    Process a single location through the 3-step workflow.

    Args:
        soap_client: SOAP client instance
        location_code: Location code (e.g., L001)
        extract_date: Date for extract_date field
        incremental_since: If set, only fetch charges for ledgers with dUpdated > this datetime
        max_workers: Max concurrent workers (not used yet)

    Returns:
        Dict with keys: tenants, ledgers, charges (legacy cc_*)
                        ccws_tenants, ccws_ledgers, ccws_charges (API-only)
    """
    result = {
        'tenants': [], 'ledgers': [], 'charges': [],
        'ccws_tenants': [], 'ccws_ledgers': [], 'ccws_charges': [],
    }

    mode_label = "INCREMENTAL" if incremental_since else "FULL"

    # Step 1: Fetch all tenants (always full refresh - fast)
    logger.info("  Step 1: Fetching tenants...")
    try:
        raw_tenants = fetch_tenants(soap_client, location_code)
        tenant_ids = []
        for t in raw_tenants:
            transformed = transform_tenant(t, location_code)
            if transformed['TenantID']:
                result['tenants'].append(transformed)
                tenant_ids.append(transformed['TenantID'])
            # CCWS: all API fields
            ccws_t = transform_ccws_tenant(t, location_code, extract_date)
            if ccws_t['TenantID']:
                result['ccws_tenants'].append(ccws_t)
        logger.info(f"    Found {len(tenant_ids)} tenants")
    except Exception as e:
        logger.error(f"    ERROR fetching tenants: {e}")
        return result

    if not tenant_ids:
        logger.info("    No tenants found, skipping...")
        return result

    # Step 2: Fetch ledgers for all tenants (always full - catches new ledgers)
    logger.info(f"  Step 2: Fetching ledgers for {len(tenant_ids)} tenants...")
    ledger_to_site = {}  # Map ledger_id -> site_id for charge fetching
    ledgers_for_charges = []  # Ledgers that need charge refresh (raw records)

    with tqdm(total=len(tenant_ids), desc="    Tenants", unit="t") as pbar:
        for tenant_id in tenant_ids:
            try:
                raw_ledgers = fetch_ledgers_for_tenant(soap_client, location_code, tenant_id)
            except Exception as e:
                logger.warning(f"Failed to fetch ledgers for tenant {tenant_id} at {location_code}: {e}")
                pbar.update(1)
                continue

            for raw_l in raw_ledgers:
                # Legacy cc_ledgers transform
                transformed = transform_ledger(raw_l, tenant_id, extract_date)
                if not transformed['LedgerID']:
                    continue
                result['ledgers'].append(transformed)
                ledger_to_site[transformed['LedgerID']] = transformed['SiteID']

                # CCWS: all 141 API fields
                ccws_l = transform_ccws_ledger(raw_l, tenant_id, extract_date)
                result['ccws_ledgers'].append(ccws_l)

                # Determine if charges need refresh
                if incremental_since:
                    ledger_created = transformed.get('dCreated')
                    ledger_updated = transformed.get('dUpdated')
                    is_new = ledger_created and ledger_created > incremental_since
                    is_modified = ledger_updated and ledger_updated > incremental_since
                    if is_new or is_modified:
                        ledgers_for_charges.append(transformed)
                else:
                    ledgers_for_charges.append(transformed)

            pbar.update(1)

    logger.info(f"    Found {len(result['ledgers'])} ledgers total")

    if incremental_since:
        logger.info(f"    Ledgers created/updated since {incremental_since.date()}: {len(ledgers_for_charges)}")

    if not ledgers_for_charges:
        logger.info("    No ledgers need charge refresh, skipping...")
        return result

    # Step 3: Fetch charges for selected ledgers
    ledger_ids_for_charges = [l['LedgerID'] for l in ledgers_for_charges]
    logger.info(f"  Step 3: Fetching charges for {len(ledger_ids_for_charges)} ledgers...")

    with tqdm(total=len(ledger_ids_for_charges), desc="    Ledgers", unit="l") as pbar:
        for ledger_id in ledger_ids_for_charges:
            try:
                site_id = ledger_to_site.get(ledger_id)
                raw_charges = fetch_charges_for_ledger(soap_client, location_code, ledger_id)
                for c in raw_charges:
                    # Legacy cc_charges
                    transformed = transform_charge(c, ledger_id, site_id, extract_date)
                    if transformed['ChargeID']:
                        result['charges'].append(transformed)
                    # CCWS: all 16 API fields
                    ccws_c = transform_ccws_charge(c, ledger_id, site_id, extract_date)
                    if ccws_c['ChargeID']:
                        result['ccws_charges'].append(ccws_c)
            except Exception as e:
                logger.warning(f"Failed to fetch charges for ledger {ledger_id}: {e}")
            pbar.update(1)

    logger.info(f"    Found {len(result['charges'])} charges")

    return result


# =============================================================================
# Database Operations
# =============================================================================

def push_to_database(
    data: Dict[str, List[Dict]],
    config: DataLayerConfig,
    chunk_size: int = 500
) -> None:
    """Push all data to PostgreSQL database (legacy cc_* and new ccws_* tables)."""
    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    # Create tables if not exist
    logger.info("  Preparing database tables...")
    Base.metadata.create_all(engine, tables=[
        Tenant.__table__,
        Ledger.__table__,
        Charge.__table__,
        CcwsTenant.__table__,
        CcwsLedger.__table__,
        CcwsCharge.__table__,
    ])
    logger.info("    Tables ready")

    session_manager = SessionManager(engine)

    # Define upsert jobs: (label, records_key, model, constraint_columns)
    upsert_jobs = [
        ('cc_tenants', 'tenants', Tenant, ['SiteID', 'TenantID']),
        ('cc_ledgers', 'ledgers', Ledger, ['SiteID', 'LedgerID']),
        ('cc_charges', 'charges', Charge, ['SiteID', 'ChargeID', 'dcPmtAmt']),
        ('ccws_tenants', 'ccws_tenants', CcwsTenant, ['SiteID', 'TenantID']),
        ('ccws_ledgers', 'ccws_ledgers', CcwsLedger, ['SiteID', 'LedgerID']),
        ('ccws_charges', 'ccws_charges', CcwsCharge, ['SiteID', 'ChargeID', 'dcPmtAmt']),
    ]

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        for label, key, model, constraints in upsert_jobs:
            records = data.get(key, [])
            if not records:
                continue

            records = deduplicate_records(records, constraints)
            logger.info(f"  Upserting {len(records)} {label}...")
            with tqdm(total=len(records), desc=f"    {label}", unit="rec") as pbar:
                for i in range(0, len(records), chunk_size):
                    chunk = records[i:i + chunk_size]
                    upsert_ops.upsert_batch(
                        model=model,
                        records=chunk,
                        constraint_columns=constraints,
                        chunk_size=chunk_size
                    )
                    pbar.update(len(chunk))


# =============================================================================
# CLI and Main
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Tenant-Ledger-Charges to SQL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full sync - all data (for initial load / historical)
  python tenant_ledger_charges_to_sql.py --mode full --location L001

  # Incremental sync - only charges for recently updated ledgers (default: 7 days)
  python tenant_ledger_charges_to_sql.py --mode incremental --location L001

  # Incremental with specific date threshold
  python tenant_ledger_charges_to_sql.py --mode incremental --location L001 --since 2026-01-01

  # Incremental with days back
  python tenant_ledger_charges_to_sql.py --mode incremental --location L001 --days-back 30
        """
    )

    parser.add_argument(
        '--mode',
        choices=['full', 'incremental'],
        required=True,
        help='Extraction mode: full (all data) or incremental (only recently updated)'
    )

    parser.add_argument(
        '--location',
        type=str,
        default=None,
        help='Specific location code to process (default: all from CHARGES_LOCATION_CODES)'
    )

    parser.add_argument(
        '--since',
        type=str,
        default=None,
        help='For incremental mode: only process ledgers updated since this date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--days-back',
        type=int,
        default=7,
        help='For incremental mode: days to look back for updated ledgers (default: 7)'
    )

    args = parser.parse_args()

    # Parse --since date if provided
    if args.since:
        try:
            args.since_datetime = datetime.strptime(args.since, '%Y-%m-%d')
        except ValueError:
            parser.error(f"Invalid date format for --since: {args.since}. Use YYYY-MM-DD")
    else:
        args.since_datetime = None

    return args


def main():
    """Main function to run the tenant-ledger-charges pipeline."""
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    args = parse_args()

    # Load configuration
    config = DataLayerConfig.from_env()

    if not config.soap:
        raise ValueError("SOAP configuration not found in .env")

    # Get CallCenterWs URL
    cc_url = get_callcenter_url(config.soap.base_url)

    # Load location codes
    if args.location:
        location_codes = [args.location]
    else:
        location_codes = env_config('CHARGES_LOCATION_CODES', default='', cast=Csv())
        if not location_codes:
            location_codes = env_config('RENTROLL_LOCATION_CODES', cast=Csv())

    chunk_size = env_config('CHARGES_SQL_CHUNK_SIZE', default=500, cast=int)
    extract_date = date.today()

    # Determine incremental threshold
    incremental_since = None
    if args.mode == 'incremental':
        if args.since_datetime:
            incremental_since = args.since_datetime
        else:
            # Default: look back N days
            incremental_since = datetime.now() - timedelta(days=args.days_back)

    # Initialize SOAP client with CallCenterWs URL
    soap_client = SOAPClient(
        base_url=cc_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries
    )

    # Log header
    logger.info("=" * 70)
    logger.info("Tenant-Ledger-Charges to SQL Pipeline")
    logger.info("=" * 70)
    logger.info(f"Mode: {args.mode.upper()}")
    if incremental_since:
        logger.info(f"Incremental Since: {incremental_since.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Extract Date: {extract_date}")
    logger.info(f"Locations: {', '.join(location_codes)}")
    logger.info(f"Service URL: {cc_url}")
    logger.info(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    logger.info("=" * 70)

    # Collect all data across locations
    all_data = {
        'tenants': [], 'ledgers': [], 'charges': [],
        'ccws_tenants': [], 'ccws_ledgers': [], 'ccws_charges': [],
    }

    for location_code in location_codes:
        logger.info(f"\n[{location_code}] Processing...")

        loc_data = process_location(
            soap_client=soap_client,
            location_code=location_code,
            extract_date=extract_date,
            incremental_since=incremental_since
        )

        for key in all_data:
            all_data[key].extend(loc_data.get(key, []))

        logger.info(f"  Summary: {len(loc_data['tenants'])} tenants, "
                    f"{len(loc_data['ledgers'])} ledgers, {len(loc_data['charges'])} charges")

    # Push to database
    logger.info("-" * 70)
    logger.info("Pushing data to database...")
    push_to_database(all_data, config, chunk_size)

    # Phase C: Discover new tenants from rentroll not yet in ccws_tenants
    logger.info("-" * 70)
    db_config = config.databases.get('postgresql')
    engine = create_engine_from_config(db_config)
    new_tenants = discover_and_backfill_new_tenants(
        soap_client=soap_client,
        engine=engine,
        location_codes=location_codes,
        extract_date=extract_date,
        chunk_size=chunk_size,
    )
    engine.dispose()

    # Close SOAP client
    soap_client.close()

    # Final summary
    logger.info("=" * 70)
    logger.info("Pipeline completed!")
    logger.info(f"  cc_tenants:  {len(all_data['tenants'])}  |  ccws_tenants:  {len(all_data['ccws_tenants'])}")
    logger.info(f"  cc_ledgers:  {len(all_data['ledgers'])}  |  ccws_ledgers:  {len(all_data['ccws_ledgers'])}")
    logger.info(f"  cc_charges:  {len(all_data['charges'])}  |  ccws_charges:  {len(all_data['ccws_charges'])}")
    if new_tenants:
        logger.info(f"  New tenants discovered (Phase C): {new_tenants}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
