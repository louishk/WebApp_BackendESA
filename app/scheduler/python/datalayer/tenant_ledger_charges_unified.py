"""
Tenant-Ledger-Charges Unified Sync Pipeline (Expanded Schema)

Unified script for syncing tenant, ledger, and charge data to PostgreSQL.
Supports two execution modes for different environments:

BACKFILL MODE (Local Computer):
    - Runs on local machine with SQL Server access
    - Fetches ALL columns from local sldbclnt database
    - Tenants: 129 columns, Ledgers: 186 columns, Charges: 33 columns
    - Best for: Initial load, periodic full refresh, historical data recovery

DAILY MODE (VM/Cloud):
    - Runs on VM with API access
    - Fetches data via SOAP API (CallCenterWs)
    - Best for: Daily sync, real-time updates

Both modes push to the same PostgreSQL database with data_source tracking.

Usage:
    # BACKFILL - Run on local computer (has SQL Server access)
    python tenant_ledger_charges_unified.py --mode backfill
    python tenant_ledger_charges_unified.py --mode backfill --location L001

    # DAILY - Run on VM (has API access)
    python tenant_ledger_charges_unified.py --mode daily
    python tenant_ledger_charges_unified.py --mode daily --location L001
"""

import argparse
from pathlib import Path
import urllib.parse
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
from decimal import Decimal
from tqdm import tqdm

from decouple import config as env_config, Csv

# Import vault-aware config for sensitive values
try:
    from common.secrets_vault import vault_config as secure_config
except ImportError:
    secure_config = env_config

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

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
    convert_to_bool,
    convert_to_int,
    convert_to_decimal,
    convert_to_datetime,
    deduplicate_records,
)


# =============================================================================
# Constants
# =============================================================================

DATA_SOURCE_LOCAL_SQL = 'local_sql'
DATA_SOURCE_API = 'api'

SITE_ID_TO_LOCATION = {
    48: "L001", 49: "L002", 63: "L003", 26486: "L004", 1910: "L005",
    2276: "L006", 4183: "L007", 9415: "L008", 10419: "L009", 10777: "L010",
    24411: "L011", 25675: "L013", 26710: "L015", 27903: "L017", 29197: "L018",
    29064: "L019", 32663: "L020", 33881: "L021", 38782: "L022", 39284: "L023",
    40100: "L024", 43344: "L025", 44449: "L026", 52421: "L028", 54219: "L029",
    57451: "L030",
}
LOCATION_TO_SITE_ID = {v: k for k, v in SITE_ID_TO_LOCATION.items()}


# =============================================================================
# Local SQL Server Connection (with SSH Tunnel Support)
# =============================================================================

import platform
import time

# Optional SSH tunnel support
try:
    from sshtunnel import SSHTunnelForwarder
    SSH_AVAILABLE = True
except ImportError:
    SSH_AVAILABLE = False
    SSHTunnelForwarder = None

# Global SSH tunnel reference
_ssh_tunnel = None

def get_sql_driver() -> str:
    """Get the appropriate SQL Server driver for the current platform."""
    if platform.system() == 'Windows':
        return "SQL Server"
    else:
        # Linux/WSL - use ODBC Driver 18
        return "ODBC Driver 18 for SQL Server"

def strip_env_comment(value: str) -> str:
    """Strip inline comments from env values (e.g., 'value # comment' -> 'value')."""
    if '#' in value:
        return value.split('#')[0].strip()
    return value.strip()

def create_ssh_tunnel() -> int:
    """Create SSH tunnel to remote VM with SQL Server. Returns local port."""
    global _ssh_tunnel

    if not SSH_AVAILABLE:
        raise ImportError("SSH tunnel requires 'sshtunnel' package. Run: pip install sshtunnel")

    ssh_host = strip_env_comment(env_config('VM_SSH_HOST'))
    ssh_port = int(strip_env_comment(env_config('VM_SSH_PORT', default='22')))
    ssh_username = strip_env_comment(env_config('VM_SSH_USERNAME'))
    ssh_password = strip_env_comment(secure_config('VM_SSH_PASSWORD'))  # From vault

    vm_sql_port = int(env_config('VM_SQL_PORT', default='1433'))
    local_tunnel_port = int(env_config('VM_LOCAL_TUNNEL_PORT', default='9999'))

    print(f"  Creating SSH tunnel to {ssh_host}...")
    print(f"    SSH User: {ssh_username}")
    print(f"    Remote SQL Port: {vm_sql_port}")
    print(f"    Local Tunnel Port: {local_tunnel_port}")

    _ssh_tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        remote_bind_address=('127.0.0.1', vm_sql_port),
        local_bind_address=('127.0.0.1', local_tunnel_port),
        set_keepalive=30
    )

    _ssh_tunnel.start()
    time.sleep(2)  # Give tunnel time to establish

    print(f"  SSH tunnel established: localhost:{local_tunnel_port} -> {ssh_host}:{vm_sql_port}")
    return local_tunnel_port

def close_ssh_tunnel():
    """Close the SSH tunnel if active."""
    global _ssh_tunnel
    if _ssh_tunnel:
        _ssh_tunnel.stop()
        _ssh_tunnel = None
        print("  SSH tunnel closed")

def create_local_sql_engine(
    server: Optional[str] = None,
    database: Optional[str] = None,
    driver: Optional[str] = None,
    use_ssh: bool = False
) -> Engine:
    """Create SQLAlchemy engine for local SQL Server.

    Args:
        server: SQL Server hostname (ignored if use_ssh=True)
        database: Database name
        driver: ODBC driver name
        use_ssh: If True, connect via SSH tunnel to VM
    """
    driver = driver or env_config('VM_SQL_DRIVER', default=get_sql_driver())

    if use_ssh:
        # SSH tunnel mode - connect to VM's SQL Server
        local_port = create_ssh_tunnel()
        server = '127.0.0.1'
        database = database or env_config('VM_DATABASE_NAME', default='sldbclnt')

        driver_encoded = urllib.parse.quote_plus(driver)
        connection_url = (
            f"mssql+pyodbc://@{server}:{local_port}/{database}"
            f"?driver={driver_encoded}&Trusted_Connection=yes&TrustServerCertificate=yes"
            f"&Connection Timeout=60&Encrypt=no"
        )
    else:
        # Direct local connection (Windows only)
        server = server or env_config('LOCAL_SQL_SERVER', default=r"LOUISVER-T14\VSDOTNET")
        database = database or env_config('LOCAL_SQL_DATABASE', default="sldbclnt")

        driver_encoded = urllib.parse.quote_plus(driver)
        connection_url = (
            f"mssql+pyodbc://@{server}/{database}"
            f"?driver={driver_encoded}&Trusted_Connection=yes&TrustServerCertificate=yes&Connection Timeout=60"
        )

    print(f"  Driver: {driver}")
    print(f"  Server: {server}")
    print(f"  Database: {database}")
    print(f"  Mode: {'SSH Tunnel' if use_ssh else 'Direct'}")

    engine = create_engine(connection_url, pool_size=5, max_overflow=10,
                           pool_timeout=60, pool_recycle=1800, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


# =============================================================================
# Local SQL Fetch Functions (ALL COLUMNS)
# =============================================================================

def fetch_tenants_from_local_sql(engine: Engine, site_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
    """Fetch ALL tenant columns from local SQL Server."""
    where_clause = f"WHERE SiteID IN ({','.join(str(s) for s in site_ids)})" if site_ids else ""

    query = text(f"""
        SELECT
            TenantID, SiteID, EmployeeID, sAccessCode, sWebPassword,
            sMrMrs, sFName, sMI, sLName, sCompany,
            sAddr1, sAddr2, sCity, sRegion, sPostalCode, sCountry, sPhone,
            sMrMrsAlt, sFNameAlt, sMIAlt, sLNameAlt,
            sAddr1Alt, sAddr2Alt, sCityAlt, sRegionAlt, sPostalCodeAlt, sCountryAlt, sPhoneAlt,
            sEmployer, sMrMrsBus, sFNameBus, sMIBus, sLNameBus, sCompanyBus,
            sAddr1Bus, sAddr2Bus, sCityBus, sRegionBus, sPostalCodeBus, sCountryBus, sPhoneBus,
            sFax, sEmail, sEmailAlt, sEmailBus, sPager, sMobile,
            bCommercial, bTaxExempt, bSpecial, bNeverLockOut, bCompanyIsTenant,
            bOnWaitingList, bNoChecks, dDOB, sIconList, sTaxExemptCode, sTenNote,
            iPrimaryPic, sPicFileN1, sPicFileN2, sPicFileN3, sPicFileN4,
            sPicFileN5, sPicFileN6, sPicFileN7, sPicFileN8, sPicFileN9,
            sLicense, sLicRegion, sSSN, MarketingID, iGender,
            MktgDistanceID, MktgWhatID, MktgReasonID, MktgWhyID, MktgTypeID,
            bPermanent, bWalkInPOS, sWebSecurityQ, sWebSecurityQA,
            dcLongitude, dcLatitude, bSpecialAlert,
            iHowManyOtherStorageCosDidYouContact, iUsedSelfStorageInThePast,
            bPermanentGateLockout, dExit_SurveyTaken, sExit_Comment,
            bExit_OnEmailOfferList, dExit_WhenNeedAgain,
            MktgExitRentAgainID, MktgExitReasonID, MktgExitSatisfactionID,
            sRelationshipAlt, iExitSat_Cleanliness, iExitSat_Safety,
            iExitSat_Services, iExitSat_Staff, iExitSat_Price,
            iMktg_DidYouVisitWebSite, bi_Tenant_GlobalNum, iBlackListRating,
            bSMSOptIn, sCountryCodeMobile, sTaxID, sAccessCode2,
            iGlobalNum_NationalMasterAccount, iGlobalNum_NationalFranchiseAccount,
            iAccessCode2Type, dCreated, dDeleted, dUpdated, dArchived,
            iTenEvents_OptOut, sMrMrsAdd, sFNameAdd, sMIAdd, sLNameAdd,
            sAddr1Add, sAddr2Add, sCityAdd, sRegionAdd, sPostalCodeAdd,
            sCountryAdd, sPhoneAdd, sEmailAdd
        FROM Tenants
        {where_clause}
        ORDER BY SiteID, TenantID
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result]


def fetch_ledgers_from_local_sql(engine: Engine, site_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
    """Fetch ALL ledger columns from local SQL Server.

    Note: TenantID is not available in local SQL (no direct Tenant→Ledger link).
    TenantID will be NULL and can be enriched later via API (LedgersByTenantID).
    """
    where_clause = f"WHERE SiteID IN ({','.join(str(s) for s in site_ids)})" if site_ids else ""

    query = text(f"""
        SELECT
            LedgerID, SiteID, EmployeeID, unitID, BillingFreqID, ConcessionID, PromoRentalID,
            sPurchOrderCode, iLeaseNum, dLease, dMovedIn, dSchedOut, dMovedOut,
            bAnniv, dAnniv, iInByEmpID, iOutByEmpID, iTferByEmpID, iTferToLedID, iTferFromLedID,
            dcInsurPremium, dcSchedRent, dSchedRentStrt, dRentLastChanged, dcRent,
            dcRecChg1, dcRecChg2, dcRecChg3, dcRecChg4, dcRecChg5, dcRecChg6, dcRecChg7, dcRecChg8,
            iRecChg1Qty, iRecChg2Qty, iRecChg3Qty, iRecChg4Qty, iRecChg5Qty, iRecChg6Qty, iRecChg7Qty, iRecChg8Qty,
            dcAdminFee, dcCutLockFee, dcNSFFee, dcAuctionFee,
            dcLateFee1, dcLateFee2, dcLateFee3, dLF1Strt, dLF2Strt, dLF3Strt,
            iLateFeeType, dcPercentLateFee, bLateFee1IsApplied, bLateFee2IsApplied, bLateFee3IsApplied,
            iCreditCardTypeID, sCreditCardNum, dCreditCardExpir, sCreditCardHolderName,
            sCreditCardCVV2, sCreditCardStreet, sCreditCardZip, iCreditCardAVSResult,
            bDisablePDue, dDisablePDueStrt, dDisablePDueEnd,
            iAutoBillType, bHadNSF, nNSF, bTaxRent, bOverlocked, bGateLocked,
            bWaiveInvoiceFee, bInvoice, bInvoiceEmail, iInvoiceDeliveryType, iInvoiceDaysBefore, dInvoiceLast,
            dcSecDepPaid, dcSecDepBal, dcRentBal,
            dcLateFee1Bal, dcLateFee2Bal, dcLateFee3Bal, dcLateFee1CurrBal, dcLateFee2CurrBal, dcLateFee3CurrBal,
            dcNSFBal, dcAdminFeeBal, dcCutLockFeeBal, dcAuctionFeeBal,
            dcRecChg1Bal, dcRecChg2Bal, dcRecChg3Bal, dcRecChg4Bal, dcRecChg5Bal, dcRecChg6Bal, dcRecChg7Bal, dcRecChg8Bal,
            dcInsurBal, dcPOSBal, dcCreditBal, dcOtherBal,
            dcRentTaxBal, dcLateFeeTaxBal, dcOtherTaxBal, dcRecChgTaxBal, dcInsurTaxBal, dcPOSTaxBal,
            dPaidThru, dRentLastChgStrt, dRentLastChgEnd, dInsurLastChgStrt, dInsurLastChgEnd,
            dRecChg1LastChgStrt, dRecChg1LastChgEnd, dRecChg2LastChgStrt, dRecChg2LastChgEnd,
            dRecChg3LastChgStrt, dRecChg3LastChgEnd, dRecChg4LastChgStrt, dRecChg4LastChgEnd,
            dRecChg5LastChgStrt, dRecChg5LastChgEnd, dRecChg6LastChgStrt, dRecChg6LastChgEnd,
            dRecChg7LastChgStrt, dRecChg7LastChgEnd, dRecChg8LastChgStrt, dRecChg8LastChgEnd,
            sLicPlate, sVehicleDesc, sReasonComplimentary,
            sACH_CheckWriterAcctNum, sACH_CheckWriterAcctName, sACH_ABA_RoutingNum, sACH_RDFI, sACH_Check_SavingsCode,
            iProcessDayOfMonth, bAutoBillChargeFee, bAutoBillEmailNotify,
            dcTR_RateIncreaseAmt, dTR_LastRateIncreaseNotice, bExcludeFromRevenueMgmt, iTR_RateIncreasePendingStatus,
            sCompanySub, iAuctionStatus, dAuctionDate, CreditCardID, dInsurPaidThru, dPmtLast, dcPmtLastAmt,
            iLateFeeType1, iLateFeeType2, iLateFeeType3, iLateFeeType4, iLateFeeType5,
            dcPercentLateFee2, dcPercentLateFee3, dcPercentLateFee4, dcPercentLateFee5,
            bLateFee4IsApplied, bLateFee5IsApplied, dLF4Strt, dLF5Strt,
            dcLateFee4, dcLateFee5, dcLateFee4Bal, dcLateFee5Bal, dcLateFee4CurrBal, dcLateFee5CurrBal,
            dcRefundDue, dCreated, dMovedOutExpected, dAutoBillEnabled, dcPushRateAtMoveIn, bPermanent,
            dDeleted, dUpdated, dArchived, dTR_NextRateReview, iRemoveDiscPlanOnSchedRateChange,
            dSchedOutCreated, ACHBankInfoID
        FROM Ledgers
        {where_clause}
        ORDER BY SiteID, LedgerID
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result]


def fetch_charges_from_local_sql(engine: Engine, site_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
    """Fetch ALL charge columns from local SQL Server with ChargeDesc join."""
    where_clause = f"WHERE c.SiteID IN ({','.join(str(s) for s in site_ids)})" if site_ids else ""

    query = text(f"""
        SELECT
            c.ChargeID, c.ChargeDescID, c.SiteID, c.LedgerID, c.InsurLedgerID,
            c.FiscalID, c.ConcessionID, c.EmployeeID, c.ACHID, c.Disc_MemoID,
            c.dcAmt, c.dcTax1, c.dcTax2, c.dcQty, c.dcStdPrice, c.dcPrice, c.dcCost,
            c.dChgStrt, c.dChgEnd, c.dCreated,
            c.bMoveIn, c.bMoveOut, c.bNSF, c.ReceiptID_NSF, c.QTChargeID,
            c.iPromoGlobalNum, c.dcPriceTax1, c.dcPriceTax2,
            c.dArchived, c.dDeleted, c.dUpdated, c.iNSFFlag,
            cd.sChgCategory, cd.sChgDesc, cd.sDefChgDesc
        FROM Charges c
        LEFT JOIN ChargeDesc cd ON c.ChargeDescID = cd.ChargeDescID AND c.SiteID = cd.SiteID
        {where_clause}
        ORDER BY c.SiteID, c.LedgerID, c.ChargeID
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result]


# =============================================================================
# Transform Functions (Local SQL -> PostgreSQL)
# =============================================================================

def transform_tenant_from_local_sql(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform ALL tenant fields from local SQL."""
    location_code = SITE_ID_TO_LOCATION.get(record.get('SiteID'), 'UNKNOWN')

    return {
        'TenantID': convert_to_int(record.get('TenantID')),
        'SiteID': convert_to_int(record.get('SiteID')),
        'EmployeeID': convert_to_int(record.get('EmployeeID')),
        'sAccessCode': record.get('sAccessCode'),
        'sAccessCode2': record.get('sAccessCode2'),
        'iAccessCode2Type': convert_to_int(record.get('iAccessCode2Type')),
        'sWebPassword': record.get('sWebPassword'),
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
        'sEmployer': record.get('sEmployer'),
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
        'dExit_SurveyTaken': convert_to_datetime(record.get('dExit_SurveyTaken')),
        'sExit_Comment': record.get('sExit_Comment'),
        'bExit_OnEmailOfferList': convert_to_bool(record.get('bExit_OnEmailOfferList')),
        'dExit_WhenNeedAgain': convert_to_datetime(record.get('dExit_WhenNeedAgain')),
        'MktgExitRentAgainID': convert_to_int(record.get('MktgExitRentAgainID')),
        'MktgExitReasonID': convert_to_int(record.get('MktgExitReasonID')),
        'MktgExitSatisfactionID': convert_to_int(record.get('MktgExitSatisfactionID')),
        'iExitSat_Cleanliness': convert_to_int(record.get('iExitSat_Cleanliness')),
        'iExitSat_Safety': convert_to_int(record.get('iExitSat_Safety')),
        'iExitSat_Services': convert_to_int(record.get('iExitSat_Services')),
        'iExitSat_Staff': convert_to_int(record.get('iExitSat_Staff')),
        'iExitSat_Price': convert_to_int(record.get('iExitSat_Price')),
        # Web Security
        'sWebSecurityQ': record.get('sWebSecurityQ'),
        'sWebSecurityQA': record.get('sWebSecurityQA'),
        # Geographic
        'dcLongitude': convert_to_decimal(record.get('dcLongitude')),
        'dcLatitude': convert_to_decimal(record.get('dcLatitude')),
        # Notes
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
        # Global Account
        'bi_Tenant_GlobalNum': convert_to_int(record.get('bi_Tenant_GlobalNum')),
        'iGlobalNum_NationalMasterAccount': convert_to_int(record.get('iGlobalNum_NationalMasterAccount')),
        'iGlobalNum_NationalFranchiseAccount': convert_to_int(record.get('iGlobalNum_NationalFranchiseAccount')),
        # Timestamps
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'dDeleted': convert_to_datetime(record.get('dDeleted')),
        'dArchived': convert_to_datetime(record.get('dArchived')),
        # Tracking
        'sLocationCode': location_code,
        'extract_date': extract_date,
        'data_source': DATA_SOURCE_LOCAL_SQL,
    }


def transform_ledger_from_local_sql(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform ALL ledger fields from local SQL.

    Note: TenantID is NULL for local SQL records (no direct link in source DB).
    API can enrich TenantID later via LedgersByTenantID endpoint.
    """
    return {
        'LedgerID': convert_to_int(record.get('LedgerID')),
        'SiteID': convert_to_int(record.get('SiteID')),
        'TenantID': None,  # Not available in local SQL; enriched by API later
        'EmployeeID': convert_to_int(record.get('EmployeeID')),
        'unitID': convert_to_int(record.get('unitID')),
        'BillingFreqID': convert_to_int(record.get('BillingFreqID')),
        'ConcessionID': convert_to_int(record.get('ConcessionID')),
        'PromoRentalID': convert_to_int(record.get('PromoRentalID')),
        'CreditCardID': convert_to_int(record.get('CreditCardID')),
        'ACHBankInfoID': convert_to_int(record.get('ACHBankInfoID')),
        # Lease Info
        'sPurchOrderCode': record.get('sPurchOrderCode'),
        'iLeaseNum': convert_to_int(record.get('iLeaseNum')),
        'dLease': convert_to_datetime(record.get('dLease')),
        'dMovedIn': convert_to_datetime(record.get('dMovedIn')),
        'dSchedOut': convert_to_datetime(record.get('dSchedOut')),
        'dMovedOut': convert_to_datetime(record.get('dMovedOut')),
        'dMovedOutExpected': convert_to_datetime(record.get('dMovedOutExpected')),
        'dSchedOutCreated': convert_to_datetime(record.get('dSchedOutCreated')),
        # Anniversary
        'bAnniv': convert_to_bool(record.get('bAnniv')),
        'dAnniv': convert_to_datetime(record.get('dAnniv')),
        'dPaidThru': convert_to_datetime(record.get('dPaidThru')),
        'dInsurPaidThru': convert_to_datetime(record.get('dInsurPaidThru')),
        'dPmtLast': convert_to_datetime(record.get('dPmtLast')),
        'dcPmtLastAmt': convert_to_decimal(record.get('dcPmtLastAmt')),
        # Employee Tracking
        'iInByEmpID': convert_to_int(record.get('iInByEmpID')),
        'iOutByEmpID': convert_to_int(record.get('iOutByEmpID')),
        'iTferByEmpID': convert_to_int(record.get('iTferByEmpID')),
        'iTferToLedID': convert_to_int(record.get('iTferToLedID')),
        'iTferFromLedID': convert_to_int(record.get('iTferFromLedID')),
        # Rent
        'dcRent': convert_to_decimal(record.get('dcRent')),
        'dcSchedRent': convert_to_decimal(record.get('dcSchedRent')),
        'dSchedRentStrt': convert_to_datetime(record.get('dSchedRentStrt')),
        'dRentLastChanged': convert_to_datetime(record.get('dRentLastChanged')),
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),
        'dcPushRateAtMoveIn': convert_to_decimal(record.get('dcPushRateAtMoveIn')),
        # Recurring Charges
        'dcRecChg1': convert_to_decimal(record.get('dcRecChg1')),
        'dcRecChg2': convert_to_decimal(record.get('dcRecChg2')),
        'dcRecChg3': convert_to_decimal(record.get('dcRecChg3')),
        'dcRecChg4': convert_to_decimal(record.get('dcRecChg4')),
        'dcRecChg5': convert_to_decimal(record.get('dcRecChg5')),
        'dcRecChg6': convert_to_decimal(record.get('dcRecChg6')),
        'dcRecChg7': convert_to_decimal(record.get('dcRecChg7')),
        'dcRecChg8': convert_to_decimal(record.get('dcRecChg8')),
        'iRecChg1Qty': convert_to_int(record.get('iRecChg1Qty')),
        'iRecChg2Qty': convert_to_int(record.get('iRecChg2Qty')),
        'iRecChg3Qty': convert_to_int(record.get('iRecChg3Qty')),
        'iRecChg4Qty': convert_to_int(record.get('iRecChg4Qty')),
        'iRecChg5Qty': convert_to_int(record.get('iRecChg5Qty')),
        'iRecChg6Qty': convert_to_int(record.get('iRecChg6Qty')),
        'iRecChg7Qty': convert_to_int(record.get('iRecChg7Qty')),
        'iRecChg8Qty': convert_to_int(record.get('iRecChg8Qty')),
        # Fees
        'dcAdminFee': convert_to_decimal(record.get('dcAdminFee')),
        'dcCutLockFee': convert_to_decimal(record.get('dcCutLockFee')),
        'dcNSFFee': convert_to_decimal(record.get('dcNSFFee')),
        'dcAuctionFee': convert_to_decimal(record.get('dcAuctionFee')),
        # Late Fees
        'dcLateFee1': convert_to_decimal(record.get('dcLateFee1')),
        'dcLateFee2': convert_to_decimal(record.get('dcLateFee2')),
        'dcLateFee3': convert_to_decimal(record.get('dcLateFee3')),
        'dcLateFee4': convert_to_decimal(record.get('dcLateFee4')),
        'dcLateFee5': convert_to_decimal(record.get('dcLateFee5')),
        'dLF1Strt': convert_to_datetime(record.get('dLF1Strt')),
        'dLF2Strt': convert_to_datetime(record.get('dLF2Strt')),
        'dLF3Strt': convert_to_datetime(record.get('dLF3Strt')),
        'dLF4Strt': convert_to_datetime(record.get('dLF4Strt')),
        'dLF5Strt': convert_to_datetime(record.get('dLF5Strt')),
        'iLateFeeType': convert_to_int(record.get('iLateFeeType')),
        'iLateFeeType1': convert_to_int(record.get('iLateFeeType1')),
        'iLateFeeType2': convert_to_int(record.get('iLateFeeType2')),
        'iLateFeeType3': convert_to_int(record.get('iLateFeeType3')),
        'iLateFeeType4': convert_to_int(record.get('iLateFeeType4')),
        'iLateFeeType5': convert_to_int(record.get('iLateFeeType5')),
        'dcPercentLateFee': convert_to_decimal(record.get('dcPercentLateFee')),
        'dcPercentLateFee2': convert_to_decimal(record.get('dcPercentLateFee2')),
        'dcPercentLateFee3': convert_to_decimal(record.get('dcPercentLateFee3')),
        'dcPercentLateFee4': convert_to_decimal(record.get('dcPercentLateFee4')),
        'dcPercentLateFee5': convert_to_decimal(record.get('dcPercentLateFee5')),
        'bLateFee1IsApplied': convert_to_bool(record.get('bLateFee1IsApplied')),
        'bLateFee2IsApplied': convert_to_bool(record.get('bLateFee2IsApplied')),
        'bLateFee3IsApplied': convert_to_bool(record.get('bLateFee3IsApplied')),
        'bLateFee4IsApplied': convert_to_bool(record.get('bLateFee4IsApplied')),
        'bLateFee5IsApplied': convert_to_bool(record.get('bLateFee5IsApplied')),
        # Credit Card
        'iCreditCardTypeID': convert_to_int(record.get('iCreditCardTypeID')),
        'sCreditCardNum': record.get('sCreditCardNum'),
        'dCreditCardExpir': convert_to_datetime(record.get('dCreditCardExpir')),
        'sCreditCardHolderName': record.get('sCreditCardHolderName'),
        'sCreditCardCVV2': record.get('sCreditCardCVV2'),
        'sCreditCardStreet': record.get('sCreditCardStreet'),
        'sCreditCardZip': record.get('sCreditCardZip'),
        'iCreditCardAVSResult': convert_to_int(record.get('iCreditCardAVSResult')),
        # ACH
        'sACH_CheckWriterAcctNum': record.get('sACH_CheckWriterAcctNum'),
        'sACH_CheckWriterAcctName': record.get('sACH_CheckWriterAcctName'),
        'sACH_ABA_RoutingNum': record.get('sACH_ABA_RoutingNum'),
        'sACH_RDFI': record.get('sACH_RDFI'),
        'sACH_Check_SavingsCode': record.get('sACH_Check_SavingsCode'),
        # Auto-Billing
        'iAutoBillType': convert_to_int(record.get('iAutoBillType')),
        'iProcessDayOfMonth': convert_to_int(record.get('iProcessDayOfMonth')),
        'bAutoBillChargeFee': convert_to_bool(record.get('bAutoBillChargeFee')),
        'bAutoBillEmailNotify': convert_to_bool(record.get('bAutoBillEmailNotify')),
        'dAutoBillEnabled': convert_to_datetime(record.get('dAutoBillEnabled')),
        # Past Due
        'bDisablePDue': convert_to_bool(record.get('bDisablePDue')),
        'dDisablePDueStrt': convert_to_datetime(record.get('dDisablePDueStrt')),
        'dDisablePDueEnd': convert_to_datetime(record.get('dDisablePDueEnd')),
        # NSF
        'bHadNSF': convert_to_bool(record.get('bHadNSF')),
        'nNSF': convert_to_int(record.get('nNSF')),
        # Invoice
        'bInvoice': convert_to_bool(record.get('bInvoice')),
        'bInvoiceEmail': convert_to_bool(record.get('bInvoiceEmail')),
        'iInvoiceDeliveryType': convert_to_int(record.get('iInvoiceDeliveryType')),
        'iInvoiceDaysBefore': convert_to_int(record.get('iInvoiceDaysBefore')),
        'dInvoiceLast': convert_to_datetime(record.get('dInvoiceLast')),
        'bWaiveInvoiceFee': convert_to_bool(record.get('bWaiveInvoiceFee')),
        # Flags
        'bTaxRent': convert_to_bool(record.get('bTaxRent')),
        'bOverlocked': convert_to_bool(record.get('bOverlocked')),
        'bGateLocked': convert_to_bool(record.get('bGateLocked')),
        'bPermanent': convert_to_bool(record.get('bPermanent')),
        'bExcludeFromRevenueMgmt': convert_to_bool(record.get('bExcludeFromRevenueMgmt')),
        # Security Deposit
        'dcSecDepPaid': convert_to_decimal(record.get('dcSecDepPaid')),
        'dcSecDepBal': convert_to_decimal(record.get('dcSecDepBal')),
        # Balances
        'dcRentBal': convert_to_decimal(record.get('dcRentBal')),
        'dcLateFee1Bal': convert_to_decimal(record.get('dcLateFee1Bal')),
        'dcLateFee2Bal': convert_to_decimal(record.get('dcLateFee2Bal')),
        'dcLateFee3Bal': convert_to_decimal(record.get('dcLateFee3Bal')),
        'dcLateFee4Bal': convert_to_decimal(record.get('dcLateFee4Bal')),
        'dcLateFee5Bal': convert_to_decimal(record.get('dcLateFee5Bal')),
        'dcLateFee1CurrBal': convert_to_decimal(record.get('dcLateFee1CurrBal')),
        'dcLateFee2CurrBal': convert_to_decimal(record.get('dcLateFee2CurrBal')),
        'dcLateFee3CurrBal': convert_to_decimal(record.get('dcLateFee3CurrBal')),
        'dcLateFee4CurrBal': convert_to_decimal(record.get('dcLateFee4CurrBal')),
        'dcLateFee5CurrBal': convert_to_decimal(record.get('dcLateFee5CurrBal')),
        'dcNSFBal': convert_to_decimal(record.get('dcNSFBal')),
        'dcAdminFeeBal': convert_to_decimal(record.get('dcAdminFeeBal')),
        'dcCutLockFeeBal': convert_to_decimal(record.get('dcCutLockFeeBal')),
        'dcAuctionFeeBal': convert_to_decimal(record.get('dcAuctionFeeBal')),
        'dcRecChg1Bal': convert_to_decimal(record.get('dcRecChg1Bal')),
        'dcRecChg2Bal': convert_to_decimal(record.get('dcRecChg2Bal')),
        'dcRecChg3Bal': convert_to_decimal(record.get('dcRecChg3Bal')),
        'dcRecChg4Bal': convert_to_decimal(record.get('dcRecChg4Bal')),
        'dcRecChg5Bal': convert_to_decimal(record.get('dcRecChg5Bal')),
        'dcRecChg6Bal': convert_to_decimal(record.get('dcRecChg6Bal')),
        'dcRecChg7Bal': convert_to_decimal(record.get('dcRecChg7Bal')),
        'dcRecChg8Bal': convert_to_decimal(record.get('dcRecChg8Bal')),
        'dcInsurBal': convert_to_decimal(record.get('dcInsurBal')),
        'dcPOSBal': convert_to_decimal(record.get('dcPOSBal')),
        'dcCreditBal': convert_to_decimal(record.get('dcCreditBal')),
        'dcOtherBal': convert_to_decimal(record.get('dcOtherBal')),
        'dcRefundDue': convert_to_decimal(record.get('dcRefundDue')),
        # Tax Balances
        'dcRentTaxBal': convert_to_decimal(record.get('dcRentTaxBal')),
        'dcLateFeeTaxBal': convert_to_decimal(record.get('dcLateFeeTaxBal')),
        'dcOtherTaxBal': convert_to_decimal(record.get('dcOtherTaxBal')),
        'dcRecChgTaxBal': convert_to_decimal(record.get('dcRecChgTaxBal')),
        'dcInsurTaxBal': convert_to_decimal(record.get('dcInsurTaxBal')),
        'dcPOSTaxBal': convert_to_decimal(record.get('dcPOSTaxBal')),
        # Charge Period Dates
        'dRentLastChgStrt': convert_to_datetime(record.get('dRentLastChgStrt')),
        'dRentLastChgEnd': convert_to_datetime(record.get('dRentLastChgEnd')),
        'dInsurLastChgStrt': convert_to_datetime(record.get('dInsurLastChgStrt')),
        'dInsurLastChgEnd': convert_to_datetime(record.get('dInsurLastChgEnd')),
        'dRecChg1LastChgStrt': convert_to_datetime(record.get('dRecChg1LastChgStrt')),
        'dRecChg1LastChgEnd': convert_to_datetime(record.get('dRecChg1LastChgEnd')),
        'dRecChg2LastChgStrt': convert_to_datetime(record.get('dRecChg2LastChgStrt')),
        'dRecChg2LastChgEnd': convert_to_datetime(record.get('dRecChg2LastChgEnd')),
        'dRecChg3LastChgStrt': convert_to_datetime(record.get('dRecChg3LastChgStrt')),
        'dRecChg3LastChgEnd': convert_to_datetime(record.get('dRecChg3LastChgEnd')),
        'dRecChg4LastChgStrt': convert_to_datetime(record.get('dRecChg4LastChgStrt')),
        'dRecChg4LastChgEnd': convert_to_datetime(record.get('dRecChg4LastChgEnd')),
        'dRecChg5LastChgStrt': convert_to_datetime(record.get('dRecChg5LastChgStrt')),
        'dRecChg5LastChgEnd': convert_to_datetime(record.get('dRecChg5LastChgEnd')),
        'dRecChg6LastChgStrt': convert_to_datetime(record.get('dRecChg6LastChgStrt')),
        'dRecChg6LastChgEnd': convert_to_datetime(record.get('dRecChg6LastChgEnd')),
        'dRecChg7LastChgStrt': convert_to_datetime(record.get('dRecChg7LastChgStrt')),
        'dRecChg7LastChgEnd': convert_to_datetime(record.get('dRecChg7LastChgEnd')),
        'dRecChg8LastChgStrt': convert_to_datetime(record.get('dRecChg8LastChgStrt')),
        'dRecChg8LastChgEnd': convert_to_datetime(record.get('dRecChg8LastChgEnd')),
        # Vehicle
        'sLicPlate': record.get('sLicPlate'),
        'sVehicleDesc': record.get('sVehicleDesc'),
        # Complimentary
        'sReasonComplimentary': record.get('sReasonComplimentary'),
        'sCompanySub': record.get('sCompanySub'),
        # Revenue Management
        'dcTR_RateIncreaseAmt': convert_to_decimal(record.get('dcTR_RateIncreaseAmt')),
        'dTR_LastRateIncreaseNotice': convert_to_datetime(record.get('dTR_LastRateIncreaseNotice')),
        'dTR_NextRateReview': convert_to_datetime(record.get('dTR_NextRateReview')),
        'iTR_RateIncreasePendingStatus': convert_to_int(record.get('iTR_RateIncreasePendingStatus')),
        'iRemoveDiscPlanOnSchedRateChange': convert_to_int(record.get('iRemoveDiscPlanOnSchedRateChange')),
        # Auction
        'iAuctionStatus': convert_to_int(record.get('iAuctionStatus')),
        'dAuctionDate': convert_to_datetime(record.get('dAuctionDate')),
        # Timestamps
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'dDeleted': convert_to_datetime(record.get('dDeleted')),
        'dArchived': convert_to_datetime(record.get('dArchived')),
        # Tracking
        'extract_date': extract_date,
        'data_source': DATA_SOURCE_LOCAL_SQL,
    }


def transform_charge_from_local_sql(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform ALL charge fields from local SQL."""
    return {
        'ChargeID': convert_to_int(record.get('ChargeID')),
        'SiteID': convert_to_int(record.get('SiteID')),
        'dcPmtAmt': Decimal('0'),  # Not in local SQL, default to 0
        'ChargeDescID': convert_to_int(record.get('ChargeDescID')),
        'LedgerID': convert_to_int(record.get('LedgerID')),
        'InsurLedgerID': convert_to_int(record.get('InsurLedgerID')),
        'FiscalID': convert_to_int(record.get('FiscalID')),
        'ConcessionID': convert_to_int(record.get('ConcessionID')),
        'EmployeeID': convert_to_int(record.get('EmployeeID')),
        'ACHID': convert_to_int(record.get('ACHID')),
        'Disc_MemoID': convert_to_int(record.get('Disc_MemoID')),
        'ReceiptID_NSF': convert_to_int(record.get('ReceiptID_NSF')),
        'QTChargeID': convert_to_int(record.get('QTChargeID')),
        # Amounts
        'dcAmt': convert_to_decimal(record.get('dcAmt')),
        'dcTax1': convert_to_decimal(record.get('dcTax1')),
        'dcTax2': convert_to_decimal(record.get('dcTax2')),
        'dcQty': convert_to_decimal(record.get('dcQty')),
        'dcStdPrice': convert_to_decimal(record.get('dcStdPrice')),
        'dcPrice': convert_to_decimal(record.get('dcPrice')),
        'dcCost': convert_to_decimal(record.get('dcCost')),
        'dcPriceTax1': convert_to_decimal(record.get('dcPriceTax1')),
        'dcPriceTax2': convert_to_decimal(record.get('dcPriceTax2')),
        # Dates
        'dChgStrt': convert_to_datetime(record.get('dChgStrt')),
        'dChgEnd': convert_to_datetime(record.get('dChgEnd')),
        'dCreated': convert_to_datetime(record.get('dCreated')),
        # Flags
        'bMoveIn': convert_to_bool(record.get('bMoveIn')),
        'bMoveOut': convert_to_bool(record.get('bMoveOut')),
        'bNSF': convert_to_bool(record.get('bNSF')),
        'iNSFFlag': convert_to_int(record.get('iNSFFlag')),
        # Promotional
        'iPromoGlobalNum': convert_to_int(record.get('iPromoGlobalNum')),
        # Timestamps
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'dArchived': convert_to_datetime(record.get('dArchived')),
        'dDeleted': convert_to_datetime(record.get('dDeleted')),
        # Charge Description (from join)
        'sChgCategory': record.get('sChgCategory'),
        'sChgDesc': record.get('sChgDesc'),
        'sDefChgDesc': record.get('sDefChgDesc'),
        # Tracking
        'extract_date': extract_date,
        'data_source': DATA_SOURCE_LOCAL_SQL,
    }


# =============================================================================
# API Transform Functions (subset of fields)
# =============================================================================

def transform_tenant_from_api(record: Dict[str, Any], location_code: str) -> Dict[str, Any]:
    """Transform TenantList API record (subset of fields)."""
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
        'data_source': DATA_SOURCE_API,
    }


def transform_ledger_from_api(record: Dict[str, Any], tenant_id: int, extract_date: date) -> Dict[str, Any]:
    """Transform LedgersByTenantID_v3 API record (subset of fields)."""
    return {
        'SiteID': convert_to_int(record.get('SiteID')),
        'LedgerID': convert_to_int(record.get('LedgerID')),
        'TenantID': tenant_id,
        'unitID': convert_to_int(record.get('UnitID')),
        'EmployeeID': convert_to_int(record.get('EmployeeID')),
        'sUnitName': record.get('sUnitName'),
        'TenantName': record.get('TenantName'),
        'dcRent': convert_to_decimal(record.get('dcRent')),
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),
        'dMovedIn': convert_to_datetime(record.get('dMovedIn')),
        'dPaidThru': convert_to_datetime(record.get('dPaidThru')),
        'dAnniv': convert_to_datetime(record.get('dAnniv')),
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'iLeaseNum': convert_to_int(record.get('iLeaseNum')),
        'bInvoice': convert_to_bool(record.get('bInvoice')),
        'iAutoBillType': convert_to_int(record.get('iAutoBillType')),
        'iInvoiceDeliveryType': convert_to_int(record.get('iInvoiceDeliveryType')),
        'bOverlocked': convert_to_bool(record.get('bOverlocked')),
        'bPermanent': convert_to_bool(record.get('bPermanent')),
        'dcChargeBalance': convert_to_decimal(record.get('dcChargeBalance')),
        'dcTotalDue': convert_to_decimal(record.get('dcTotalDue')),
        'extract_date': extract_date,
        'data_source': DATA_SOURCE_API,
    }


def transform_charge_from_api(record: Dict[str, Any], ledger_id: int, site_id: int, extract_date: date) -> Dict[str, Any]:
    """Transform ChargesAllByLedgerID API record."""
    pmt_amt = convert_to_decimal(record.get('dcPmtAmt'))
    if pmt_amt is None:
        pmt_amt = Decimal('0')

    return {
        'SiteID': site_id,
        'ChargeID': convert_to_int(record.get('ChargeID')),
        'dcPmtAmt': pmt_amt,
        'LedgerID': ledger_id,
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
        'data_source': DATA_SOURCE_API,
    }


# =============================================================================
# Backfill Mode (Local SQL Server)
# =============================================================================

def run_backfill_mode(config: DataLayerConfig, site_ids: Optional[List[int]] = None,
                      db_batch_size: int = 100, use_ssh: bool = False) -> Tuple[int, int, int]:
    """Run backfill mode: fetch ALL data from local SQL Server.

    Args:
        config: DataLayerConfig instance
        site_ids: Optional list of site IDs to filter
        db_batch_size: Batch size for database upserts
        use_ssh: If True, connect via SSH tunnel to VM's SQL Server
    """
    extract_date = date.today()

    mode_label = "SSH Tunnel to VM" if use_ssh else "Local SQL Server"
    print("\n" + "=" * 70)
    print(f"BACKFILL MODE - {mode_label} (Full Schema)")
    print("=" * 70)

    print(f"\nConnecting to SQL Server ({mode_label})...")
    try:
        local_engine = create_local_sql_engine(use_ssh=use_ssh)
        print("  Connected successfully")
    except Exception as e:
        print(f"  ERROR: Failed to connect: {e}")
        if use_ssh:
            close_ssh_tunnel()
        return 0, 0, 0

    # Fetch data
    print("\n[1/3] Fetching tenants...")
    sql_tenants = fetch_tenants_from_local_sql(local_engine, site_ids)
    print(f"  Found {len(sql_tenants)} tenants")

    print("\n[2/3] Fetching ledgers...")
    sql_ledgers = fetch_ledgers_from_local_sql(local_engine, site_ids)
    print(f"  Found {len(sql_ledgers)} ledgers")

    print("\n[3/3] Fetching charges...")
    sql_charges = fetch_charges_from_local_sql(local_engine, site_ids)
    print(f"  Found {len(sql_charges)} charges")

    local_engine.dispose()
    if use_ssh:
        close_ssh_tunnel()

    if not sql_tenants and not sql_ledgers and not sql_charges:
        print("No data found.")
        return 0, 0, 0

    # Transform
    print("\nTransforming records...")
    all_tenants = [transform_tenant_from_local_sql(t, extract_date) for t in tqdm(sql_tenants, desc="  Tenants")]
    all_ledgers = [transform_ledger_from_local_sql(l, extract_date) for l in tqdm(sql_ledgers, desc="  Ledgers")
                   if convert_to_int(l.get('LedgerID'))]
    all_charges = [transform_charge_from_local_sql(c, extract_date) for c in tqdm(sql_charges, desc="  Charges")
                   if convert_to_int(c.get('ChargeID'))]

    # Push to PostgreSQL
    print("\n" + "-" * 70)
    print("Pushing to PostgreSQL...")
    push_to_database(all_tenants, all_ledgers, all_charges, config, db_batch_size)

    return len(all_tenants), len(all_ledgers), len(all_charges)


# =============================================================================
# Daily Mode (API) - Simplified version
# =============================================================================

def get_callcenter_url(reporting_url: str) -> str:
    return reporting_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')


def call_soap_endpoint(soap_client: SOAPClient, report_name: str, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
    config = REPORT_REGISTRY[report_name]
    return soap_client.call(operation=config.operation, parameters=parameters,
                            soap_action=config.soap_action, namespace=config.namespace, result_tag=config.result_tag)


def run_daily_mode(config: DataLayerConfig, location_codes: List[str],
                   incremental_since: Optional[datetime] = None,
                   db_batch_size: int = 100) -> Tuple[int, int, int]:
    """Run daily mode: fetch data from SOAP API."""
    if not config.soap:
        raise ValueError("SOAP configuration not found")

    extract_date = date.today()
    cc_url = get_callcenter_url(config.soap.base_url)

    print("\n" + "=" * 70)
    print("DAILY MODE - SOAP API")
    print("=" * 70)
    print(f"Service URL: {cc_url}")

    soap_client = SOAPClient(base_url=cc_url, corp_code=config.soap.corp_code,
                             corp_user=config.soap.corp_user, api_key=config.soap.api_key,
                             corp_password=config.soap.corp_password,
                             timeout=config.soap.timeout, retries=config.soap.retries)

    all_tenants, all_ledgers, all_charges = [], [], []

    for location_code in location_codes:
        print(f"\n[{location_code}] Processing...")

        # Fetch tenants
        try:
            raw_tenants = call_soap_endpoint(soap_client, 'tenant_list',
                                             {'sLocationCode': location_code, 'sTenantFirstName': '', 'sTenantLastName': ''})
            tenant_ids = []
            for t in raw_tenants:
                transformed = transform_tenant_from_api(t, location_code)
                if transformed['TenantID']:
                    all_tenants.append(transformed)
                    tenant_ids.append(transformed['TenantID'])
            print(f"  Tenants: {len(tenant_ids)}")
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        # Fetch ledgers per tenant
        ledger_to_site = {}
        for tid in tqdm(tenant_ids, desc="  Ledgers"):
            try:
                raw_ledgers = call_soap_endpoint(soap_client, 'ledgers_by_tenant_id_v3',
                                                 {'sLocationCode': location_code, 'sTenantID': str(tid)})
                for l in raw_ledgers:
                    transformed = transform_ledger_from_api(l, tid, extract_date)
                    if transformed['LedgerID']:
                        all_ledgers.append(transformed)
                        ledger_to_site[transformed['LedgerID']] = transformed['SiteID']
            except Exception:
                pass

        # Fetch charges per ledger
        for lid in tqdm(list(ledger_to_site.keys()), desc="  Charges"):
            try:
                raw_charges = call_soap_endpoint(soap_client, 'charges_all_by_ledger_id',
                                                 {'sLocationCode': location_code, 'ledgerId': lid})
                site_id = ledger_to_site[lid]
                for c in raw_charges:
                    transformed = transform_charge_from_api(c, lid, site_id, extract_date)
                    if transformed['ChargeID']:
                        all_charges.append(transformed)
            except Exception:
                pass

    soap_client.close()

    print("\n" + "-" * 70)
    print("Pushing to PostgreSQL...")
    push_to_database(all_tenants, all_ledgers, all_charges, config, db_batch_size)

    return len(all_tenants), len(all_ledgers), len(all_charges)


# =============================================================================
# Database Operations with Real-time Error Logging
# =============================================================================

import traceback

# Global error log path
ERROR_LOG_PATH = Path(__file__).parent / 'backfill_errors.log'


def log_error_immediately(entity_type: str, batch_num: int, error: Exception,
                          sample_record: Dict = None) -> None:
    """Log error immediately to file as it occurs."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create/append to log file
    with open(ERROR_LOG_PATH, 'a') as f:
        f.write(f"\n{'=' * 70}\n")
        f.write(f"[{timestamp}] {entity_type} ERROR - Batch {batch_num}\n")
        f.write(f"{'=' * 70}\n")
        f.write(f"Error Type: {type(error).__name__}\n")
        f.write(f"Error Message: {str(error)}\n")
        f.write(f"\nFull Traceback:\n{traceback.format_exc()}\n")

        if sample_record:
            f.write(f"\nSample Record Keys: {list(sample_record.keys())}\n")
            # Log first few values for debugging
            f.write(f"Sample Record Values (first 10):\n")
            for i, (k, v) in enumerate(sample_record.items()):
                if i >= 10:
                    f.write(f"  ... and {len(sample_record) - 10} more fields\n")
                    break
                f.write(f"  {k}: {repr(v)[:100]}\n")

        f.write(f"\n")
        f.flush()  # Force write to disk immediately


def init_error_log() -> None:
    """Initialize error log file with header."""
    with open(ERROR_LOG_PATH, 'w') as f:
        f.write(f"Backfill Error Log - Started {datetime.now()}\n")
        f.write(f"{'=' * 70}\n")
        f.write(f"Errors will be logged here as they occur.\n")
        f.flush()


def push_to_database(tenants: List[Dict], ledgers: List[Dict], charges: List[Dict],
                     config: DataLayerConfig, chunk_size: int = 100) -> None:
    """Push all data to PostgreSQL with real-time error logging.

    Uses individual transactions per batch to prevent one error from
    cascading and failing all subsequent batches.
    """
    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    engine = create_engine_from_config(db_config)

    # Initialize error log at start of push
    init_error_log()
    print(f"  Error log: {ERROR_LOG_PATH}")

    error_count = 0
    success_count = {'tenants': 0, 'ledgers': 0, 'charges': 0}

    # Process tenants
    if tenants:
        print(f"  Upserting {len(tenants)} tenants...")
        tenants = deduplicate_records(tenants, ['SiteID', 'TenantID'])
        total_batches = (len(tenants) + chunk_size - 1) // chunk_size
        for i in tqdm(range(0, len(tenants), chunk_size), desc="    Tenants"):
            batch_num = i // chunk_size + 1
            batch = tenants[i:i+chunk_size]
            # Use a fresh session for each batch to isolate transactions
            session_manager = SessionManager(engine)
            try:
                with session_manager.session_scope() as session:
                    upsert_ops = UpsertOperations(session, db_config.db_type)
                    upsert_ops.upsert_batch(Tenant, batch, ['SiteID', 'TenantID'], chunk_size)
                    success_count['tenants'] += len(batch)
            except Exception as e:
                error_count += 1
                sample = batch[0] if batch else None
                log_error_immediately("TENANT", batch_num, e, sample)
                print(f"\n    ERROR logged (batch {batch_num}/{total_batches}): {str(e)[:100]}")

    # Process ledgers
    if ledgers:
        print(f"  Upserting {len(ledgers)} ledgers...")
        ledgers = deduplicate_records(ledgers, ['SiteID', 'LedgerID'])
        total_batches = (len(ledgers) + chunk_size - 1) // chunk_size
        for i in tqdm(range(0, len(ledgers), chunk_size), desc="    Ledgers"):
            batch_num = i // chunk_size + 1
            batch = ledgers[i:i+chunk_size]
            session_manager = SessionManager(engine)
            try:
                with session_manager.session_scope() as session:
                    upsert_ops = UpsertOperations(session, db_config.db_type)
                    upsert_ops.upsert_batch(Ledger, batch, ['SiteID', 'LedgerID'], chunk_size)
                    success_count['ledgers'] += len(batch)
            except Exception as e:
                error_count += 1
                sample = batch[0] if batch else None
                log_error_immediately("LEDGER", batch_num, e, sample)
                print(f"\n    ERROR logged (batch {batch_num}/{total_batches}): {str(e)[:100]}")

    # Process charges
    if charges:
        print(f"  Upserting {len(charges)} charges...")
        charges = deduplicate_records(charges, ['SiteID', 'ChargeID', 'dcPmtAmt'])
        total_batches = (len(charges) + chunk_size - 1) // chunk_size
        for i in tqdm(range(0, len(charges), chunk_size), desc="    Charges"):
            batch_num = i // chunk_size + 1
            batch = charges[i:i+chunk_size]
            session_manager = SessionManager(engine)
            try:
                with session_manager.session_scope() as session:
                    upsert_ops = UpsertOperations(session, db_config.db_type)
                    upsert_ops.upsert_batch(Charge, batch, ['SiteID', 'ChargeID', 'dcPmtAmt'], chunk_size)
                    success_count['charges'] += len(batch)
            except Exception as e:
                error_count += 1
                sample = batch[0] if batch else None
                log_error_immediately("CHARGE", batch_num, e, sample)
                print(f"\n    ERROR logged (batch {batch_num}/{total_batches}): {str(e)[:100]}")

    engine.dispose()

    # Final summary
    print(f"\n  Results:")
    print(f"    Tenants: {success_count['tenants']} succeeded")
    print(f"    Ledgers: {success_count['ledgers']} succeeded")
    print(f"    Charges: {success_count['charges']} succeeded")
    if error_count > 0:
        print(f"\n  Total errors: {error_count} (see {ERROR_LOG_PATH})")
    else:
        print(f"\n  All records pushed successfully (no errors)")


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description='Tenant-Ledger-Charges Unified Sync (Expanded Schema)')
    parser.add_argument('--mode', choices=['backfill', 'daily'], required=True)
    parser.add_argument('--location', type=str, default=None)
    parser.add_argument('--site-id', type=int, default=None)
    parser.add_argument('--days-back', type=int, default=7)
    parser.add_argument('--db-batch-size', type=int, default=100)
    parser.add_argument('--ssh', action='store_true',
                        help='Use SSH tunnel to connect to VM SQL Server (for backfill mode)')
    parser.add_argument('--no-ssh', action='store_true',
                        help='Force direct connection (no SSH tunnel)')
    return parser.parse_args()


def main():
    args = parse_args()
    config = DataLayerConfig.from_env()

    print("=" * 70)
    print("Tenant-Ledger-Charges Unified Sync (Expanded Schema)")
    print("=" * 70)
    print(f"Mode: {args.mode.upper()}")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")

    if args.mode == 'backfill':
        site_ids = None
        if args.location:
            site_ids = [LOCATION_TO_SITE_ID.get(args.location)]
        elif args.site_id:
            site_ids = [args.site_id]

        # Determine connection mode
        if args.no_ssh:
            use_ssh = False
        elif args.ssh:
            use_ssh = True
        else:
            use_ssh = False  # Default to direct connection

        if use_ssh:
            print(f"Connection: SSH Tunnel to VM")
        else:
            print(f"Connection: Direct (Local SQL Server)")

        t, l, c = run_backfill_mode(config, site_ids, args.db_batch_size, use_ssh=use_ssh)
    else:
        location_codes = [args.location] if args.location else env_config('RENTROLL_LOCATION_CODES', cast=Csv())
        incremental_since = datetime.now() - timedelta(days=args.days_back)
        t, l, c = run_daily_mode(config, location_codes, incremental_since, args.db_batch_size)

    print("\n" + "=" * 70)
    print(f"Completed! Tenants: {t}, Ledgers: {l}, Charges: {c}")
    print("=" * 70)


if __name__ == "__main__":
    main()
