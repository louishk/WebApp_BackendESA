"""
Tenant-Ledger-Charges Historical Load from Local SQL Server

Fetches ALL tenant IDs from local SQL Server database (bypassing 50-record API limit),
then retrieves ledger and charge data via SOAP API.

Workflow:
1. Connect to local SQL Server (sldbclnt database)
2. Fetch all TenantID + SiteID from Tenants table
3. Map SiteID -> LocationCode
4. For each tenant: fetch ledgers via API, then charges per ledger
5. Upsert to PostgreSQL

Usage:
    # Load all tenants from all sites
    python tenant_ledger_charges_historical.py

    # Load specific location only
    python tenant_ledger_charges_historical.py --location L001

    # Load specific SiteID only
    python tenant_ledger_charges_historical.py --site-id 48
"""

import argparse
import urllib.parse
from datetime import date
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

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
# SiteID to LocationCode Mapping
# =============================================================================

SITE_ID_TO_LOCATION = {
    48: "L001",      # IMM, Singapore
    49: "L002",      # BKR, Singapore
    63: "L003",      # ELK, Singapore
    26486: "L004",   # WCT, Singapore
    1910: "L005",    # MMR, Singapore
    2276: "L006",    # ESKY, South Korea
    4183: "L007",    # CSL, Malaysia
    9415: "L008",    # KWY, Singapore
    10419: "L009",   # SEG, Malaysia
    10777: "L010",   # S51A, Malaysia
    24411: "L011",   # ESKB, South Korea
    25675: "L013",   # ESKG, South Korea
    26710: "L015",   # SW, Hong Kong
    27903: "L017",   # WDL, Singapore
    29197: "L018",   # AMK, Singapore
    29064: "L019",   # ESKA, South Korea
    32663: "L020",   # HH, Hong Kong
    33881: "L021",   # ESKYP, South Korea
    38782: "L022",   # TPY, Singapore
    39284: "L023",   # ESKYS, South Korea
    40100: "L024",   # ESKBP, South Korea
    43344: "L025",   # HV, Singapore
    44449: "L026",   # KD, Malaysia
    52421: "L028",   # CW, Singapore
    54219: "L029",   # CW, Singapore
    57451: "L030",   # TS, Singapore
    # EXCLUDED: 27525 = LSETUP (test/setup site)
}

LOCATION_TO_SITE_ID = {v: k for k, v in SITE_ID_TO_LOCATION.items()}


# =============================================================================
# Local SQL Server Connection
# =============================================================================

def create_local_sql_engine(
    server: str = r"LOUISVER-T14\VSDOTNET",
    database: str = "sldbclnt",
    driver: str = "SQL Server"
) -> Engine:
    """
    Create SQLAlchemy engine for local SQL Server with Windows Authentication.

    Args:
        server: SQL Server instance name
        database: Database name
        driver: ODBC driver name

    Returns:
        SQLAlchemy Engine
    """
    driver_encoded = urllib.parse.quote_plus(driver)
    connection_url = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver={driver_encoded}"
        f"&Trusted_Connection=yes"
        f"&TrustServerCertificate=yes"
        f"&Connection Timeout=60"
    )

    engine = create_engine(
        connection_url,
        pool_size=5,
        max_overflow=10,
        pool_timeout=60,
        pool_recycle=1800,
        pool_pre_ping=True
    )

    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    return engine


def fetch_tenants_from_sql(
    engine: Engine,
    site_ids: Optional[List[int]] = None
) -> List[Dict[str, Any]]:
    """
    Fetch TenantID and SiteID from local SQL Server Tenants table.

    Args:
        engine: SQLAlchemy engine for local SQL Server
        site_ids: Optional list of SiteIDs to filter (None = all sites)

    Returns:
        List of dicts with TenantID and SiteID
    """
    if site_ids:
        site_id_list = ','.join(str(s) for s in site_ids)
        query = text(f"""
            SELECT DISTINCT TenantID, SiteID
            FROM Tenants
            WHERE SiteID IN ({site_id_list})
            AND TenantID IS NOT NULL
            ORDER BY SiteID, TenantID
        """)
    else:
        query = text("""
            SELECT DISTINCT TenantID, SiteID
            FROM Tenants
            WHERE TenantID IS NOT NULL
            ORDER BY SiteID, TenantID
        """)

    tenants = []
    with engine.connect() as conn:
        result = conn.execute(query)
        for row in result:
            tenants.append({
                'TenantID': row[0],
                'SiteID': row[1],
            })

    return tenants


def fetch_ledgers_from_sql(
    engine: Engine,
    site_ids: Optional[List[int]] = None
) -> List[Dict[str, Any]]:
    """
    Fetch all ledgers from local SQL Server Ledgers table.

    Args:
        engine: SQLAlchemy engine for local SQL Server
        site_ids: Optional list of SiteIDs to filter (None = all sites)

    Returns:
        List of dicts with ledger data
    """
    if site_ids:
        site_id_list = ','.join(str(s) for s in site_ids)
        where_clause = f"WHERE SiteID IN ({site_id_list})"
    else:
        where_clause = ""

    # First try with TenantID, fall back without if column doesn't exist
    query_with_tenant = text(f"""
        SELECT LedgerID, SiteID, unitID as UnitID, EmployeeID,
               dcRent, dcInsurPremium, dMovedIn, dPaidThru, dAnniv,
               dCreated, dUpdated, iLeaseNum, bInvoice, iAutoBillType,
               iInvoiceDeliveryType, bOverlocked, bPermanent,
               ISNULL(TenantID, 0) as TenantID
        FROM Ledgers
        {where_clause}
        ORDER BY SiteID, LedgerID
    """)

    query_without_tenant = text(f"""
        SELECT LedgerID, SiteID, unitID as UnitID, EmployeeID,
               dcRent, dcInsurPremium, dMovedIn, dPaidThru, dAnniv,
               dCreated, dUpdated, iLeaseNum, bInvoice, iAutoBillType,
               iInvoiceDeliveryType, bOverlocked, bPermanent,
               0 as TenantID
        FROM Ledgers
        {where_clause}
        ORDER BY SiteID, LedgerID
    """)

    ledgers = []
    with engine.connect() as conn:
        # Try with TenantID first, fall back if column doesn't exist
        try:
            result = conn.execute(query_with_tenant)
        except Exception:
            result = conn.execute(query_without_tenant)

        columns = result.keys()
        for row in result:
            ledgers.append(dict(zip(columns, row)))

    return ledgers


def fetch_charges_from_sql(
    engine: Engine,
    site_ids: Optional[List[int]] = None
) -> List[Dict[str, Any]]:
    """
    Fetch all charges from local SQL Server with ChargeDesc join.

    Args:
        engine: SQLAlchemy engine for local SQL Server
        site_ids: Optional list of SiteIDs to filter (None = all sites)

    Returns:
        List of dicts with charge data including descriptions
    """
    if site_ids:
        site_id_list = ','.join(str(s) for s in site_ids)
        where_clause = f"WHERE c.SiteID IN ({site_id_list})"
    else:
        where_clause = ""

    query = text(f"""
        SELECT c.ChargeID, c.SiteID, c.LedgerID, c.ChargeDescID,
               c.dcAmt, c.dcTax1, c.dcTax2, c.dcQty, c.dcPrice,
               c.dChgStrt, c.dChgEnd, c.bMoveIn, c.bMoveOut,
               cd.sChgCategory, cd.sChgDesc, cd.sDefChgDesc
        FROM Charges c
        LEFT JOIN ChargeDesc cd ON c.ChargeDescID = cd.ChargeDescID
                                AND c.SiteID = cd.SiteID
        {where_clause}
        ORDER BY c.SiteID, c.LedgerID, c.ChargeID
    """)

    charges = []
    with engine.connect() as conn:
        result = conn.execute(query)
        columns = result.keys()
        for row in result:
            charges.append(dict(zip(columns, row)))

    return charges


# =============================================================================
# Configuration
# =============================================================================

def get_callcenter_url(reporting_url: str) -> str:
    """Convert ReportingWs URL to CallCenterWs URL."""
    return reporting_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')


# =============================================================================
# Transformation Functions (from tenant_ledger_charges_to_sql.py)
# =============================================================================

def transform_tenant(site_id: int, tenant_id: int, location_code: str) -> Dict[str, Any]:
    """Create minimal tenant record from SQL source."""
    return {
        'SiteID': site_id,
        'TenantID': tenant_id,
        'sLocationCode': location_code,
        'sFName': None,
        'sMI': None,
        'sLName': None,
        'sCompany': None,
        'sAddr1': None,
        'sAddr2': None,
        'sCity': None,
        'sRegion': None,
        'sPostalCode': None,
        'sPhone': None,
        'sEmail': None,
        'sMobile': None,
        'sLicense': None,
        'sAccessCode': None,
    }


def transform_ledger(record: Dict[str, Any], tenant_id: int, extract_date: date) -> Dict[str, Any]:
    """Transform LedgersByTenantID_v3 API record to database format."""
    return {
        'SiteID': convert_to_int(record.get('SiteID')),
        'LedgerID': convert_to_int(record.get('LedgerID')),
        'TenantID': tenant_id,
        'UnitID': convert_to_int(record.get('UnitID')),
        'EmployeeID': convert_to_int(record.get('EmployeeID')),
        'sUnitName': record.get('sUnitName'),
        'TenantName': record.get('TenantName'),
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
        'sMobile': record.get('sMobile'),
        'sEmail': record.get('sEmail'),
        'sFax': record.get('sFax'),
        'sAccessCode': record.get('sAccessCode'),
        'sAccessCode2': record.get('sAccessCode2'),
        'dcRent': convert_to_decimal(record.get('dcRent')),
        'dcChargeBalance': convert_to_decimal(record.get('dcChargeBalance')),
        'dcTotalDue': convert_to_decimal(record.get('dcTotalDue')),
        'dcTaxRateRent': convert_to_decimal(record.get('dcTaxRateRent')),
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),
        'dcTaxRateInsurance': convert_to_decimal(record.get('dcTaxRateInsurance')),
        'dMovedIn': convert_to_datetime(record.get('dMovedIn')),
        'dPaidThru': convert_to_datetime(record.get('dPaidThru')),
        'dAnniv': convert_to_datetime(record.get('dAnniv')),
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'sBillingFrequency': record.get('sBillingFrequency'),
        'iLeaseNum': convert_to_int(record.get('iLeaseNum')),
        'iDefLeaseNum': convert_to_int(record.get('iDefLeaseNum')),
        'bInvoice': convert_to_bool(record.get('bInvoice')),
        'iAutoBillType': convert_to_int(record.get('iAutoBillType')),
        'iInvoiceDeliveryType': convert_to_int(record.get('iInvoiceDeliveryType')),
        'bOverlocked': convert_to_bool(record.get('bOverlocked')),
        'bCommercial': convert_to_bool(record.get('bCommercial')),
        'bTaxExempt': convert_to_bool(record.get('bTaxExempt')),
        'bSpecial': convert_to_bool(record.get('bSpecial')),
        'bNeverLockOut': convert_to_bool(record.get('bNeverLockOut')),
        'bCompanyIsTenant': convert_to_bool(record.get('bCompanyIsTenant')),
        'bPermanent': convert_to_bool(record.get('bPermanent')),
        'bExcludeFromInsurance': convert_to_bool(record.get('bExcludeFromInsurance')),
        'bSMSOptIn': convert_to_bool(record.get('bSMSOptIn')),
        'MarketingID': convert_to_int(record.get('MarketingID')),
        'MktgDistanceID': convert_to_int(record.get('MktgDistanceID')),
        'MktgReasonID': convert_to_int(record.get('MktgReasonID')),
        'MktgTypeID': convert_to_int(record.get('MktgTypeID')),
        'sLicense': record.get('sLicense'),
        'sTaxID': record.get('sTaxID'),
        'sTaxExemptCode': record.get('sTaxExemptCode'),
        'sTenNote': record.get('sTenNote'),
        'dcLongitude': convert_to_decimal(record.get('dcLongitude')),
        'dcLatitude': convert_to_decimal(record.get('dcLatitude')),
        'extract_date': extract_date,
    }


def transform_charge(record: Dict[str, Any], ledger_id: int, site_id: int, extract_date: date) -> Dict[str, Any]:
    """Transform ChargesAllByLedgerID API record to database format."""
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
    }


# =============================================================================
# SQL Transform Functions (for local SQL Server data)
# =============================================================================

def transform_ledger_from_sql(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform local SQL Server ledger record to database format.

    Maps available fields from local SQL, sets unavailable fields to NULL.
    """
    return {
        # Primary keys
        'SiteID': convert_to_int(record.get('SiteID')),
        'LedgerID': convert_to_int(record.get('LedgerID')),
        # Foreign keys (TenantID and UnitID are NOT NULL in schema)
        'TenantID': convert_to_int(record.get('TenantID')) or 0,  # Default 0 if not available
        'UnitID': convert_to_int(record.get('UnitID')) or 0,  # Default 0 if NULL
        'EmployeeID': convert_to_int(record.get('EmployeeID')),
        # Unit info - not in local SQL Ledgers
        'sUnitName': None,
        # Tenant info - not in local SQL Ledgers
        'TenantName': None,
        'sMrMrs': None,
        'sFName': None,
        'sMI': None,
        'sLName': None,
        'sCompany': None,
        # Address - not in local SQL Ledgers
        'sAddr1': None,
        'sAddr2': None,
        'sCity': None,
        'sRegion': None,
        'sPostalCode': None,
        'sCountry': None,
        # Contact - not in local SQL Ledgers
        'sPhone': None,
        'sMobile': None,
        'sEmail': None,
        'sFax': None,
        # Access - not in local SQL Ledgers
        'sAccessCode': None,
        'sAccessCode2': None,
        # Financial - available
        'dcRent': convert_to_decimal(record.get('dcRent')),
        'dcChargeBalance': None,  # Not in local SQL
        'dcTotalDue': None,  # Not in local SQL
        'dcTaxRateRent': None,  # Not in local SQL
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),
        'dcTaxRateInsurance': None,  # Not in local SQL
        # Dates - available (with explicit conversion)
        'dMovedIn': convert_to_datetime(record.get('dMovedIn')),
        'dPaidThru': convert_to_datetime(record.get('dPaidThru')),
        'dAnniv': convert_to_datetime(record.get('dAnniv')),
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        # Billing info
        'sBillingFrequency': None,  # Not in local SQL
        'iLeaseNum': convert_to_int(record.get('iLeaseNum')),
        'iDefLeaseNum': None,  # Not in local SQL
        'bInvoice': convert_to_bool(record.get('bInvoice')),
        'iAutoBillType': convert_to_int(record.get('iAutoBillType')),
        'iInvoiceDeliveryType': convert_to_int(record.get('iInvoiceDeliveryType')),
        # Status flags
        'bOverlocked': convert_to_bool(record.get('bOverlocked')),
        'bCommercial': None,  # Not in local SQL Ledgers
        'bTaxExempt': None,  # Not in local SQL Ledgers
        'bSpecial': None,  # Not in local SQL Ledgers
        'bNeverLockOut': None,  # Not in local SQL Ledgers
        'bCompanyIsTenant': None,  # Not in local SQL Ledgers
        'bPermanent': convert_to_bool(record.get('bPermanent')),
        'bExcludeFromInsurance': None,  # Not in local SQL Ledgers
        'bSMSOptIn': None,  # Not in local SQL Ledgers
        # Marketing - not in local SQL Ledgers
        'MarketingID': None,
        'MktgDistanceID': None,
        'MktgReasonID': None,
        'MktgTypeID': None,
        # Other - not in local SQL Ledgers
        'sLicense': None,
        'sTaxID': None,
        'sTaxExemptCode': None,
        'sTenNote': None,
        'dcLongitude': None,
        'dcLatitude': None,
        # Tracking
        'extract_date': extract_date,
    }


def transform_charge_from_sql(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform local SQL Server charge record to database format.

    Includes fields from ChargeDesc join.
    """
    return {
        'SiteID': convert_to_int(record.get('SiteID')),
        'ChargeID': convert_to_int(record.get('ChargeID')),
        'dcPmtAmt': Decimal('0'),  # Not in Charges table, default to 0
        'LedgerID': convert_to_int(record.get('LedgerID')),
        'sChgCategory': record.get('sChgCategory'),  # From ChargeDesc join
        'sChgDesc': record.get('sChgDesc'),  # From ChargeDesc join
        'sDefChgDesc': record.get('sDefChgDesc'),  # From ChargeDesc join
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


# =============================================================================
# Processing Functions
# =============================================================================

def process_tenant_chunk(
    soap_client: SOAPClient,
    site_id: int,
    tenant_ids: List[int],
    location_code: str,
    extract_date: date,
    max_workers: int = 20
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Process a chunk of tenants using parallel API calls.

    Args:
        soap_client: SOAP client instance
        site_id: SiteID
        tenant_ids: List of TenantIDs to process
        location_code: Location code (e.g., L001)
        extract_date: Date for extract_date field
        max_workers: Number of parallel threads (default: 20)

    Returns:
        Tuple of (tenant_records, ledger_records, charge_records)
    """
    all_tenants = []
    all_ledgers = []
    all_charges = []

    # Create tenant records (minimal - API doesn't return full tenant data)
    for tid in tenant_ids:
        all_tenants.append(transform_tenant(site_id, tid, location_code))

    # Step 2: Fetch ledgers for all tenants (PARALLEL)
    ledger_to_site = {}

    def fetch_tenant_ledgers(tenant_id: int) -> List[Dict]:
        """Fetch ledgers for a single tenant."""
        try:
            raw_ledgers = fetch_ledgers_for_tenant(soap_client, location_code, tenant_id)
            ledgers = []
            for l in raw_ledgers:
                transformed = transform_ledger(l, tenant_id, extract_date)
                if transformed['LedgerID']:
                    ledgers.append(transformed)
            return ledgers
        except Exception:
            return []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_tenant_ledgers, tid): tid for tid in tenant_ids}
        with tqdm(total=len(tenant_ids), desc="        Tenants", unit="t", leave=False) as pbar:
            for future in as_completed(futures):
                ledgers = future.result()
                for l in ledgers:
                    all_ledgers.append(l)
                    ledger_to_site[l['LedgerID']] = l['SiteID']
                pbar.update(1)

    if not all_ledgers:
        return all_tenants, all_ledgers, all_charges

    # Step 3: Fetch charges for all ledgers (PARALLEL)
    ledger_ids = [l['LedgerID'] for l in all_ledgers]

    def fetch_ledger_charges(ledger_id: int) -> List[Dict]:
        """Fetch charges for a single ledger."""
        try:
            site_id_for_charge = ledger_to_site.get(ledger_id, site_id)
            raw_charges = fetch_charges_for_ledger(soap_client, location_code, ledger_id)
            charges = []
            for c in raw_charges:
                transformed = transform_charge(c, ledger_id, site_id_for_charge, extract_date)
                if transformed['ChargeID']:
                    charges.append(transformed)
            return charges
        except Exception:
            return []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_ledger_charges, lid): lid for lid in ledger_ids}
        with tqdm(total=len(ledger_ids), desc="        Ledgers", unit="l", leave=False) as pbar:
            for future in as_completed(futures):
                charges = future.result()
                all_charges.extend(charges)
                pbar.update(1)

    return all_tenants, all_ledgers, all_charges


def process_site_chunked(
    soap_client: SOAPClient,
    site_id: int,
    tenant_ids: List[int],
    location_code: str,
    extract_date: date,
    config: DataLayerConfig,
    chunk_size: int = 500,
    max_workers: int = 20,
    db_chunk_size: int = 500
) -> Tuple[int, int, int]:
    """
    Process all tenants for a site in chunks, pushing to DB after each chunk.

    Args:
        soap_client: SOAP client instance
        site_id: SiteID
        tenant_ids: List of TenantIDs to process
        location_code: Location code (e.g., L001)
        extract_date: Date for extract_date field
        config: DataLayerConfig for database connection
        chunk_size: Number of tenants per chunk (default: 500)
        max_workers: Number of parallel threads (default: 20)
        db_chunk_size: Batch size for database upserts (default: 500)

    Returns:
        Tuple of (total_tenants, total_ledgers, total_charges) counts
    """
    total_tenants = 0
    total_ledgers = 0
    total_charges = 0

    # Split tenant_ids into chunks
    chunks = [tenant_ids[i:i + chunk_size] for i in range(0, len(tenant_ids), chunk_size)]
    print(f"    Processing {len(tenant_ids)} tenants in {len(chunks)} chunks of {chunk_size}...")

    for chunk_idx, chunk_tenant_ids in enumerate(chunks):
        print(f"      Chunk {chunk_idx + 1}/{len(chunks)} ({len(chunk_tenant_ids)} tenants)...")

        # Process chunk
        tenants, ledgers, charges = process_tenant_chunk(
            soap_client=soap_client,
            site_id=site_id,
            tenant_ids=chunk_tenant_ids,
            location_code=location_code,
            extract_date=extract_date,
            max_workers=max_workers
        )

        # Push chunk to database immediately
        if tenants or ledgers or charges:
            push_to_database(tenants, ledgers, charges, config, db_chunk_size, quiet=True)
            print(f"      -> Saved: {len(tenants)} tenants, {len(ledgers)} ledgers, {len(charges)} charges")

        total_tenants += len(tenants)
        total_ledgers += len(ledgers)
        total_charges += len(charges)

    return total_tenants, total_ledgers, total_charges


# =============================================================================
# Database Operations
# =============================================================================

def push_to_database(
    tenants: List[Dict],
    ledgers: List[Dict],
    charges: List[Dict],
    config: DataLayerConfig,
    chunk_size: int = 500,
    quiet: bool = False
) -> None:
    """Push all data to PostgreSQL database."""
    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    if not quiet:
        print("  Preparing database tables...")
    Base.metadata.create_all(engine, tables=[
        Tenant.__table__,
        Ledger.__table__,
        Charge.__table__
    ])
    if not quiet:
        print("    Tables ready")

    session_manager = SessionManager(engine)

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        # Upsert tenants
        if tenants:
            if not quiet:
                print(f"  Upserting {len(tenants)} tenants...")
            tenants = deduplicate_records(tenants, ['SiteID', 'TenantID'])
            if quiet:
                for i in range(0, len(tenants), chunk_size):
                    chunk = tenants[i:i + chunk_size]
                    upsert_ops.upsert_batch(
                        model=Tenant,
                        records=chunk,
                        constraint_columns=['SiteID', 'TenantID'],
                        chunk_size=chunk_size
                    )
            else:
                with tqdm(total=len(tenants), desc="    Tenants", unit="rec") as pbar:
                    for i in range(0, len(tenants), chunk_size):
                        chunk = tenants[i:i + chunk_size]
                        upsert_ops.upsert_batch(
                            model=Tenant,
                            records=chunk,
                            constraint_columns=['SiteID', 'TenantID'],
                            chunk_size=chunk_size
                        )
                        pbar.update(len(chunk))

        # Upsert ledgers
        if ledgers:
            if not quiet:
                print(f"  Upserting {len(ledgers)} ledgers...")
            ledgers = deduplicate_records(ledgers, ['SiteID', 'LedgerID'])
            if quiet:
                for i in range(0, len(ledgers), chunk_size):
                    chunk = ledgers[i:i + chunk_size]
                    upsert_ops.upsert_batch(
                        model=Ledger,
                        records=chunk,
                        constraint_columns=['SiteID', 'LedgerID'],
                        chunk_size=chunk_size
                    )
            else:
                with tqdm(total=len(ledgers), desc="    Ledgers", unit="rec") as pbar:
                    for i in range(0, len(ledgers), chunk_size):
                        chunk = ledgers[i:i + chunk_size]
                        upsert_ops.upsert_batch(
                            model=Ledger,
                            records=chunk,
                            constraint_columns=['SiteID', 'LedgerID'],
                            chunk_size=chunk_size
                        )
                        pbar.update(len(chunk))

        # Upsert charges
        if charges:
            if not quiet:
                print(f"  Upserting {len(charges)} charges...")
            charges = deduplicate_records(charges, ['SiteID', 'ChargeID', 'dcPmtAmt'])
            if not quiet:
                print(f"  After deduplication: {len(charges)} charges")

            failed_count = 0
            success_count = 0

            if quiet:
                for i in range(0, len(charges), chunk_size):
                    chunk = charges[i:i + chunk_size]
                    try:
                        upsert_ops.upsert_batch(
                            model=Charge,
                            records=chunk,
                            constraint_columns=['SiteID', 'ChargeID', 'dcPmtAmt'],
                            chunk_size=chunk_size
                        )
                        success_count += len(chunk)
                    except Exception as e:
                        failed_count += len(chunk)
                        if failed_count <= chunk_size:  # Only print first batch of errors
                            print(f"      ERROR in chunk starting at {i}: {str(e)[:200]}")
                            if chunk:
                                print(f"      First record: {chunk[0]}")
            else:
                with tqdm(total=len(charges), desc="    Charges", unit="rec") as pbar:
                    for i in range(0, len(charges), chunk_size):
                        chunk = charges[i:i + chunk_size]
                        try:
                            upsert_ops.upsert_batch(
                                model=Charge,
                                records=chunk,
                                constraint_columns=['SiteID', 'ChargeID', 'dcPmtAmt'],
                                chunk_size=chunk_size
                            )
                            success_count += len(chunk)
                        except Exception as e:
                            failed_count += len(chunk)
                            if failed_count <= chunk_size:  # Only print first batch of errors
                                print(f"\n      ERROR in chunk starting at {i}: {str(e)[:200]}")
                                if chunk:
                                    print(f"      First record: {chunk[0]}")
                        pbar.update(len(chunk))

            if not quiet and failed_count > 0:
                print(f"  Charges: {success_count} succeeded, {failed_count} failed")


# =============================================================================
# CLI and Main
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Historical Tenant-Ledger-Charges Load from Local SQL Server',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load all tenants from all sites
  python tenant_ledger_charges_historical.py

  # Load specific location only
  python tenant_ledger_charges_historical.py --location L001

  # Load specific SiteID only
  python tenant_ledger_charges_historical.py --site-id 48
        """
    )

    parser.add_argument(
        '--location',
        type=str,
        default=None,
        help='Specific location code to process (e.g., L001)'
    )

    parser.add_argument(
        '--site-id',
        type=int,
        default=None,
        help='Specific SiteID to process (e.g., 48)'
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=500,
        help='Number of tenants per processing chunk (default: 500)'
    )

    parser.add_argument(
        '--db-batch-size',
        type=int,
        default=100,
        help='Database batch size for upserts (default: 100, smaller = safer)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=20,
        help='Number of parallel API workers (default: 20)'
    )

    return parser.parse_args()


def main():
    """Main function to run the historical tenant-ledger-charges pipeline.

    Fetches ALL data from local SQL Server (no API calls).
    """
    args = parse_args()

    # Load configuration
    config = DataLayerConfig.from_env()

    extract_date = date.today()

    # Print header
    print("=" * 70)
    print("Historical Tenant-Ledger-Charges Load (SQL-Only)")
    print("=" * 70)
    print(f"Source: Local SQL Server (LOUISVER-T14\\VSDOTNET/sldbclnt)")
    print(f"Extract Date: {extract_date}")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    print(f"Chunk Size: {args.chunk_size}")
    print(f"DB Batch Size: {args.db_batch_size}")
    print("=" * 70)

    # Connect to local SQL Server
    print("\nConnecting to local SQL Server...")
    try:
        local_engine = create_local_sql_engine()
        print("  Connected successfully")
    except Exception as e:
        print(f"  ERROR: Failed to connect to local SQL Server: {e}")
        return

    # Determine which SiteIDs to process
    site_ids_to_fetch = None
    if args.location:
        site_id = LOCATION_TO_SITE_ID.get(args.location)
        if not site_id:
            print(f"ERROR: Unknown location code: {args.location}")
            print(f"Available: {', '.join(sorted(LOCATION_TO_SITE_ID.keys()))}")
            return
        site_ids_to_fetch = [site_id]
        print(f"Filtering to location {args.location} (SiteID: {site_id})")
    elif args.site_id:
        if args.site_id not in SITE_ID_TO_LOCATION:
            print(f"ERROR: Unknown SiteID: {args.site_id}")
            print(f"Available: {', '.join(str(s) for s in sorted(SITE_ID_TO_LOCATION.keys()))}")
            return
        site_ids_to_fetch = [args.site_id]
        print(f"Filtering to SiteID {args.site_id} ({SITE_ID_TO_LOCATION[args.site_id]})")

    # =========================================================================
    # Fetch ALL data from local SQL Server
    # =========================================================================

    # Fetch tenants
    print("\n[1/3] Fetching tenants from local SQL Server...")
    sql_tenants = fetch_tenants_from_sql(local_engine, site_ids_to_fetch)
    print(f"  Found {len(sql_tenants)} tenants")

    # Fetch ledgers
    print("\n[2/3] Fetching ledgers from local SQL Server...")
    sql_ledgers = fetch_ledgers_from_sql(local_engine, site_ids_to_fetch)
    print(f"  Found {len(sql_ledgers)} ledgers")

    # Fetch charges (with ChargeDesc join)
    print("\n[3/3] Fetching charges from local SQL Server...")
    sql_charges = fetch_charges_from_sql(local_engine, site_ids_to_fetch)
    print(f"  Found {len(sql_charges)} charges")

    # Close local SQL connection
    local_engine.dispose()
    print("\n  Local SQL connection closed")

    if not sql_tenants and not sql_ledgers and not sql_charges:
        print("No data found. Exiting.")
        return

    # =========================================================================
    # Transform records
    # =========================================================================

    print("\nTransforming records...")

    # Transform tenants
    all_tenants = []
    for t in tqdm(sql_tenants, desc="  Tenants", unit="rec"):
        location_code = SITE_ID_TO_LOCATION.get(t['SiteID'], 'UNKNOWN')
        all_tenants.append(transform_tenant(t['SiteID'], t['TenantID'], location_code))

    # Transform ledgers
    all_ledgers = []
    for l in tqdm(sql_ledgers, desc="  Ledgers", unit="rec"):
        transformed = transform_ledger_from_sql(l, extract_date)
        if transformed['LedgerID']:
            all_ledgers.append(transformed)

    # Transform charges
    all_charges = []
    for c in tqdm(sql_charges, desc="  Charges", unit="rec"):
        transformed = transform_charge_from_sql(c, extract_date)
        if transformed['ChargeID']:
            all_charges.append(transformed)

    # =========================================================================
    # Push to PostgreSQL
    # =========================================================================

    # Quick validation - show sample records
    print("\nSample data (first record of each type):")
    if all_tenants:
        print(f"  Tenant: SiteID={all_tenants[0].get('SiteID')}, TenantID={all_tenants[0].get('TenantID')}")
    if all_ledgers:
        l = all_ledgers[0]
        print(f"  Ledger: SiteID={l.get('SiteID')}, LedgerID={l.get('LedgerID')}, dMovedIn={l.get('dMovedIn')}")
    if all_charges:
        c = all_charges[0]
        print(f"  Charge: SiteID={c.get('SiteID')}, ChargeID={c.get('ChargeID')}, LedgerID={c.get('LedgerID')}, dcPmtAmt={c.get('dcPmtAmt')}, dChgStrt={c.get('dChgStrt')}")

    print("\n" + "-" * 70)
    print("Pushing data to PostgreSQL...")
    push_to_database(all_tenants, all_ledgers, all_charges, config, args.db_batch_size)

    # Final summary
    print("\n" + "=" * 70)
    print("Historical load completed!")
    print(f"  Total tenants: {len(all_tenants)}")
    print(f"  Total ledgers: {len(all_ledgers)}")
    print(f"  Total charges: {len(all_charges)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
