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
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple, Optional
from decimal import Decimal
from decouple import config as env_config, Csv
from tqdm import tqdm

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
        'UnitID': convert_to_int(record.get('UnitID')),
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


# =============================================================================
# Data Pipeline Functions
# =============================================================================

def process_location(
    soap_client: SOAPClient,
    location_code: str,
    extract_date: date,
    incremental_since: Optional[datetime] = None,
    max_workers: int = 10
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Process a single location through the 3-step workflow.

    Args:
        soap_client: SOAP client instance
        location_code: Location code (e.g., L001)
        extract_date: Date for extract_date field
        incremental_since: If set, only fetch charges for ledgers with dUpdated > this datetime
        max_workers: Max concurrent workers (not used yet)

    Returns:
        Tuple of (tenant_records, ledger_records, charge_records)
    """
    all_tenants = []
    all_ledgers = []
    all_charges = []

    mode_label = "INCREMENTAL" if incremental_since else "FULL"

    # Step 1: Fetch all tenants (always full refresh - fast)
    print(f"  Step 1: Fetching tenants...")
    try:
        raw_tenants = fetch_tenants(soap_client, location_code)
        tenant_ids = []
        for t in raw_tenants:
            transformed = transform_tenant(t, location_code)
            if transformed['TenantID']:  # Skip if no TenantID
                all_tenants.append(transformed)
                tenant_ids.append(transformed['TenantID'])
        print(f"    Found {len(tenant_ids)} tenants")
    except Exception as e:
        print(f"    ERROR fetching tenants: {e}")
        return all_tenants, all_ledgers, all_charges

    if not tenant_ids:
        print("    No tenants found, skipping...")
        return all_tenants, all_ledgers, all_charges

    # Step 2: Fetch ledgers for all tenants (always full - catches new ledgers)
    print(f"  Step 2: Fetching ledgers for {len(tenant_ids)} tenants...")
    ledger_to_site = {}  # Map ledger_id -> site_id for charge fetching
    ledgers_for_charges = []  # Ledgers that need charge refresh

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
        except Exception as e:
            return []

    # Sequential processing for ledgers
    with tqdm(total=len(tenant_ids), desc="    Tenants", unit="t") as pbar:
        for tenant_id in tenant_ids:
            ledgers = fetch_tenant_ledgers(tenant_id)
            for l in ledgers:
                all_ledgers.append(l)
                ledger_to_site[l['LedgerID']] = l['SiteID']

                # For incremental mode, only include ledgers created/updated since threshold
                if incremental_since:
                    ledger_created = l.get('dCreated')
                    ledger_updated = l.get('dUpdated')
                    # Include if either dCreated or dUpdated is after threshold
                    is_new = ledger_created and ledger_created > incremental_since
                    is_modified = ledger_updated and ledger_updated > incremental_since
                    if is_new or is_modified:
                        ledgers_for_charges.append(l)
                else:
                    ledgers_for_charges.append(l)

            pbar.update(1)

    print(f"    Found {len(all_ledgers)} ledgers total")

    if incremental_since:
        print(f"    Ledgers created/updated since {incremental_since.date()}: {len(ledgers_for_charges)}")

    if not ledgers_for_charges:
        print("    No ledgers need charge refresh, skipping...")
        return all_tenants, all_ledgers, all_charges

    # Step 3: Fetch charges for selected ledgers
    ledger_ids_for_charges = [l['LedgerID'] for l in ledgers_for_charges]
    print(f"  Step 3: Fetching charges for {len(ledger_ids_for_charges)} ledgers...")

    def fetch_ledger_charges(ledger_id: int) -> List[Dict]:
        """Fetch charges for a single ledger."""
        try:
            site_id = ledger_to_site.get(ledger_id)
            raw_charges = fetch_charges_for_ledger(soap_client, location_code, ledger_id)
            charges = []
            for c in raw_charges:
                transformed = transform_charge(c, ledger_id, site_id, extract_date)
                if transformed['ChargeID']:
                    charges.append(transformed)
            return charges
        except Exception as e:
            return []

    # Sequential processing for charges
    with tqdm(total=len(ledger_ids_for_charges), desc="    Ledgers", unit="l") as pbar:
        for ledger_id in ledger_ids_for_charges:
            charges = fetch_ledger_charges(ledger_id)
            all_charges.extend(charges)
            pbar.update(1)

    print(f"    Found {len(all_charges)} charges")

    return all_tenants, all_ledgers, all_charges


# =============================================================================
# Database Operations
# =============================================================================

def push_to_database(
    tenants: List[Dict],
    ledgers: List[Dict],
    charges: List[Dict],
    config: DataLayerConfig,
    chunk_size: int = 500
) -> None:
    """Push all data to PostgreSQL database."""
    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    # Create tables if not exist
    print("  Preparing database tables...")
    Base.metadata.create_all(engine, tables=[
        Tenant.__table__,
        Ledger.__table__,
        Charge.__table__
    ])
    print("    Tables ready")

    session_manager = SessionManager(engine)

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        # Upsert tenants
        if tenants:
            print(f"  Upserting {len(tenants)} tenants...")
            tenants = deduplicate_records(tenants, ['SiteID', 'TenantID'])
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
            print(f"  Upserting {len(ledgers)} ledgers...")
            ledgers = deduplicate_records(ledgers, ['SiteID', 'LedgerID'])
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
            print(f"  Upserting {len(charges)} charges...")
            charges = deduplicate_records(charges, ['SiteID', 'ChargeID', 'dcPmtAmt'])
            with tqdm(total=len(charges), desc="    Charges", unit="rec") as pbar:
                for i in range(0, len(charges), chunk_size):
                    chunk = charges[i:i + chunk_size]
                    upsert_ops.upsert_batch(
                        model=Charge,
                        records=chunk,
                        constraint_columns=['SiteID', 'ChargeID', 'dcPmtAmt'],
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

    # Print header
    print("=" * 70)
    print("Tenant-Ledger-Charges to SQL Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode.upper()}")
    if incremental_since:
        print(f"Incremental Since: {incremental_since.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Extract Date: {extract_date}")
    print(f"Locations: {', '.join(location_codes)}")
    print(f"Service URL: {cc_url}")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    print("=" * 70)

    # Collect all data
    all_tenants = []
    all_ledgers = []
    all_charges = []

    for location_code in location_codes:
        print(f"\n[{location_code}] Processing...")

        tenants, ledgers, charges = process_location(
            soap_client=soap_client,
            location_code=location_code,
            extract_date=extract_date,
            incremental_since=incremental_since
        )

        all_tenants.extend(tenants)
        all_ledgers.extend(ledgers)
        all_charges.extend(charges)

        print(f"  Summary: {len(tenants)} tenants, {len(ledgers)} ledgers, {len(charges)} charges")

    # Push to database
    print("\n" + "-" * 70)
    print("Pushing data to database...")
    push_to_database(all_tenants, all_ledgers, all_charges, config, chunk_size)

    # Close SOAP client
    soap_client.close()

    # Final summary
    print("\n" + "=" * 70)
    print("Pipeline completed!")
    print(f"  Total tenants: {len(all_tenants)}")
    print(f"  Total ledgers: {len(all_ledgers)}")
    print(f"  Total charges: {len(all_charges)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
