"""
RentRoll to SQL Pipeline

Fetches RentRoll data from SOAP API and pushes to PostgreSQL database.

Features:
- Two modes: manual (specify date range) and auto (previous + current month)
- Smart extract_date: closed months use last day, current month uses today
- Fetches data for multiple locations
- Uses merge/upsert on composite key (extract_date + SiteID + UnitID)
- Processes in chunks for large datasets

Usage:
    # Manual mode - specify date range (for historical loads)
    python rentroll_to_sql.py --mode manual --start 2025-01 --end 2025-12

    # Automatic mode - previous month + current month (for scheduler)
    python rentroll_to_sql.py --mode auto

Configuration (in .env):
    - SOAP_* : SOAP API connection settings
    - RENTROLL_LOCATION_CODES: Comma-separated location codes
    - RENTROLL_SQL_CHUNK_SIZE: Batch size for upsert (default: 500)
"""

import argparse
from datetime import datetime, date
from typing import List, Dict, Any
from decouple import config as env_config, Csv
from tqdm import tqdm

from common import (
    DataLayerConfig,
    SOAPClient,
    SOAPReportClient,
    create_engine_from_config,
    SessionManager,
    UpsertOperations,
    Base,
    RentRoll,
    # Date utilities
    get_last_day_of_month,
    get_extract_date,
    get_date_range_manual,
    get_date_range_auto,
    # Data utilities
    convert_to_bool,
    convert_to_int,
    convert_to_decimal,
    convert_to_datetime,
    deduplicate_records,
    # Current month delete utility
    delete_current_month_records,
)


# =============================================================================
# RentRoll-specific Transformation
# =============================================================================

def transform_record(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform API record to database-ready format."""
    return {
        # Primary key columns
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'UnitID': convert_to_int(record.get('UnitID')),

        # Core unit information
        'LedgerID': convert_to_int(record.get('LedgerID')),
        'sUnit': record.get('sUnit'),
        'sSize': record.get('sSize'),
        'Area': convert_to_decimal(record.get('Area')),
        'sUnitName': record.get('sUnitName'),
        'UnitTypeID': convert_to_int(record.get('UnitTypeID')),
        'sTypeName': record.get('sTypeName'),

        # Physical unit attributes
        'iFloor': convert_to_int(record.get('iFloor')),
        'dcWidth': convert_to_decimal(record.get('dcWidth')),
        'dcLength': convert_to_decimal(record.get('dcLength')),
        'iWalkThruOrder': convert_to_int(record.get('iWalkThruOrder')),
        'iDoorType': convert_to_int(record.get('iDoorType')),

        # Map/location attributes
        'dcMapTop': convert_to_decimal(record.get('dcMapTop')),
        'dcMapLeft': convert_to_decimal(record.get('dcMapLeft')),
        'dcMapTheta': convert_to_decimal(record.get('dcMapTheta')),
        'bMapReversWL': convert_to_bool(record.get('bMapReversWL')),
        'iEntryLoc': convert_to_int(record.get('iEntryLoc')),

        # Pricing information
        'dcPushRate': convert_to_decimal(record.get('dcPushRate')),
        'dcStdRate': convert_to_decimal(record.get('dcStdRate')),
        'dcStdWeeklyRate': convert_to_decimal(record.get('dcStdWeeklyRate')),
        'dcStdSecDep': convert_to_decimal(record.get('dcStdSecDep')),
        'dcStdLateFee': convert_to_decimal(record.get('dcStdLateFee')),
        'dcWebRate': convert_to_decimal(record.get('dcWebRate')),
        'dcWebPushRate': convert_to_decimal(record.get('dcWebPushRate')),
        'dcWebRateDated': convert_to_decimal(record.get('dcWebRateDated')),
        'dcSchedRateMonthly': convert_to_decimal(record.get('dcSchedRateMonthly')),
        'dcSchedRateWeekly': convert_to_decimal(record.get('dcSchedRateWeekly')),

        # Unit features (booleans)
        'bPower': convert_to_bool(record.get('bPower')),
        'bClimate': convert_to_bool(record.get('bClimate')),
        'bInside': convert_to_bool(record.get('bInside')),
        'bAlarm': convert_to_bool(record.get('bAlarm')),
        'bRentable': convert_to_bool(record.get('bRentable')),
        'bRented': convert_to_bool(record.get('bRented')),
        'bCorporate': convert_to_bool(record.get('bCorporate')),
        'bMobile': convert_to_bool(record.get('bMobile')),
        'bDamaged': convert_to_bool(record.get('bDamaged')),
        'bCollapsible': convert_to_bool(record.get('bCollapsible')),
        'bPermanent': convert_to_bool(record.get('bPermanent')),
        'bExcludeFromSqftReports': convert_to_bool(record.get('bExcludeFromSqftReports')),
        'bExcludeFromWebsite': convert_to_bool(record.get('bExcludeFromWebsite')),
        'bNotReadyToRent': convert_to_bool(record.get('bNotReadyToRent')),
        'bExcludeFromInsurance': convert_to_bool(record.get('bExcludeFromInsurance')),

        # Unit status & tracking
        'iMobileStatus': convert_to_int(record.get('iMobileStatus')),
        'iADA': convert_to_int(record.get('iADA')),
        'iVehicleStorageAllowed': convert_to_int(record.get('iVehicleStorageAllowed')),
        'iDaysVacant': convert_to_int(record.get('iDaysVacant')),
        'EmployeeID': convert_to_int(record.get('EmployeeID')),

        # Dates
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'dUnitNote': convert_to_datetime(record.get('dUnitNote')),
        'dLeaseDate': convert_to_datetime(record.get('dLeaseDate')),
        'dPaidThru': convert_to_datetime(record.get('dPaidThru')),
        'dRentLastChanged': convert_to_datetime(record.get('dRentLastChanged')),
        'dSchedRentStrt': convert_to_datetime(record.get('dSchedRentStrt')),

        # Tenant information
        'TenantID': convert_to_int(record.get('TenantID')),
        'sTenant': record.get('sTenant'),
        'sCompany': record.get('sCompany'),
        'sEmail': record.get('sEmail'),
        'iAnnivDays': convert_to_int(record.get('iAnnivDays')),
        'sTaxExempt': record.get('sTaxExempt'),

        # Rental rates & charges
        'dcSecDep': convert_to_decimal(record.get('dcSecDep')),
        'dcStandardRate': convert_to_decimal(record.get('dcStandardRate')),
        'dcRent': convert_to_decimal(record.get('dcRent')),
        'dcVar': convert_to_decimal(record.get('dcVar')),
        'dcSchedRent': convert_to_decimal(record.get('dcSchedRent')),
        'dcPrePaidRentLiability': convert_to_decimal(record.get('dcPrePaidRentLiability')),
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),

        # Billing & payment
        'iAutoBillType': convert_to_int(record.get('iAutoBillType')),
        'DaysSame': convert_to_int(record.get('DaysSame')),

        # Legacy & system fields
        'SiteID1': convert_to_int(record.get('SiteID1')),
        'Area1': convert_to_decimal(record.get('Area1')),
        'OldPK': record.get('OldPK'),
        'uTS': record.get('uTS'),
        'sUnitNote': record.get('sUnitNote'),
    }


# =============================================================================
# Data Operations
# =============================================================================

def fetch_rentroll_data(
    report_client: SOAPReportClient,
    location_codes: List[str],
    start_date: datetime,
    end_date: datetime,
    extract_date: date
) -> List[Dict[str, Any]]:
    """Fetch RentRoll data for multiple locations."""
    all_data = []

    with tqdm(total=len(location_codes), desc="  Fetching locations", unit="loc") as pbar:
        for location_code in location_codes:
            try:
                results = report_client.get_rent_roll(
                    location_code=location_code,
                    start_date=start_date,
                    end_date=end_date
                )

                for record in results:
                    transformed = transform_record(record, extract_date)
                    all_data.append(transformed)

                pbar.set_postfix({"location": location_code, "records": len(results)})
                pbar.update(1)

            except Exception as e:
                pbar.set_postfix({"location": location_code, "status": "ERROR"})
                pbar.update(1)
                tqdm.write(f"  ✗ {location_code}: Error - {str(e)}")
                continue

    # Deduplicate by primary key (extract_date, SiteID, UnitID)
    original_count = len(all_data)
    all_data = deduplicate_records(all_data, ['extract_date', 'SiteID', 'UnitID'])
    if len(all_data) < original_count:
        tqdm.write(f"  ℹ Deduplicated: {original_count} → {len(all_data)} records")

    return all_data


def push_to_database(
    data: List[Dict[str, Any]],
    config: DataLayerConfig,
    year: int,
    month: int,
    status: str
) -> None:
    """Push rentroll data to PostgreSQL database.

    For current month extracts, deletes previous records first to avoid
    accumulation. Historical (closed) months use upsert as before.

    Args:
        data: List of transformed records
        config: DataLayerConfig with database settings
        year: Extract year
        month: Extract month
        status: "current" or "closed" - determines delete behavior
    """
    if not data:
        print("  ⚠ No data to push")
        return

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    with tqdm(total=1, desc="  Preparing database", bar_format='{desc}') as pbar:
        Base.metadata.create_all(engine, tables=[RentRoll.__table__])
        pbar.update(1)
    tqdm.write("  ✓ Table 'rentroll' ready")

    session_manager = SessionManager(engine)
    chunk_size = env_config('RENTROLL_SQL_CHUNK_SIZE', default=500, cast=int)
    num_chunks = (len(data) + chunk_size - 1) // chunk_size

    with session_manager.session_scope() as session:
        # For current month, delete previous records first to avoid accumulation
        if status == "current":
            deleted = delete_current_month_records(session, RentRoll, year, month)
            tqdm.write(f"  ✓ Deleted {deleted} previous current-month records")

        upsert_ops = UpsertOperations(session, db_config.db_type)

        with tqdm(total=len(data), desc="  Upserting records", unit="rec") as pbar:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]

                upsert_ops.upsert_batch(
                    model=RentRoll,
                    records=chunk,
                    constraint_columns=['extract_date', 'SiteID', 'UnitID'],
                    chunk_size=chunk_size
                )

                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i//chunk_size + 1}/{num_chunks}"})

    tqdm.write(f"  ✓ Upserted {len(data)} records to PostgreSQL")


# =============================================================================
# CLI and Main
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='RentRoll to SQL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Manual mode - load historical data
  python rentroll_to_sql.py --mode manual --start 2025-01 --end 2025-12

  # Automatic mode - previous month + current month (for scheduler)
  python rentroll_to_sql.py --mode auto
        """
    )

    parser.add_argument(
        '--mode',
        choices=['manual', 'auto'],
        required=True,
        help='Extraction mode: manual (specify date range) or auto (prev + current month)'
    )

    parser.add_argument(
        '--start',
        type=str,
        help='Start month in YYYY-MM format (required for manual mode)'
    )

    parser.add_argument(
        '--end',
        type=str,
        help='End month in YYYY-MM format (required for manual mode)'
    )

    args = parser.parse_args()

    # Validate manual mode requires start and end
    if args.mode == 'manual':
        if not args.start or not args.end:
            parser.error("Manual mode requires --start and --end arguments")

    return args


def main():
    """Main function to fetch and push RentRoll data to SQL."""

    args = parse_args()

    # Load configuration
    config = DataLayerConfig.from_env()

    if not config.soap:
        raise ValueError("SOAP configuration not found in .env")

    # Load location codes from .env
    location_codes = env_config('RENTROLL_LOCATION_CODES', cast=Csv())

    # Determine date range based on mode
    if args.mode == 'manual':
        months = get_date_range_manual(args.start, args.end)
        mode_label = "MANUAL"
        date_range_str = f"{args.start} to {args.end}"
    else:
        months = get_date_range_auto()
        mode_label = "AUTOMATIC"
        date_range_str = ", ".join([f"{y}-{m:02d}" for y, m in months])

    # Initialize SOAP client
    soap_client = SOAPClient(
        base_url=config.soap.base_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries
    )

    report_client = SOAPReportClient(soap_client)

    # Print header
    print("=" * 70)
    print("RentRoll to SQL Pipeline")
    print("=" * 70)
    print(f"Mode: {mode_label}")
    print(f"Date Range: {date_range_str}")
    print(f"Locations: {', '.join(location_codes)}")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    print("=" * 70)

    total_records = 0

    # Process each month
    for year, month in months:
        # Calculate first and last day of month
        first_day = datetime(year, month, 1)
        last_day_dt = datetime.combine(get_last_day_of_month(year, month), datetime.min.time())

        # Get extract_date based on whether month is closed or current
        extract_date, status = get_extract_date(year, month)

        print(f"\n[{first_day.strftime('%b %Y')}] - Extract Date: {extract_date} ({status})")

        # Fetch data for all locations
        all_data = fetch_rentroll_data(
            report_client=report_client,
            location_codes=location_codes,
            start_date=first_day,
            end_date=last_day_dt,
            extract_date=extract_date
        )

        # Push to database
        if all_data:
            push_to_database(all_data, config, year, month, status)
            total_records += len(all_data)
        else:
            print(f"  ⚠ No data found for {first_day.strftime('%b %Y')}")

    # Close SOAP client
    soap_client.close()

    print("\n" + "=" * 70)
    print(f"Pipeline completed! Total records: {total_records}")
    print("=" * 70)


if __name__ == "__main__":
    main()
