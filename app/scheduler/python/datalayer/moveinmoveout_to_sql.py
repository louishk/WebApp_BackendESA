"""
MoveInMoveOut to SQL Pipeline

Fetches MoveInsAndMoveOuts data from SOAP API and pushes to PostgreSQL database.

Features:
- Cumulative data model (no extract_date in primary key)
- Two modes: manual (specify date range) and auto (D-60 delete + repush)
- Auto mode: deletes records where MoveDate >= D-60, then repushes all data from D-60 onwards
- Manual mode: fetches data for specified date range (YYYY-MM-DD format)
- Uses merge/upsert on composite key (SiteID + TenantID + MoveDate)
- Processes in chunks for large datasets

Usage:
    # Manual mode - specify date range (for historical loads)
    python moveinmoveout_to_sql.py --mode manual --start 2025-01-01 --end 2025-06-30

    # Automatic mode - delete D-60 and repush (for scheduler)
    python moveinmoveout_to_sql.py --mode auto

Configuration (in .env):
    - SOAP_* : SOAP API connection settings
    - MOVEINMOVEOUT_LOCATION_CODES: Comma-separated location codes (or uses RENTROLL_LOCATION_CODES)
    - MOVEINMOVEOUT_SQL_CHUNK_SIZE: Batch size for upsert (default: 500)
    - MOVEINMOVEOUT_DAYS_BACK: Days to look back in auto mode (default: 60)
    - MOVEINMOVEOUT_DAYS_FORWARD: Days to look forward in auto mode (default: 365)
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
    MoveInsAndMoveOuts,
    # Date utilities
    get_date_range_days_back,
    parse_date_string,
    # Data utilities
    convert_to_bool,
    convert_to_int,
    convert_to_decimal,
    convert_to_datetime,
    deduplicate_records,
)


# =============================================================================
# MoveInMoveOut-specific Transformation
# =============================================================================

def transform_record(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    """Transform API record to database-ready format."""
    return {
        # Primary key columns (cumulative - no extract_date in PK)
        'SiteID': convert_to_int(record.get('SiteID')),
        'TenantID': convert_to_int(record.get('TenantID')),
        'MoveDate': convert_to_datetime(record.get('MoveDate')),

        # Tracking column (not part of PK)
        'extract_date': extract_date,

        # Activity Type
        'MoveIn': convert_to_int(record.get('MoveIn')),
        'MoveOut': convert_to_int(record.get('MoveOut')),
        'Transfer': convert_to_int(record.get('Transfer')),

        # Unit Info
        'UnitName': record.get('UnitName'),
        'UnitSize': record.get('UnitSize'),
        'Width': convert_to_decimal(record.get('Width')),
        'Length': convert_to_decimal(record.get('Length')),
        'sUnitType': record.get('sUnitType'),

        # Tenant Info
        'TenantName': record.get('TenantName'),
        'sCompany': record.get('sCompany'),
        'sEmail': record.get('sEmail'),
        'Address': record.get('Address'),
        'City': record.get('City'),
        'Region': record.get('Region'),
        'PostalCode': record.get('PostalCode'),
        'Country': record.get('Country'),

        # Rate Info
        'StandardRate': convert_to_decimal(record.get('StandardRate')),
        'MovedInArea': convert_to_decimal(record.get('MovedInArea')),
        'MovedInRentalRate': convert_to_decimal(record.get('MovedInRentalRate')),
        'MovedInVariance': convert_to_decimal(record.get('MovedInVariance')),
        'MovedInDaysVacant': convert_to_int(record.get('MovedInDaysVacant')),
        'MovedOutArea': convert_to_decimal(record.get('MovedOutArea')),
        'MovedOutRentalRate': convert_to_decimal(record.get('MovedOutRentalRate')),
        'MovedOutVariance': convert_to_decimal(record.get('MovedOutVariance')),
        'MovedOutDaysRented': convert_to_int(record.get('MovedOutDaysRented')),

        # Additional Fields
        'iLeaseNum': convert_to_int(record.get('iLeaseNum')),
        'dRentLastChanged': convert_to_datetime(record.get('dRentLastChanged')),
        'sLicPlate': record.get('sLicPlate'),
        'sEmpInitials': record.get('sEmpInitials'),
        'sPlanTerm': record.get('sPlanTerm'),
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),
        'dcDiscount': convert_to_decimal(record.get('dcDiscount')),
        'sDiscountPlan': record.get('sDiscountPlan'),
        'iAuctioned': convert_to_int(record.get('iAuctioned')),
        'sAuctioned': record.get('sAuctioned'),
        'iDaysSinceMoveOut': convert_to_int(record.get('iDaysSinceMoveOut')),
        'dcAmtPaid': convert_to_decimal(record.get('dcAmtPaid')),
        'sSource': record.get('sSource'),

        # Features
        'bPower': convert_to_bool(record.get('bPower')),
        'bClimate': convert_to_bool(record.get('bClimate')),
        'bAlarm': convert_to_bool(record.get('bAlarm')),
        'bInside': convert_to_bool(record.get('bInside')),

        # Move-in Rates
        'dcPushRateAtMoveIn': convert_to_decimal(record.get('dcPushRateAtMoveIn')),
        'dcStdRateAtMoveIn': convert_to_decimal(record.get('dcStdRateAtMoveIn')),
        'dcInsurPremiumAtMoveIn': convert_to_decimal(record.get('dcInsurPremiumAtMoveIn')),
        'sDiscountPlanAtMoveIn': record.get('sDiscountPlanAtMoveIn'),

        # Inquiry/Waiting Info
        'WaitingID': convert_to_int(record.get('WaitingID')),
        'InquiryEmployeeID': convert_to_int(record.get('InquiryEmployeeID')),
        'sInquiryPlacedBy': record.get('sInquiryPlacedBy'),
        'CorpUserID_Placed': convert_to_int(record.get('CorpUserID_Placed')),
        'CorpUserID_ConvertedToMoveIn': convert_to_int(record.get('CorpUserID_ConvertedToMoveIn')),
    }


# =============================================================================
# Data Operations
# =============================================================================

def fetch_moveinmoveout_data(
    report_client: SOAPReportClient,
    location_codes: List[str],
    start_date: date,
    end_date: date,
    extract_date: date
) -> List[Dict[str, Any]]:
    """Fetch MoveInsAndMoveOuts data for multiple locations."""
    all_data = []

    with tqdm(total=len(location_codes), desc="  Fetching locations", unit="loc") as pbar:
        for location_code in location_codes:
            try:
                results = report_client.call_report(
                    report_name='move_ins_and_move_outs',
                    parameters={
                        'sLocationCode': location_code,
                        'dReportDateStart': start_date.strftime('%Y-%m-%dT00:00:00'),
                        'dReportDateEnd': end_date.strftime('%Y-%m-%dT23:59:59'),
                    }
                )

                for record in results:
                    transformed = transform_record(record, extract_date)
                    all_data.append(transformed)

                pbar.set_postfix({"location": location_code, "records": len(results)})
                pbar.update(1)

            except Exception as e:
                pbar.set_postfix({"location": location_code, "status": "ERROR"})
                pbar.update(1)
                tqdm.write(f"  x {location_code}: Error - {str(e)}")
                continue

    # Deduplicate by primary key (SiteID, TenantID, MoveDate)
    original_count = len(all_data)
    all_data = deduplicate_records(all_data, ['SiteID', 'TenantID', 'MoveDate'])
    if len(all_data) < original_count:
        tqdm.write(f"  i Deduplicated: {original_count} -> {len(all_data)} records")

    return all_data


def delete_recent_records(
    config: DataLayerConfig,
    delete_from_date: date
) -> int:
    """Delete records where MoveDate >= delete_from_date."""
    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)
    session_manager = SessionManager(engine)

    deleted_count = 0
    with session_manager.session_scope() as session:
        # Convert date to datetime for comparison with DateTime column
        delete_from_datetime = datetime.combine(delete_from_date, datetime.min.time())

        deleted_count = session.query(MoveInsAndMoveOuts).filter(
            MoveInsAndMoveOuts.MoveDate >= delete_from_datetime
        ).delete()

    return deleted_count


def push_to_database(
    data: List[Dict[str, Any]],
    config: DataLayerConfig
) -> None:
    """Push moveinmoveout data to PostgreSQL database."""
    if not data:
        print("  ! No data to push")
        return

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    with tqdm(total=1, desc="  Preparing database", bar_format='{desc}') as pbar:
        Base.metadata.create_all(engine, tables=[MoveInsAndMoveOuts.__table__])
        pbar.update(1)
    tqdm.write("  v Table 'mimo' ready")

    session_manager = SessionManager(engine)
    chunk_size = env_config('MOVEINMOVEOUT_SQL_CHUNK_SIZE', default=500, cast=int)
    num_chunks = (len(data) + chunk_size - 1) // chunk_size

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        with tqdm(total=len(data), desc="  Upserting records", unit="rec") as pbar:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]

                upsert_ops.upsert_batch(
                    model=MoveInsAndMoveOuts,
                    records=chunk,
                    constraint_columns=['SiteID', 'TenantID', 'MoveDate'],
                    chunk_size=chunk_size
                )

                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i//chunk_size + 1}/{num_chunks}"})

    tqdm.write(f"  v Upserted {len(data)} records to PostgreSQL")


# =============================================================================
# CLI and Main
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='MoveInMoveOut to SQL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Manual mode - load data for specific date range
  python moveinmoveout_to_sql.py --mode manual --start 2025-01-01 --end 2025-06-30

  # Automatic mode - delete D-60 and repush (for scheduler)
  python moveinmoveout_to_sql.py --mode auto
        """
    )

    parser.add_argument(
        '--mode',
        choices=['manual', 'auto'],
        required=True,
        help='Extraction mode: manual (specify date range) or auto (delete D-60 + repush)'
    )

    parser.add_argument(
        '--start',
        type=str,
        help='Start date in YYYY-MM-DD format (required for manual mode)'
    )

    parser.add_argument(
        '--end',
        type=str,
        help='End date in YYYY-MM-DD format (required for manual mode)'
    )

    args = parser.parse_args()

    # Validate manual mode requires start and end
    if args.mode == 'manual':
        if not args.start or not args.end:
            parser.error("Manual mode requires --start and --end arguments in YYYY-MM-DD format")

    return args


def main():
    """Main function to fetch and push MoveInMoveOut data to SQL."""

    args = parse_args()

    # Load configuration
    config = DataLayerConfig.from_env()

    if not config.soap:
        raise ValueError("SOAP configuration not found in .env")

    # Load location codes from .env (fallback to RENTROLL_LOCATION_CODES)
    location_codes = env_config(
        'MOVEINMOVEOUT_LOCATION_CODES',
        default=env_config('RENTROLL_LOCATION_CODES', default=''),
        cast=Csv()
    )

    # Load auto mode settings
    days_back = env_config('MOVEINMOVEOUT_DAYS_BACK', default=60, cast=int)
    days_forward = env_config('MOVEINMOVEOUT_DAYS_FORWARD', default=365, cast=int)

    # Determine date range based on mode
    if args.mode == 'manual':
        start_date = parse_date_string(args.start)
        end_date = parse_date_string(args.end)
        mode_label = "MANUAL"
        date_range_str = f"{args.start} to {args.end}"
        delete_before_push = False
    else:
        start_date, end_date = get_date_range_days_back(days_back, days_forward)
        mode_label = "AUTOMATIC"
        date_range_str = f"D-{days_back} ({start_date}) to D+{days_forward} ({end_date})"
        delete_before_push = True

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
    print("MoveInMoveOut to SQL Pipeline")
    print("=" * 70)
    print(f"Mode: {mode_label}")
    print(f"Date Range: {date_range_str}")
    print(f"Locations: {', '.join(location_codes)}")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    print("=" * 70)

    # For auto mode, delete recent records first
    if delete_before_push:
        print(f"\n[Step 1] Deleting records where MoveDate >= {start_date}")
        try:
            deleted_count = delete_recent_records(config, start_date)
            print(f"  v Deleted {deleted_count} records")
        except Exception as e:
            print(f"  ! Delete failed (table may not exist yet): {str(e)}")

    # Fetch data
    step_num = 2 if delete_before_push else 1
    print(f"\n[Step {step_num}] Fetching data from {start_date} to {end_date}")

    # Use today as extract_date for tracking purposes
    extract_date = date.today()

    all_data = fetch_moveinmoveout_data(
        report_client=report_client,
        location_codes=location_codes,
        start_date=start_date,
        end_date=end_date,
        extract_date=extract_date
    )

    # Push to database
    step_num += 1
    print(f"\n[Step {step_num}] Pushing data to database")
    if all_data:
        push_to_database(all_data, config)
    else:
        print(f"  ! No data found for date range")

    # Close SOAP client
    soap_client.close()

    print("\n" + "=" * 70)
    print(f"Pipeline completed! Total records: {len(all_data)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
