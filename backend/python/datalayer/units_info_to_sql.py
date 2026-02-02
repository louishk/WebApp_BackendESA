"""
UnitsInformation to SQL Pipeline

Fetches unit information from UnitsInformation_v3 SOAP API (CallCenterWs)
and pushes to PostgreSQL with standardized type categorization.

Features:
- Fetches all units for configured locations
- Standardizes sTypeName into sTypeName_clean (base type) and sTypeName_feature (climate)
- Original sTypeName preserved for audit/reference
- Uses upsert on composite key (SiteID + UnitID)
- Processes in chunks for large datasets

Usage:
    python units_info_to_sql.py

Configuration (in scheduler.yaml):
    pipelines.unitsinfo.location_codes: List of location codes
    pipelines.unitsinfo.sql_chunk_size: Batch size for upsert (default: 500)
"""

import sys
from pathlib import Path
from typing import List, Dict, Any
from tqdm import tqdm

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import (
    DataLayerConfig,
    SOAPClient,
    create_engine_from_config,
    SessionManager,
    UpsertOperations,
    Base,
    UnitsInfo,
    convert_to_bool,
    convert_to_int,
    convert_to_decimal,
    convert_to_datetime,
    deduplicate_records,
)
from common.config import get_pipeline_config


# =============================================================================
# CallCenterWs SOAP Configuration
# =============================================================================
CALL_CENTER_WS_URL = "https://api.smdservers.net/CCWs_3.5/CallCenterWs.asmx"
NAMESPACE = "http://tempuri.org/CallCenterWs/CallCenterWs"
SOAP_ACTION = "http://tempuri.org/CallCenterWs/CallCenterWs/UnitsInformation_v3"


# =============================================================================
# Record Transformation
# =============================================================================

def transform_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform API record to database-ready format (raw data only).

    Standardization/enrichment is done in SQL views, not here.

    Args:
        record: Raw record from UnitsInformation_v3 API

    Returns:
        Transformed record ready for database insertion
    """
    return {
        # Core identifiers
        'SiteID': convert_to_int(record.get('SiteID')),
        'UnitID': convert_to_int(record.get('UnitID')),
        'UnitTypeID': convert_to_int(record.get('UnitTypeID')),
        'sLocationCode': record.get('sLocationCode'),

        # Unit identification
        'sUnitName': record.get('sUnitName'),
        'sTypeName': record.get('sTypeName'),
        'sUnitNote': record.get('sUnitNote'),
        'sUnitDesc': record.get('sUnitDesc'),

        # Physical dimensions
        'dcWidth': convert_to_decimal(record.get('dcWidth')),
        'dcLength': convert_to_decimal(record.get('dcLength')),
        'iFloor': convert_to_int(record.get('iFloor')),
        'dcMapTheta': convert_to_decimal(record.get('dcMapTheta')),
        'bMapReversWL': convert_to_bool(record.get('bMapReversWL')),
        'iEntryLoc': convert_to_int(record.get('iEntryLoc')),
        'iDoorType': convert_to_int(record.get('iDoorType')),
        'iADA': convert_to_int(record.get('iADA')),

        # Feature flags
        'bClimate': convert_to_bool(record.get('bClimate')),
        'bPower': convert_to_bool(record.get('bPower')),
        'bInside': convert_to_bool(record.get('bInside')),
        'bAlarm': convert_to_bool(record.get('bAlarm')),
        'bRentable': convert_to_bool(record.get('bRentable')),
        'bMobile': convert_to_bool(record.get('bMobile')),
        'bServiceRequired': convert_to_bool(record.get('bServiceRequired')),
        'bExcludeFromWebsite': convert_to_bool(record.get('bExcludeFromWebsite')),

        # Rental status
        'bRented': convert_to_bool(record.get('bRented')),
        'bWaitingListReserved': convert_to_bool(record.get('bWaitingListReserved')),
        'bCorporate': convert_to_bool(record.get('bCorporate')),
        'iDaysVacant': convert_to_int(record.get('iDaysVacant')),
        'iDaysRented': convert_to_int(record.get('iDaysRented')),
        'dMovedIn': convert_to_datetime(record.get('dMovedIn')),

        # Lease configuration
        'iDefLeaseNum': convert_to_int(record.get('iDefLeaseNum')),
        'DefaultCoverageID': convert_to_int(record.get('DefaultCoverageID')),

        # Pricing
        'dcStdRate': convert_to_decimal(record.get('dcStdRate')),
        'dcWebRate': convert_to_decimal(record.get('dcWebRate')),
        'dcPushRate': convert_to_decimal(record.get('dcPushRate')),
        'dcPushRate_NotRounded': convert_to_decimal(record.get('dcPushRate_NotRounded')),
        'dcBoardRate': convert_to_decimal(record.get('dcBoardRate')),
        'dcPreferredRate': convert_to_decimal(record.get('dcPreferredRate')),
        'dcStdWeeklyRate': convert_to_decimal(record.get('dcStdWeeklyRate')),
        'dcStdSecDep': convert_to_decimal(record.get('dcStdSecDep')),
        'dcRM_RoundTo': convert_to_decimal(record.get('dcRM_RoundTo')),

        # Tax rates
        'dcTax1Rate': convert_to_decimal(record.get('dcTax1Rate')),
        'dcTax2Rate': convert_to_decimal(record.get('dcTax2Rate')),

        # Preferred channel
        'iPreferredChannelType': convert_to_int(record.get('iPreferredChannelType')),
        'bPreferredIsPushRate': convert_to_bool(record.get('bPreferredIsPushRate')),
    }


# =============================================================================
# Data Operations
# =============================================================================

def fetch_units_info(
    soap_client: SOAPClient,
    location_codes: List[str]
) -> List[Dict[str, Any]]:
    """
    Fetch units information for multiple locations.

    Args:
        soap_client: Configured SOAP client for CallCenterWs
        location_codes: List of location codes to fetch

    Returns:
        List of transformed unit records
    """
    all_data = []

    with tqdm(total=len(location_codes), desc="  Fetching locations", unit="loc") as pbar:
        for location_code in location_codes:
            try:
                # Call UnitsInformation_v3 endpoint
                results = soap_client.call(
                    operation="UnitsInformation_v3",
                    parameters={
                        "sLocationCode": location_code.strip(),
                        "lngLastTimePolled": "0",  # Get all units
                        "bReturnExcludedFromWebsiteUnits": "true",
                    },
                    soap_action=SOAP_ACTION,
                    namespace=NAMESPACE,
                    result_tag="Table"  # Response has Table elements containing unit data
                )

                # Transform each record
                for record in results:
                    transformed = transform_record(record)
                    all_data.append(transformed)

                pbar.set_postfix({"location": location_code, "units": len(results)})
                pbar.update(1)

            except Exception as e:
                pbar.set_postfix({"location": location_code, "status": "ERROR"})
                pbar.update(1)
                tqdm.write(f"  ✗ {location_code}: Error - {str(e)}")
                continue

    # Deduplicate by primary key (SiteID, UnitID)
    original_count = len(all_data)
    all_data = deduplicate_records(all_data, ['SiteID', 'UnitID'])
    if len(all_data) < original_count:
        tqdm.write(f"  ℹ Deduplicated: {original_count} → {len(all_data)} records")

    return all_data


def push_to_database(
    data: List[Dict[str, Any]],
    config: DataLayerConfig,
    chunk_size: int = 500
) -> None:
    """
    Push units info data to PostgreSQL database.

    Args:
        data: List of transformed records
        config: DataLayerConfig with database settings
        chunk_size: Batch size for upsert operations
    """
    if not data:
        print("  ⚠ No data to push")
        return

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    # Create table if not exists
    with tqdm(total=1, desc="  Preparing database", bar_format='{desc}') as pbar:
        Base.metadata.create_all(engine, tables=[UnitsInfo.__table__])
        pbar.update(1)
    tqdm.write("  ✓ Table 'units_info' ready")

    session_manager = SessionManager(engine)
    num_chunks = (len(data) + chunk_size - 1) // chunk_size

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        with tqdm(total=len(data), desc="  Upserting records", unit="rec") as pbar:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]

                upsert_ops.upsert_batch(
                    model=UnitsInfo,
                    records=chunk,
                    constraint_columns=['SiteID', 'UnitID'],
                    chunk_size=chunk_size
                )

                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i//chunk_size + 1}/{num_chunks}"})

    tqdm.write(f"  ✓ Upserted {len(data)} records to PostgreSQL")


# =============================================================================
# Main
# =============================================================================

def main():
    """Main function to fetch and push Units Information to SQL."""

    # Load configuration
    config = DataLayerConfig.from_env()

    if not config.soap:
        raise ValueError("SOAP configuration not found. Check apis.yaml and vault secrets.")

    # Load location codes from unified config (uses shared location_codes)
    location_codes = get_pipeline_config('unitsinfo', 'location_codes', [])
    if not location_codes:
        raise ValueError("UNITSINFO location_codes not configured in scheduler.yaml")

    # Initialize SOAP client for CallCenterWs (different from ReportingWs)
    soap_client = SOAPClient(
        base_url=CALL_CENTER_WS_URL,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=120,  # Longer timeout for large unit lists
        retries=3
    )

    # Print header
    print("=" * 70)
    print("UnitsInformation to SQL Pipeline")
    print("=" * 70)
    print(f"Endpoint: CallCenterWs/UnitsInformation_v3")
    print(f"Locations: {len(location_codes)} ({', '.join(location_codes[:5])}...)")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    print("=" * 70)

    # Fetch data for all locations
    print("\n[Fetching Units Information]")
    all_data = fetch_units_info(
        soap_client=soap_client,
        location_codes=location_codes
    )

    # Push to database
    if all_data:
        print(f"\n[Pushing to Database]")
        push_to_database(all_data, config)

        # Print summary
        print("\n[Summary]")
        print("-" * 70)

        # Count by raw sTypeName
        type_counts = {}
        for record in all_data:
            key = record.get('sTypeName') or 'Unknown'
            type_counts[key] = type_counts.get(key, 0) + 1

        print("Unit Type Distribution (raw sTypeName):")
        for type_name, count in sorted(type_counts.items(), key=lambda x: -x[1])[:15]:
            print(f"  {type_name}: {count}")
        if len(type_counts) > 15:
            print(f"  ... and {len(type_counts) - 15} more types")
    else:
        print("\n⚠ No data found for any location")

    # Close SOAP client
    soap_client.close()

    print("\n" + "=" * 70)
    print(f"Pipeline completed! Total units: {len(all_data)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
