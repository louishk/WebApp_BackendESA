"""
CC Discount Plans to SQL Pipeline

Fetches discount/concession plan data from DiscountPlansRetrieve SOAP API
(CallCenterWs) and pushes to PostgreSQL.

Features:
- Fetches all discount plans for configured locations
- Uses upsert on composite key (SiteID + ConcessionID)
- Processes in chunks for large datasets

Usage:
    python cc_discount_plans_to_sql.py

Configuration (in pipelines.yaml):
    pipelines.cc_discount_plans.location_codes: List of location codes
    pipelines.cc_discount_plans.sql_chunk_size: Batch size for upsert (default: 500)
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
    CCDiscount,
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
SOAP_ACTION = "http://tempuri.org/CallCenterWs/CallCenterWs/DiscountPlansRetrieve"


# =============================================================================
# Record Transformation
# =============================================================================

def transform_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform API record to database-ready format.

    Args:
        record: Raw record from DiscountPlansRetrieve API

    Returns:
        Transformed record ready for database insertion
    """
    return {
        # Core identifiers
        'ConcessionID': convert_to_int(record.get('ConcessionID')),
        'SiteID': convert_to_int(record.get('SiteID')),
        'iConcessionGlobalNum': convert_to_int(record.get('iConcessionGlobalNum')),
        'QTTouchDiscPlanID': record.get('QTTouchDiscPlanID'),
        'PlanName_TermID': record.get('PlanName_TermID'),
        'OldPK': record.get('OldPK'),

        # Plan info
        'sDefPlanName': record.get('sDefPlanName'),
        'sPlanName': record.get('sPlanName'),
        'sDescription': record.get('sDescription'),
        'sComment': record.get('sComment'),
        'sCouponCode': record.get('sCouponCode'),

        # Plan dates
        'dPlanStrt': convert_to_datetime(record.get('dPlanStrt')),
        'dPlanEnd': convert_to_datetime(record.get('dPlanEnd')),
        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'dArchived': convert_to_datetime(record.get('dArchived')),
        'dDisabled': convert_to_datetime(record.get('dDisabled')),
        'dDeleted': convert_to_datetime(record.get('dDeleted')),

        # Plan configuration
        'iShowOn': convert_to_int(record.get('iShowOn')),
        'bNeverExpires': convert_to_bool(record.get('bNeverExpires')),
        'iExpirMonths': convert_to_int(record.get('iExpirMonths')),
        'bPrepay': convert_to_bool(record.get('bPrepay')),
        'bOnPmt': convert_to_bool(record.get('bOnPmt')),
        'bManualCredit': convert_to_bool(record.get('bManualCredit')),
        'iPrePaidMonths': convert_to_int(record.get('iPrePaidMonths')),
        'iInMonth': convert_to_int(record.get('iInMonth')),
        'bPermanent': convert_to_bool(record.get('bPermanent')),

        # Discount amounts
        'iAmtType': convert_to_int(record.get('iAmtType')),
        'dcChgAmt': convert_to_decimal(record.get('dcChgAmt')),
        'dcFixedDiscount': convert_to_decimal(record.get('dcFixedDiscount')),
        'dcPCDiscount': convert_to_decimal(record.get('dcPCDiscount')),
        'bRound': convert_to_bool(record.get('bRound')),
        'dcRoundTo': convert_to_decimal(record.get('dcRoundTo')),
        'dcMaxAmountOff': convert_to_decimal(record.get('dcMaxAmountOff')),

        # Charge reference
        'ChargeDescID': convert_to_int(record.get('ChargeDescID')),
        'iQty': convert_to_int(record.get('iQty')),
        'iOfferItemAction': convert_to_int(record.get('iOfferItemAction')),

        # Corporate / occupancy rules
        'bForCorp': convert_to_bool(record.get('bForCorp')),
        'dcMaxOccPct': convert_to_decimal(record.get('dcMaxOccPct')),
        'bForAllUnits': convert_to_bool(record.get('bForAllUnits')),
        'iExcludeIfLessThanUnitsTotal': convert_to_int(record.get('iExcludeIfLessThanUnitsTotal')),
        'dcMaxOccPctExcludeIfMoreThanUnitsTotal': convert_to_decimal(record.get('dcMaxOccPctExcludeIfMoreThanUnitsTotal')),
        'iExcludeIfMoreThanUnitsTotal': convert_to_int(record.get('iExcludeIfMoreThanUnitsTotal')),
        'iAvailableAt': convert_to_int(record.get('iAvailableAt')),
        'bEligibleToRemoveIfPastDue': convert_to_bool(record.get('bEligibleToRemoveIfPastDue')),
        'iRestrictionFlags': convert_to_int(record.get('iRestrictionFlags')),
        'iOccupancyPctUnitCountMethod': convert_to_int(record.get('iOccupancyPctUnitCountMethod')),

        # ChargeDesc joined fields (suffix "1" from API join)
        'ChargeDescID1': convert_to_int(record.get('ChargeDescID1')),
        'SiteID1': convert_to_int(record.get('SiteID1')),
        'ChartOfAcctID': convert_to_int(record.get('ChartOfAcctID')),
        'ChgDesc_TermID': convert_to_int(record.get('ChgDesc_TermID')),
        'sDefChgDesc': record.get('sDefChgDesc'),
        'sChgDesc': record.get('sChgDesc'),
        'sVendor': record.get('sVendor'),
        'sVendorPhone': record.get('sVendorPhone'),
        'sReorderPartNum': record.get('sReorderPartNum'),
        'sChgCategory': record.get('sChgCategory'),
        'bApplyAtMoveIn': convert_to_bool(record.get('bApplyAtMoveIn')),
        'bProrateAtMoveIn': convert_to_bool(record.get('bProrateAtMoveIn')),
        'bPermanent1': convert_to_bool(record.get('bPermanent1')),
        'dcPrice': convert_to_decimal(record.get('dcPrice')),
        'dcTax1Rate': convert_to_decimal(record.get('dcTax1Rate')),
        'dcTax2Rate': convert_to_decimal(record.get('dcTax2Rate')),
        'dcCost': convert_to_decimal(record.get('dcCost')),
        'dcInStock': convert_to_decimal(record.get('dcInStock')),
        'dcOrderPt': convert_to_decimal(record.get('dcOrderPt')),
        'dChgStrt': convert_to_datetime(record.get('dChgStrt')),
        'dChgDisabled': convert_to_datetime(record.get('dChgDisabled')),
        'bUseMileageRate': convert_to_bool(record.get('bUseMileageRate')),
        'dcMileageRate': convert_to_decimal(record.get('dcMileageRate')),
        'iIncludedMiles': convert_to_int(record.get('iIncludedMiles')),
        'dDisabled1': convert_to_datetime(record.get('dDisabled1')),
        'dDeleted1': convert_to_datetime(record.get('dDeleted1')),
        'dUpdated1': convert_to_datetime(record.get('dUpdated1')),
        'OldPK1': convert_to_int(record.get('OldPK1')),
        'sCorpCategory': record.get('sCorpCategory'),
        'sBarCode': record.get('sBarCode'),
        'iPriceType': convert_to_int(record.get('iPriceType')),
        'dcPCRate': convert_to_decimal(record.get('dcPCRate')),
        'dcMinPriceIfPC': convert_to_decimal(record.get('dcMinPriceIfPC')),
        'bRound1': convert_to_bool(record.get('bRound1')),
        'dcRoundTo1': convert_to_decimal(record.get('dcRoundTo1')),
    }


# =============================================================================
# Data Operations
# =============================================================================

def fetch_discount_plans(
    soap_client: SOAPClient,
    location_codes: List[str]
) -> List[Dict[str, Any]]:
    """
    Fetch discount plans for multiple locations.

    Args:
        soap_client: Configured SOAP client for CallCenterWs
        location_codes: List of location codes to fetch

    Returns:
        List of transformed discount plan records
    """
    all_data = []

    with tqdm(total=len(location_codes), desc="  Fetching locations", unit="loc") as pbar:
        for location_code in location_codes:
            try:
                results = soap_client.call(
                    operation="DiscountPlansRetrieve",
                    parameters={
                        "sLocationCode": location_code.strip(),
                    },
                    soap_action=SOAP_ACTION,
                    namespace=NAMESPACE,
                    result_tag="ConcessionPlans"
                )

                for record in results:
                    transformed = transform_record(record)
                    all_data.append(transformed)

                pbar.set_postfix({"location": location_code, "plans": len(results)})
                pbar.update(1)

            except Exception as e:
                pbar.set_postfix({"location": location_code, "status": "ERROR"})
                pbar.update(1)
                tqdm.write(f"  ✗ {location_code}: Error - {str(e)}")
                continue

    # Deduplicate by natural key (SiteID, ConcessionID)
    original_count = len(all_data)
    all_data = deduplicate_records(all_data, ['SiteID', 'ConcessionID'])
    if len(all_data) < original_count:
        tqdm.write(f"  ℹ Deduplicated: {original_count} → {len(all_data)} records")

    return all_data


def push_to_database(
    data: List[Dict[str, Any]],
    config: DataLayerConfig,
    chunk_size: int = 500
) -> None:
    """
    Push discount plan data to PostgreSQL database.

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
        Base.metadata.create_all(engine, tables=[CCDiscount.__table__])
        pbar.update(1)
    tqdm.write("  ✓ Table 'cc_discount' ready")

    session_manager = SessionManager(engine)
    num_chunks = (len(data) + chunk_size - 1) // chunk_size

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        with tqdm(total=len(data), desc="  Upserting records", unit="rec") as pbar:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]

                upsert_ops.upsert_batch(
                    model=CCDiscount,
                    records=chunk,
                    constraint_columns=['SiteID', 'ConcessionID'],
                    chunk_size=chunk_size
                )

                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i//chunk_size + 1}/{num_chunks}"})

    tqdm.write(f"  ✓ Upserted {len(data)} records to PostgreSQL")


# =============================================================================
# Main
# =============================================================================

def main():
    """Main function to fetch and push CC Discount Plans to SQL."""

    # Load configuration
    config = DataLayerConfig.from_env()

    if not config.soap:
        raise ValueError("SOAP configuration not found. Check apis.yaml and vault secrets.")

    # Load location codes from unified config
    location_codes = get_pipeline_config('cc_discount_plans', 'location_codes', [])
    if not location_codes:
        raise ValueError("cc_discount_plans location_codes not configured in scheduler.yaml")

    # Initialize SOAP client for CallCenterWs
    soap_client = SOAPClient(
        base_url=CALL_CENTER_WS_URL,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=120,
        retries=3
    )

    # Print header
    print("=" * 70)
    print("CC Discount Plans to SQL Pipeline")
    print("=" * 70)
    print(f"Endpoint: CallCenterWs/DiscountPlansRetrieve")
    print(f"Locations: {len(location_codes)} ({', '.join(location_codes[:5])}...)")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    print("=" * 70)

    # Fetch data for all locations
    print("\n[Fetching Discount Plans]")
    all_data = fetch_discount_plans(
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
        print(f"Total discount plans: {len(all_data)}")

        # Count by site
        site_counts = {}
        for record in all_data:
            site_id = record.get('SiteID', 'Unknown')
            site_counts[site_id] = site_counts.get(site_id, 0) + 1

        print("Plans per site:")
        for site_id, count in sorted(site_counts.items(), key=lambda x: -x[1])[:15]:
            print(f"  SiteID {site_id}: {count}")
        if len(site_counts) > 15:
            print(f"  ... and {len(site_counts) - 15} more sites")
    else:
        print("\n⚠ No data found for any location")

    # Close SOAP client
    soap_client.close()

    print("\n" + "=" * 70)
    print(f"Pipeline completed! Total discount plans: {len(all_data)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
