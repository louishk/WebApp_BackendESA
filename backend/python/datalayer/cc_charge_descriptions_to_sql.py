"""
CC Charge Descriptions to SQL Pipeline

Fetches charge type configuration (tax rates, default prices) from
ChargeDescriptionsRetrieve SOAP API and pushes to PostgreSQL.

This is the missing puzzle piece for the internal MoveInCost calculator:
- Per-charge-type tax rates (Rent=9%, Insurance=8%, POS=7%, etc.)
- Default prices for AdminFee and other site-configured fees

Usage:
    python cc_charge_descriptions_to_sql.py

Configuration (in pipelines.yaml):
    pipelines.ccws_charge_descriptions.location_codes: List of location codes
    pipelines.ccws_charge_descriptions.sql_chunk_size: Batch size (default: 500)
"""

import sys
from pathlib import Path
from typing import List, Dict, Any
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from common import (
    DataLayerConfig,
    SOAPClient,
    create_engine_from_config,
    SessionManager,
    UpsertOperations,
    Base,
    convert_to_bool,
    convert_to_int,
    convert_to_decimal,
    convert_to_datetime,
    deduplicate_records,
)
from common.models import CcwsChargeDescription
from common.config import get_pipeline_config


CALL_CENTER_WS_URL = "https://api.smdservers.net/CCWs_3.5/CallCenterWs.asmx"
NAMESPACE = "http://tempuri.org/CallCenterWs/CallCenterWs"
SOAP_ACTION = f"{NAMESPACE}/ChargeDescriptionsRetrieve"


def transform_record(record: Dict[str, Any], site_code: str) -> Dict[str, Any]:
    """Transform API record to DB-ready format."""
    return {
        'ChargeDescID': convert_to_int(record.get('ChargeDescID')),
        'SiteID': convert_to_int(record.get('SiteID')),
        'SiteCode': site_code,
        'sChgDesc': record.get('sChgDesc'),
        'sChgCategory': record.get('sChgCategory'),
        'dcPrice': convert_to_decimal(record.get('dcPrice')),
        'dcTax1Rate': convert_to_decimal(record.get('dcTax1Rate')),
        'dcTax2Rate': convert_to_decimal(record.get('dcTax2Rate')),
        'bApplyAtMoveIn': convert_to_bool(record.get('bApplyAtMoveIn')),
        'bProrateAtMoveIn': convert_to_bool(record.get('bProrateAtMoveIn')),
        'bPermanent': convert_to_bool(record.get('bPermanent')),
        'dDisabled': convert_to_datetime(record.get('dDisabled')),
    }


def fetch_charge_descriptions(
    soap_client: SOAPClient,
    location_codes: List[str]
) -> List[Dict[str, Any]]:
    """Fetch charge descriptions for multiple locations."""
    all_data = []

    with tqdm(total=len(location_codes), desc="  Fetching locations", unit="loc") as pbar:
        for location_code in location_codes:
            site_code = location_code.strip()
            try:
                results = soap_client.call(
                    operation="ChargeDescriptionsRetrieve",
                    parameters={"sLocationCode": site_code},
                    soap_action=SOAP_ACTION,
                    namespace=NAMESPACE,
                    result_tag="Table",
                )

                for record in results:
                    transformed = transform_record(record, site_code)
                    if transformed['ChargeDescID'] and transformed['SiteID']:
                        all_data.append(transformed)

                pbar.set_postfix({"location": site_code, "charges": len(results)})
                pbar.update(1)

            except Exception as e:
                pbar.set_postfix({"location": site_code, "status": "ERROR"})
                pbar.update(1)
                tqdm.write(f"  ✗ {site_code}: Error - {str(e)}")
                continue

    original_count = len(all_data)
    all_data = deduplicate_records(all_data, ['SiteID', 'ChargeDescID'])
    if len(all_data) < original_count:
        tqdm.write(f"  ℹ Deduplicated: {original_count} → {len(all_data)} records")

    return all_data


def push_to_database(
    data: List[Dict[str, Any]],
    config: DataLayerConfig,
    chunk_size: int = 500
) -> None:
    """Upsert charge description data to PostgreSQL."""
    if not data:
        print("  ⚠ No data to push")
        return

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    engine = create_engine_from_config(db_config)

    with tqdm(total=1, desc="  Preparing database", bar_format='{desc}') as pbar:
        Base.metadata.create_all(engine, tables=[CcwsChargeDescription.__table__])
        pbar.update(1)
    tqdm.write("  ✓ Table 'ccws_charge_descriptions' ready")

    session_manager = SessionManager(engine)
    num_chunks = (len(data) + chunk_size - 1) // chunk_size

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        with tqdm(total=len(data), desc="  Upserting records", unit="rec") as pbar:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                upsert_ops.upsert_batch(
                    model=CcwsChargeDescription,
                    records=chunk,
                    constraint_columns=['SiteID', 'ChargeDescID'],
                    chunk_size=chunk_size,
                )
                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i//chunk_size + 1}/{num_chunks}"})

    tqdm.write(f"  ✓ Upserted {len(data)} records to PostgreSQL")


def main():
    config = DataLayerConfig.from_env()
    if not config.soap:
        raise ValueError("SOAP configuration not found")

    location_codes = get_pipeline_config(
        'ccws_charge_descriptions', 'location_codes', [])
    if not location_codes:
        # Fall back to discount plans location list (same set of sites)
        location_codes = get_pipeline_config(
            'ccws_discount_plans', 'location_codes', [])
    if not location_codes:
        raise ValueError(
            "ccws_charge_descriptions location_codes not configured")

    chunk_size = get_pipeline_config(
        'ccws_charge_descriptions', 'sql_chunk_size', 500)

    soap_client = SOAPClient(
        base_url=CALL_CENTER_WS_URL,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=120,
        retries=3,
    )

    print("=" * 70)
    print("CC Charge Descriptions to SQL Pipeline")
    print("=" * 70)
    print(f"Endpoint: CallCenterWs/ChargeDescriptionsRetrieve")
    print(f"Locations: {len(location_codes)} ({', '.join(location_codes[:5])}...)")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    print("=" * 70)
    print("[STAGE:INIT] CcwsChargeDescriptions")

    print("[STAGE:FETCH] Fetching charge descriptions from SOAP API")
    all_data = fetch_charge_descriptions(soap_client, location_codes)

    if all_data:
        print("[STAGE:PUSH] Upserting to PostgreSQL")
        push_to_database(all_data, config, chunk_size)

        site_counts = {}
        for record in all_data:
            site_counts[record['SiteCode']] = site_counts.get(
                record['SiteCode'], 0) + 1
        print("\nCharges per site:")
        for site, count in sorted(site_counts.items(), key=lambda x: -x[1])[:15]:
            print(f"  {site}: {count}")
    else:
        print("\n⚠ No data found for any location")

    soap_client.close()
    print(f"[STAGE:COMPLETE] {len(all_data)} records")
    print("=" * 70)


if __name__ == "__main__":
    main()
