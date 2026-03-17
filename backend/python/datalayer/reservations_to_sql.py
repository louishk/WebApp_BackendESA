"""
Reservations to SQL Pipeline

Syncs active reservations from SiteLink ReservationList_v3 SOAP API
into the api_reservations table. Preserves attribution data (source,
gclid, gid, botid) for rows created by the /track API endpoints.

Usage:
    python reservations_to_sql.py

Configuration (in scheduler.yaml):
    pipelines.reservations_sync.location_codes: List of location codes
    pipelines.reservations_sync.sql_chunk_size: Batch size for upsert (default: 500)
"""

import sys
from datetime import datetime, timezone
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
    convert_to_int,
    convert_to_decimal,
    deduplicate_records,
)
from common.config import get_pipeline_config
from sqlalchemy import text


# =============================================================================
# CallCenterWs SOAP Configuration
# =============================================================================
CALL_CENTER_WS_URL = "https://api.smdservers.net/CCWs_3.5/CallCenterWs.asmx"
NAMESPACE = "http://tempuri.org/CallCenterWs/CallCenterWs"
SOAP_ACTION = "http://tempuri.org/CallCenterWs/CallCenterWs/ReservationList_v3"


# =============================================================================
# Status Mapping
# =============================================================================
# iWaitingStatus from SOAP → (status, lifecycle_date_column)
STATUS_MAP = {
    '0': 'created',
    '1': 'moved_in',
    '2': 'cancelled',
}


def parse_soap_date(value):
    """Parse SOAP date string to date or None."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00')).date()
    except (ValueError, TypeError):
        return None


def parse_soap_datetime(value):
    """Parse SOAP datetime string to datetime or None."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


# =============================================================================
# Record Transformation
# =============================================================================

def transform_record(record: Dict[str, Any], location_code: str = '') -> Dict[str, Any]:
    """
    Transform ReservationList_v3 record to api_reservations format.

    Args:
        record: Raw record from ReservationList_v3 SOAP API
        location_code: The site code used in the SOAP call (response may not include it)

    Returns:
        Transformed record ready for database upsert
    """
    waiting_id = convert_to_int(record.get('WaitingID'))
    if not waiting_id:
        return None

    site_code = record.get('sLocationCode') or location_code or ''
    if not site_code:
        return None

    status_code = str(record.get('iWaitingStatus', '0'))
    status = STATUS_MAP.get(status_code, 'created')

    # Parse dates
    needed_date = parse_soap_date(record.get('dNeeded'))
    expires_date = parse_soap_date(record.get('dExpires'))
    followup_date = parse_soap_date(record.get('dFollowup'))
    created_at = parse_soap_datetime(record.get('dCreated')) or parse_soap_datetime(record.get('dPlaced'))
    soap_updated_at = parse_soap_datetime(record.get('dUpdated'))

    # Fee paid detection
    paid_reserve_fee = convert_to_decimal(record.get('dcPaidReserveFee')) or 0
    reserve_fee_receipt_id = convert_to_int(record.get('iReserveFeeReceiptID')) or 0

    # Build lifecycle dates based on status
    reserved_at = created_at
    moved_in_at = None
    cancelled_at = None

    if status == 'moved_in':
        moved_in_at = parse_soap_datetime(record.get('dConverted_ToMoveIn')) or created_at
    elif status == 'cancelled':
        cancelled_at = soap_updated_at or created_at

    # Truncate comment to fit varchar(500)
    comment = (record.get('sComment') or '')[:500] or None

    return {
        'waiting_id': waiting_id,
        'global_waiting_num': convert_to_int(record.get('iGlobalWaitingNum')),
        'tenant_id': convert_to_int(record.get('TenantID')),
        'site_code': site_code.strip(),
        'unit_id': convert_to_int(record.get('UnitID')) or 0,
        'first_name': (record.get('sFName') or '')[:100],
        'last_name': (record.get('sLName') or '')[:100],
        'email': (record.get('sEmail') or '')[:100] or None,
        'phone': (record.get('sPhone') or '')[:20] or None,
        'mobile': (record.get('sMobile') or '')[:20] or None,
        'quoted_rate': convert_to_decimal(record.get('dcRate_Quoted')) or 0,
        'concession_id': convert_to_int(record.get('ConcessionID')) or 0,
        'needed_date': needed_date,
        'expires_date': expires_date,
        'followup_date': followup_date,
        'inquiry_type': convert_to_int(record.get('iInquiryType')) or 0,
        'rental_type_id': convert_to_int(record.get('QTRentalTypeID')) or 0,
        'paid_reserve_fee': paid_reserve_fee,
        'reserve_fee_receipt_id': reserve_fee_receipt_id,
        'soap_updated_at': soap_updated_at,
        'comment': comment,
        'status': status,
        'reserved_at': reserved_at,
        'moved_in_at': moved_in_at,
        'cancelled_at': cancelled_at,
        # Default source for SOAP-synced rows (preserved on conflict)
        'source': 'sitelink',
        'source_name': record.get('sSource') or 'SiteLink',
    }


# =============================================================================
# Data Operations
# =============================================================================

def fetch_reservations(
    soap_client: SOAPClient,
    location_codes: List[str]
) -> List[Dict[str, Any]]:
    """Fetch reservations for all locations via ReservationList_v3."""
    all_data = []

    with tqdm(total=len(location_codes), desc="  Fetching locations", unit="loc") as pbar:
        for location_code in location_codes:
            try:
                results = soap_client.call(
                    operation="ReservationList_v3",
                    parameters={
                        "sLocationCode": location_code.strip(),
                        "iGlobalWaitingNum": "0",
                        "WaitingID": "0",
                    },
                    soap_action=SOAP_ACTION,
                    namespace=NAMESPACE,
                    result_tag="Table",
                )

                count = 0
                for record in (results or []):
                    transformed = transform_record(record, location_code=location_code.strip())
                    if transformed:
                        all_data.append(transformed)
                        count += 1

                pbar.set_postfix({"location": location_code, "reservations": count})
                pbar.update(1)

            except Exception as e:
                pbar.set_postfix({"location": location_code, "status": "ERROR"})
                pbar.update(1)
                tqdm.write(f"  ✗ {location_code}: Error - {str(e)}")
                continue

    # Deduplicate by (site_code, waiting_id)
    original_count = len(all_data)
    all_data = deduplicate_records(all_data, ['site_code', 'waiting_id'])
    if len(all_data) < original_count:
        tqdm.write(f"  ℹ Deduplicated: {original_count} → {len(all_data)} records")

    return all_data


def push_to_database(
    data: List[Dict[str, Any]],
    config: DataLayerConfig,
    chunk_size: int = 500
) -> None:
    """Upsert reservations into api_reservations with attribution preservation."""
    if not data:
        print("  ⚠ No data to push")
        return

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    engine = create_engine_from_config(db_config)
    session_manager = SessionManager(engine)
    num_chunks = (len(data) + chunk_size - 1) // chunk_size

    upsert_sql = text("""
        INSERT INTO api_reservations (
            site_code, unit_id, waiting_id, global_waiting_num, tenant_id,
            first_name, last_name, email, phone, mobile,
            quoted_rate, concession_id, needed_date, expires_date, followup_date,
            inquiry_type, rental_type_id, paid_reserve_fee, reserve_fee_receipt_id,
            comment, source, source_name, status,
            reserved_at, moved_in_at, cancelled_at, soap_updated_at,
            soap_synced_at
        ) VALUES (
            :site_code, :unit_id, :waiting_id, :global_waiting_num, :tenant_id,
            :first_name, :last_name, :email, :phone, :mobile,
            :quoted_rate, :concession_id, :needed_date, :expires_date, :followup_date,
            :inquiry_type, :rental_type_id, :paid_reserve_fee, :reserve_fee_receipt_id,
            :comment, :source, :source_name, :status,
            :reserved_at, :moved_in_at, :cancelled_at, :soap_updated_at,
            NOW()
        )
        ON CONFLICT (site_code, waiting_id) WHERE waiting_id IS NOT NULL
        DO UPDATE SET
            unit_id       = EXCLUDED.unit_id,
            tenant_id     = EXCLUDED.tenant_id,
            first_name    = EXCLUDED.first_name,
            last_name     = EXCLUDED.last_name,
            email         = EXCLUDED.email,
            phone         = EXCLUDED.phone,
            mobile        = EXCLUDED.mobile,
            quoted_rate   = EXCLUDED.quoted_rate,
            concession_id = EXCLUDED.concession_id,
            needed_date   = EXCLUDED.needed_date,
            expires_date  = EXCLUDED.expires_date,
            followup_date = EXCLUDED.followup_date,
            inquiry_type  = EXCLUDED.inquiry_type,
            rental_type_id = EXCLUDED.rental_type_id,
            paid_reserve_fee = EXCLUDED.paid_reserve_fee,
            reserve_fee_receipt_id = EXCLUDED.reserve_fee_receipt_id,
            comment       = EXCLUDED.comment,
            status        = EXCLUDED.status,
            soap_updated_at = EXCLUDED.soap_updated_at,
            -- Preserve attribution: never overwrite if already set
            source        = COALESCE(NULLIF(api_reservations.source, 'sitelink'), EXCLUDED.source),
            source_name   = COALESCE(NULLIF(api_reservations.source_name, 'SiteLink'), EXCLUDED.source_name),
            -- Lifecycle dates: only set if not already set
            reserved_at   = COALESCE(api_reservations.reserved_at, EXCLUDED.reserved_at),
            moved_in_at   = COALESCE(api_reservations.moved_in_at, EXCLUDED.moved_in_at),
            cancelled_at  = COALESCE(api_reservations.cancelled_at, EXCLUDED.cancelled_at),
            soap_synced_at = NOW(),
            updated_at    = NOW()
    """)

    with session_manager.session_scope() as session:
        with tqdm(total=len(data), desc="  Upserting records", unit="rec") as pbar:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                for record in chunk:
                    session.execute(upsert_sql, record)
                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i // chunk_size + 1}/{num_chunks}"})

    tqdm.write(f"  ✓ Upserted {len(data)} records to PostgreSQL")


# =============================================================================
# Main
# =============================================================================

def main():
    """Main function to sync SiteLink reservations to SQL."""

    config = DataLayerConfig.from_env()

    if not config.soap:
        raise ValueError("SOAP configuration not found. Check apis.yaml and vault secrets.")

    location_codes = get_pipeline_config('reservations_sync', 'location_codes', [])
    if not location_codes:
        raise ValueError("reservations_sync location_codes not configured in scheduler.yaml")

    chunk_size = get_pipeline_config('reservations_sync', 'sql_chunk_size', 500)

    soap_client = SOAPClient(
        base_url=CALL_CENTER_WS_URL,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=60,
        retries=3,
    )

    print("=" * 70)
    print("Reservations Sync Pipeline")
    print("=" * 70)
    print(f"Endpoint: CallCenterWs/ReservationList_v3")
    print(f"Locations: {len(location_codes)} ({', '.join(location_codes[:5])}...)")
    print(f"Target: PostgreSQL - api_reservations")
    print("=" * 70)
    print("[STAGE:INIT] ReservationsSync")

    print("[STAGE:FETCH] Retrieving reservations from SOAP API")
    print("\n[Fetching Reservations]")
    all_data = fetch_reservations(
        soap_client=soap_client,
        location_codes=location_codes,
    )

    if all_data:
        print("[STAGE:PUSH] Upserting reservations to PostgreSQL")
        print("\n[Pushing to Database]")
        push_to_database(all_data, config, chunk_size=chunk_size)

        # Summary
        print("\n[Summary]")
        print("-" * 70)
        status_counts = {}
        site_counts = {}
        for record in all_data:
            s = record.get('status', 'unknown')
            status_counts[s] = status_counts.get(s, 0) + 1
            sc = record.get('site_code', '?')
            site_counts[sc] = site_counts.get(sc, 0) + 1

        print("By status:")
        for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
            print(f"  {status}: {count}")

        print("By site (top 10):")
        for site, count in sorted(site_counts.items(), key=lambda x: -x[1])[:10]:
            print(f"  {site}: {count}")
        if len(site_counts) > 10:
            print(f"  ... and {len(site_counts) - 10} more sites")
    else:
        print("\n⚠ No reservations found for any location")

    soap_client.close()

    print(f"[STAGE:COMPLETE] {len(all_data)} records")
    print("\n" + "=" * 70)
    print(f"Pipeline completed! Total reservations: {len(all_data)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
