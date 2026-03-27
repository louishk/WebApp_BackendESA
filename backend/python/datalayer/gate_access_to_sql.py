"""
GateAccessData to SQL Pipeline

Fetches gate access data from GateAccessData SOAP API (CallCenterWs)
and pushes to PostgreSQL (esa_backend) with encrypted access codes.

Features:
- Fetches all unit gate access records for configured locations
- Encrypts sAccessCode/sAccessCode2 with Fernet (VAULT_MASTER_KEY + dedicated salt)
- Uses upsert on composite key (location_code + unit_id)
- Processes in chunks for large datasets

Usage:
    python gate_access_to_sql.py

Configuration (in scheduler.yaml):
    pipelines.gateaccess.location_codes: shared location_codes
    pipelines.gateaccess.sql_chunk_size: Batch size for upsert (default: 500)
"""

import logging
import sys
from pathlib import Path
from typing import List, Dict, Any

from tqdm import tqdm

logger = logging.getLogger(__name__)

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import (
    DataLayerConfig,
    SOAPClient,
    SessionManager,
    UpsertOperations,
    convert_to_bool,
    convert_to_int,
    deduplicate_records,
)
from common.config import get_pipeline_config
from common.gate_access_crypto import get_gate_crypto
from common.config_loader import get_database_url
from sqlalchemy import create_engine, text

# Import model — this pipeline targets esa_backend, not esa_pbi
from web.models.smart_lock import GateAccessData

# =============================================================================
# SOAP Configuration
# =============================================================================
CALL_CENTER_WS_URL = "https://api.smdservers.net/CCWs_3.5/CallCenterWs.asmx"
NAMESPACE = "http://tempuri.org/CallCenterWs/CallCenterWs"
SOAP_ACTION = "http://tempuri.org/CallCenterWs/CallCenterWs/GateAccessData"


# =============================================================================
# Record Transformation
# =============================================================================

def transform_record(
    record: Dict[str, Any],
    location_code: str,
    site_id: int,
    crypto,
) -> Dict[str, Any]:
    """Transform SOAP record to DB-ready format with encrypted access codes."""
    access_code = record.get('sAccessCode') or ''
    access_code2 = record.get('sAccessCode2') or ''

    return {
        'location_code': location_code,
        'site_id': site_id,
        'unit_id': convert_to_int(record.get('UnitID')),
        'unit_name': record.get('sUnitName') or '',
        'is_rented': convert_to_bool(record.get('bRented')),
        'access_code_enc': crypto.encrypt(access_code) if access_code else None,
        'access_code2_enc': crypto.encrypt(access_code2) if access_code2 else None,
        'is_gate_locked': convert_to_bool(record.get('bGateLocked')),
        'is_overlocked': convert_to_bool(record.get('bOverlocked')),
        'keypad_zone': convert_to_int(record.get('iKeypadZ')) or 0,
    }


# =============================================================================
# Data Operations
# =============================================================================

def build_location_to_site_map() -> Dict[str, int]:
    """Build location_code -> numeric SiteID mapping from esa_pbi units_info."""
    pbi_engine = create_engine(get_database_url('pbi'))
    with pbi_engine.connect() as conn:
        rows = conn.execute(text(
            'SELECT DISTINCT "sLocationCode", "SiteID" FROM units_info '
            'WHERE "sLocationCode" IS NOT NULL'
        )).fetchall()
    pbi_engine.dispose()
    return {r[0]: r[1] for r in rows}


def fetch_gate_access(
    soap_client: SOAPClient,
    location_codes: List[str],
    crypto,
    loc_to_site: Dict[str, int],
) -> List[Dict[str, Any]]:
    """Fetch and transform gate access data for all locations."""
    all_data = []

    with tqdm(total=len(location_codes), desc="  Fetching locations", unit="loc") as pbar:
        for location_code in location_codes:
            site_id = loc_to_site.get(location_code)
            if site_id is None:
                tqdm.write(f"  ⚠ {location_code}: no SiteID mapping found, skipping")
                pbar.update(1)
                continue

            try:
                results = soap_client.call(
                    operation="GateAccessData",
                    parameters={
                        "sLocationCode": location_code.strip(),
                        "iMinutesSinceLastUpdate": "0",
                    },
                    soap_action=SOAP_ACTION,
                    namespace=NAMESPACE,
                    result_tag="Table",
                )

                for record in results:
                    transformed = transform_record(record, location_code, site_id, crypto)
                    if transformed['unit_id']:  # skip records with no UnitID
                        all_data.append(transformed)

                pbar.set_postfix({"location": location_code, "units": len(results)})
                pbar.update(1)

            except Exception as e:
                logger.debug("SOAP fetch failed for %s", location_code, exc_info=True)
                pbar.set_postfix({"location": location_code, "status": "ERROR"})
                pbar.update(1)
                tqdm.write(f"  ✗ {location_code}: SOAP fetch failed")
                continue

    original_count = len(all_data)
    all_data = deduplicate_records(all_data, ['location_code', 'unit_id'])
    if len(all_data) < original_count:
        tqdm.write(f"  ℹ Deduplicated: {original_count} → {len(all_data)} records")

    return all_data


def push_to_database(
    data: List[Dict[str, Any]],
    chunk_size: int = 500,
) -> None:
    """Push gate access data to esa_backend PostgreSQL."""
    if not data:
        print("  ⚠ No data to push")
        return

    db_url = get_database_url('backend')
    engine = create_engine(db_url)
    # Table created by migration 031_gate_access_data.sql — no create_all needed

    session_manager = SessionManager(engine)
    num_chunks = (len(data) + chunk_size - 1) // chunk_size

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, 'postgresql')

        with tqdm(total=len(data), desc="  Upserting records", unit="rec") as pbar:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]

                upsert_ops.upsert_batch(
                    model=GateAccessData,
                    records=chunk,
                    constraint_columns=['location_code', 'unit_id'],
                    chunk_size=chunk_size,
                )

                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i // chunk_size + 1}/{num_chunks}"})

    tqdm.write(f"  ✓ Upserted {len(data)} records to esa_backend")


# =============================================================================
# Main
# =============================================================================

def main():
    """Main pipeline function."""
    config = DataLayerConfig.from_env()

    if not config.soap:
        raise ValueError("SOAP configuration not found. Check apis.yaml and vault secrets.")

    location_codes = get_pipeline_config('gateaccess', 'location_codes', [])
    if not location_codes:
        location_codes = get_pipeline_config('unitsinfo', 'location_codes', [])
    if not location_codes:
        raise ValueError("No location_codes configured")

    chunk_size = get_pipeline_config('gateaccess', 'sql_chunk_size', 500)

    soap_client = SOAPClient(
        base_url=CALL_CENTER_WS_URL,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=120,
        retries=3,
    )

    crypto = get_gate_crypto()

    # Build location_code -> numeric SiteID mapping from esa_pbi
    loc_to_site = build_location_to_site_map()

    print("=" * 70)
    print("GateAccessData to SQL Pipeline")
    print("=" * 70)
    print(f"Endpoint: CallCenterWs/GateAccessData")
    print(f"Locations: {len(location_codes)} ({', '.join(location_codes[:5])}...)")
    print(f"Site mappings: {len(loc_to_site)} location codes resolved")
    print(f"Target: esa_backend (PostgreSQL)")
    print("=" * 70)
    print("[STAGE:INIT] GateAccessData")

    print("[STAGE:FETCH] Retrieving gate access from SOAP API")
    all_data = fetch_gate_access(soap_client, location_codes, crypto, loc_to_site)

    if all_data:
        print("[STAGE:PUSH] Upserting to PostgreSQL")
        push_to_database(all_data, chunk_size)

        # Summary
        print("\n[Summary]")
        print("-" * 70)
        locked = sum(1 for r in all_data if r.get('is_gate_locked'))
        overlocked = sum(1 for r in all_data if r.get('is_overlocked'))
        with_code = sum(1 for r in all_data if r.get('access_code_enc'))
        print(f"  Total records: {len(all_data)}")
        print(f"  Gate locked: {locked}")
        print(f"  Overlocked: {overlocked}")
        print(f"  With access code: {with_code}")
    else:
        print("\n⚠ No data found for any location")

    soap_client.close()

    print(f"[STAGE:COMPLETE] {len(all_data)} records")
    print("=" * 70)


if __name__ == "__main__":
    main()
