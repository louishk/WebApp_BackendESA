"""
Igloo Smart Lock to SQL Pipeline

Fetches property and device data from Igloo API and pushes to PostgreSQL.
Syncs lock battery levels, sync times, and device metadata for the smart lock tool.

Features:
- Two modes: backfill (all data), auto (incremental daily)
- OAuth2 client_credentials authentication with expiry-aware token refresh
- Cursor-based pagination for all list endpoints (with page ceiling)
- Upsert by deviceId (devices) and propertyId (properties)
- Device join: igloo_devices.deviceName = padlock_id / keypad_id (no mapping table)
- Field validation: truncation to column widths, battery range clamping

Usage:
    # Backfill mode - fetch all properties and devices
    python -m datalayer.igloo_to_sql --mode backfill

    # Auto mode - incremental sync (same as backfill for now, API has no delta endpoint)
    python -m datalayer.igloo_to_sql --mode auto

Configuration (in pipelines.yaml):
    pipelines.igloo.property_site_map: Dict mapping Igloo property name to ESA SiteID
"""

import argparse
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

from tqdm import tqdm

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from sqlalchemy.orm import sessionmaker

from common import (
    SessionManager,
    UpsertOperations,
    HTTPClient,
    IglooProperty,
    IglooDevice,
)
from common.config import DatabaseType
from common.config_loader import get_database_url
from common.secrets_vault import vault_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

API_BASE_URL = 'https://api.iglooworks.co/v2'
AUTH_URL = 'https://auth.iglooworks.co/oauth2/token'
DEFAULT_PAGE_LIMIT = 300
MAX_PAGES = 500  # Safety ceiling: ~150,000 records at limit=300

# deviceId format: alphanumeric + hyphens/underscores, max 30 chars
_DEVICE_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,30}$')


# =============================================================================
# Field Validation Helpers
# =============================================================================

def _truncate(val: Any, max_len: int) -> Optional[str]:
    """Truncate a value to max_len characters, returning None for empty/None."""
    if val is None:
        return None
    s = str(val)[:max_len]
    return s if s else None


def _clamp_int(val: Any, min_val: int = 0, max_val: int = 100) -> Optional[int]:
    """Parse and clamp an integer value, returning None on failure."""
    if val is None:
        return None
    try:
        v = int(val)
    except (TypeError, ValueError):
        return None
    return max(min_val, min(max_val, v))


def _safe_int(val: Any) -> Optional[int]:
    """Parse an integer value without clamping, returning None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


# =============================================================================
# Igloo API Authentication
# =============================================================================

class IglooAuth:
    """OAuth2 client_credentials token manager for Igloo API with expiry handling."""

    def __init__(self, client_id: str, client_secret: str, http_client: HTTPClient):
        self.client_id = client_id
        self.client_secret = client_secret
        self.http_client = http_client
        self._token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

    def get_token(self) -> str:
        """Get Bearer token, refreshing if expired or about to expire."""
        now = datetime.now(timezone.utc)
        if self._token and self._expires_at and now < self._expires_at - timedelta(seconds=30):
            return self._token

        logger.info("Authenticating with Igloo API (client_credentials)...")
        response = self.http_client.post(
            AUTH_URL,
            data={
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
            },
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
        )
        response.raise_for_status()

        token_data = response.json()
        self._token = token_data['access_token']
        ttl = int(token_data.get('expires_in', 3600))
        self._expires_at = now + timedelta(seconds=ttl)
        logger.info("Igloo API authenticated (token expires in %ds)", ttl)
        return self._token

    def auth_headers(self) -> Dict[str, str]:
        """Return headers with Bearer token."""
        return {
            'Authorization': f'Bearer {self.get_token()}',
            'Content-Type': 'application/json',
        }


# =============================================================================
# Igloo API Client
# =============================================================================

def fetch_paginated(
    http_client: HTTPClient,
    auth: IglooAuth,
    endpoint: str,
    limit: int = DEFAULT_PAGE_LIMIT,
    params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch all records from a paginated Igloo API endpoint.
    Handles cursor-based pagination (nextCursor) with a page ceiling.
    """
    all_items = []
    cursor = None
    page = 0

    while True:
        page += 1
        if page > MAX_PAGES:
            logger.error("fetch_paginated: hit page ceiling (%d) for %s — aborting", MAX_PAGES, endpoint)
            break

        req_params = {'limit': limit}
        if params:
            req_params.update(params)
        if cursor:
            req_params['cursor'] = cursor

        response = http_client.get(
            f"{API_BASE_URL}/{endpoint}",
            headers=auth.auth_headers(),
            params=req_params,
        )
        response.raise_for_status()
        data = response.json()

        # Igloo API returns {payload: [...], nextCursor: "..."}
        items = data.get('payload', [])
        if not items:
            break

        all_items.extend(items)
        logger.info("  %s page %d: %d items (total: %d)", endpoint, page, len(items), len(all_items))

        # nextCursor is null/absent for most endpoints, empty string "" for /properties
        cursor = data.get('nextCursor')
        if not cursor:  # handles None, absent, and ""
            break

    return all_items


def fetch_device_detail(
    http_client: HTTPClient,
    auth: IglooAuth,
    device_id: str,
) -> Optional[Dict[str, Any]]:
    """Fetch expanded detail for a single device."""
    if not _DEVICE_ID_RE.match(device_id):
        logger.warning("Skipping device with invalid deviceId format: %r", device_id[:30])
        return None

    try:
        response = http_client.get(
            f"{API_BASE_URL}/devices/{device_id}",
            headers=auth.auth_headers(),
        )
        response.raise_for_status()
        data = response.json()
        return data.get('payload', data)
    except Exception:
        logger.warning("Failed to fetch device detail for device_id=%s", device_id)
        return None


# =============================================================================
# Data Transformation
# =============================================================================

def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO datetime string, returning None on failure."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def transform_property(
    item: Dict[str, Any],
    property_site_map: Dict[str, int],
) -> Dict[str, Any]:
    """Transform an Igloo property API response into a DB record dict."""
    name = _truncate(item.get('name', ''), 100) or ''
    return {
        'propertyId': _truncate(item.get('id', ''), 50) or '',
        'name': name,
        'timezone': _truncate(item.get('timezone'), 50),
        'totalLock': _safe_int(item.get('totalLock')),
        'site_id': property_site_map.get(name),
        'raw_json': item,
    }


def transform_device(
    item: Dict[str, Any],
    departments: Dict[str, Dict[str, Any]],
    property_site_map: Dict[str, int],
) -> Dict[str, Any]:
    """Transform an Igloo device API response into a DB record dict."""
    # Resolve property info from embedded properties list
    props = item.get('properties') or []
    property_id = _truncate(props[0].get('id'), 50) if props else None
    property_name = _truncate(props[0].get('name'), 100) if props else None

    # Resolve department info
    dept_id = None
    dept_name = None
    if item.get('departmentId'):
        dept_id = _truncate(item['departmentId'], 50)
        dept_info = departments.get(dept_id, {})
        dept_name = _truncate(dept_info.get('name'), 100)
    elif property_id:
        for d_id, d_info in departments.items():
            # Igloo returns propertyRef (with _id) or properties (with id)
            d_props = d_info.get('propertyRef', d_info.get('properties', []))
            if any(p.get('_id', p.get('id')) == property_id for p in d_props):
                dept_id = _truncate(d_id, 50)
                dept_name = _truncate(d_info.get('departmentName', d_info.get('name')), 100)
                break

    # Resolve site_id: try config map first, then extract site code from property name
    site_id = property_site_map.get(property_name) if property_name else None
    if not site_id and property_name:
        # Extract site code (e.g. "L029") from property name like "L029 - Commonwealth"
        code_match = re.match(r'^([A-Z]\d{3,4})', property_name)
        if code_match:
            site_id = _site_code_to_id.get(code_match.group(1))

    return {
        'deviceId': _truncate(item.get('deviceId', ''), 30) or '',
        'deviceName': _truncate(item.get('deviceName', ''), 50) or '',
        'type': _truncate(item.get('type', ''), 20) or '',
        'igloo_id': _truncate(item.get('id'), 50),
        'batteryLevel': _clamp_int(item.get('batteryLevel'), 0, 100),
        'pairedAt': parse_iso_datetime(item.get('pairedAt')),
        'lastSync': parse_iso_datetime(item.get('lastSync')),
        'properties': props or None,
        'linkedDevices': item.get('linkedDevices'),
        'linkedAccessories': item.get('linkedAccessories'),
        'propertyId': property_id,
        'propertyName': property_name,
        'departmentId': dept_id,
        'departmentName': dept_name,
        'site_id': site_id,
        'raw_json': item,
    }


# =============================================================================
# Database Operations
# =============================================================================

def push_to_database(
    engine: Engine,
    data: List[Dict[str, Any]],
    model,
    constraint_columns: List[str],
    label: str,
) -> int:
    """Upsert records to esa_backend PostgreSQL."""
    if not data:
        logger.info("  No %s data to push", label)
        return 0

    session_manager = SessionManager(engine)

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, DatabaseType.POSTGRESQL)
        upsert_ops.upsert_batch(
            model=model,
            records=data,
            constraint_columns=constraint_columns,
            chunk_size=100,
        )

    tqdm.write(f"  Upserted {len(data)} {label} records to esa_backend")
    return len(data)


# =============================================================================
# Smart Lock Auto-Population
# =============================================================================

def sync_devices_to_smart_locks(
    engine: Engine,
    device_records: List[Dict[str, Any]],
) -> Tuple[int, int]:
    """
    Auto-create/update smart_lock_keypads and smart_lock_padlocks from Igloo devices.

    Uses deviceId (Bluetooth hardware ID, e.g. SP2X2916499b) as keypad_id/padlock_id.
    On first create: status = 'not_assigned'. On update: preserves existing status.
    Site changes are applied (discrepancy highlighted in UI, not auto-corrected for assignments).
    """
    from web.models.smart_lock import SmartLockKeypad, SmartLockPadlock

    Session = sessionmaker(bind=engine)
    session = Session()

    kp_count = 0
    pl_count = 0

    try:
        for rec in device_records:
            device_id = rec.get('deviceId', '')
            device_type = rec.get('type', '')
            site_id = rec.get('site_id')

            if not device_id or not site_id:
                continue

            if device_type == 'Keypad':
                existing = session.query(SmartLockKeypad).filter_by(
                    keypad_id=device_id
                ).first()
                if existing:
                    if existing.site_id != site_id:
                        existing.site_id = site_id
                        logger.info("Keypad %s site changed to %s", device_id, site_id)
                else:
                    session.add(SmartLockKeypad(
                        keypad_id=device_id,
                        site_id=site_id,
                        status='not_assigned',
                        created_by='igloo_pipeline',
                    ))
                kp_count += 1

            elif device_type == 'Lock':
                existing = session.query(SmartLockPadlock).filter_by(
                    padlock_id=device_id
                ).first()
                if existing:
                    if existing.site_id != site_id:
                        existing.site_id = site_id
                        logger.info("Padlock %s site changed to %s", device_id, site_id)
                else:
                    session.add(SmartLockPadlock(
                        padlock_id=device_id,
                        site_id=site_id,
                        status='not_assigned',
                        created_by='igloo_pipeline',
                    ))
                pl_count += 1

        session.commit()
    except Exception:
        session.rollback()
        logger.exception("Failed to sync devices to smart locks")
    finally:
        session.close()

    return kp_count, pl_count


# Module-level cache for site code -> SiteID lookup (built once per pipeline run)
_site_code_to_id: Dict[str, int] = {}


def _build_site_code_map():
    """Build SiteCode -> SiteID lookup from esa_pbi siteinfo table."""
    global _site_code_to_id
    try:
        pbi_url = get_database_url('pbi')
        pbi_engine = create_engine(pbi_url)
        from sqlalchemy import text
        with pbi_engine.connect() as conn:
            rows = conn.execute(text('SELECT "SiteCode", "SiteID" FROM siteinfo')).fetchall()
        pbi_engine.dispose()
        _site_code_to_id = {r[0]: r[1] for r in rows if r[0]}
        logger.info("Built site code map: %d entries", len(_site_code_to_id))
    except Exception:
        logger.exception("Failed to build site code map from siteinfo")


# =============================================================================
# Pipeline Functions
# =============================================================================

def run_pipeline(
    mode: str,
    property_site_map: Dict[str, int],
) -> Tuple[int, int]:
    """
    Run the Igloo sync pipeline.

    Args:
        mode: 'backfill' or 'auto' (both do full sync — Igloo API has no delta endpoint)
        property_site_map: Mapping of Igloo property name -> ESA SiteID

    Returns:
        Tuple of (property_count, device_count)
    """
    # Build site code lookup for dynamic property name -> SiteID resolution
    _build_site_code_map()

    # Get credentials from vault
    client_id = vault_config('IGLOO_CLIENT_ID')
    client_secret = vault_config('IGLOO_CLIENT_SECRET')
    if not client_id or not client_secret:
        raise ValueError(
            "IGLOO_CLIENT_ID and IGLOO_CLIENT_SECRET not found. "
            "Set them in the vault via /admin/secrets."
        )

    http_client = HTTPClient(default_timeout=60)
    auth = IglooAuth(client_id, client_secret, http_client)

    # --- Stage 1: Fetch properties ---
    print("[STAGE:FETCH] Fetching properties from Igloo API")
    raw_properties = fetch_paginated(http_client, auth, 'properties')
    print(f"  Fetched {len(raw_properties)} properties")

    # --- Stage 2: Fetch departments (for department name mapping) ---
    print("[STAGE:FETCH] Fetching departments from Igloo API")
    raw_departments = fetch_paginated(http_client, auth, 'departments')
    print(f"  Fetched {len(raw_departments)} departments")

    # Build department lookup: {dept_id: dept_data}
    departments = {}
    for dept in raw_departments:
        dept_id = dept.get('id')
        if dept_id:
            departments[dept_id] = dept

    # --- Stage 3: Fetch all devices ---
    print("[STAGE:FETCH] Fetching devices from Igloo API")
    raw_devices = fetch_paginated(http_client, auth, 'devices')
    print(f"  Fetched {len(raw_devices)} devices")

    # --- Stage 4: Fetch device details for expanded property info ---
    print("[STAGE:FETCH] Fetching device details")
    detailed_devices = []
    for device in tqdm(raw_devices, desc="  Device details", unit="dev"):
        device_id = device.get('deviceId')
        if device_id:
            detail = fetch_device_detail(http_client, auth, device_id)
            if detail:
                detailed_devices.append(detail)
            else:
                detailed_devices.append(device)
        else:
            detailed_devices.append(device)

    # --- Stage 5: Transform ---
    print("[STAGE:TRANSFORM] Transforming data")
    property_records = [transform_property(p, property_site_map) for p in raw_properties]
    device_records = [transform_device(d, departments, property_site_map) for d in detailed_devices]

    # Deduplicate by unique key
    seen_props = {}
    for rec in property_records:
        seen_props[rec['propertyId']] = rec
    property_records = list(seen_props.values())

    seen_devs = {}
    for rec in device_records:
        seen_devs[rec['deviceId']] = rec
    device_records = list(seen_devs.values())

    print(f"  {len(property_records)} unique properties, {len(device_records)} unique devices")

    # --- Stage 6: Push to database ---
    print("[STAGE:PUSH] Writing to esa_backend PostgreSQL")
    db_url = get_database_url('backend')
    engine = create_engine(db_url)

    try:
        prop_count = push_to_database(
            engine, property_records, IglooProperty, ['propertyId'], 'property')
        dev_count = push_to_database(
            engine, device_records, IglooDevice, ['deviceId'], 'device')

        # --- Stage 7: Auto-populate smart lock keypads/padlocks ---
        print("[STAGE:SYNC] Auto-populating smart lock keypads/padlocks from Igloo devices")
        kp_count, pl_count = sync_devices_to_smart_locks(engine, device_records)
        print(f"  Keypads: {kp_count} synced, Padlocks: {pl_count} synced")
    finally:
        engine.dispose()

    return prop_count, dev_count


# =============================================================================
# CLI and Main
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Igloo Smart Lock to SQL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill mode - fetch all properties and devices
  python -m datalayer.igloo_to_sql --mode backfill

  # Auto mode - incremental daily sync
  python -m datalayer.igloo_to_sql --mode auto
        """
    )

    parser.add_argument(
        '--mode',
        choices=['backfill', 'auto'],
        required=True,
        help='Extraction mode: backfill (all data), auto (incremental)'
    )

    return parser.parse_args()


def main():
    """Main function to fetch and push Igloo data to SQL."""
    args = parse_args()

    # Load property-to-site mapping from pipelines.yaml
    # (get_pipeline_config looks under scheduler.yaml which has no pipeline entries)
    import yaml
    pipelines_yaml = Path(__file__).parent.parent / 'config' / 'pipelines.yaml'
    property_site_map = {}
    try:
        with open(pipelines_yaml) as f:
            cfg = yaml.safe_load(f) or {}
        property_site_map = cfg.get('pipelines', {}).get('igloo', {}).get('property_site_map', {})
    except Exception:
        logger.warning("Could not load property_site_map from pipelines.yaml")

    # Print header
    print("=" * 70)
    print("Igloo Smart Lock to SQL Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode.upper()}")
    print(f"Property-Site mappings: {len(property_site_map)}")
    print(f"Target: esa_backend (PostgreSQL)")
    print("=" * 70)
    print("[STAGE:INIT] Igloo")

    prop_count, dev_count = run_pipeline(
        mode=args.mode,
        property_site_map=property_site_map,
    )

    # Print summary
    total = prop_count + dev_count
    print(f"[STAGE:COMPLETE] {total} records")
    print("\n" + "=" * 70)
    print("Pipeline completed!")
    print(f"  Properties: {prop_count} records")
    print(f"  Devices: {dev_count} records")
    print("=" * 70)


if __name__ == "__main__":
    main()
