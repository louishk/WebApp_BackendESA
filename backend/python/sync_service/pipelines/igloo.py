"""
IglooPipeline — fetch Igloo properties + devices from the Igloo Works API and
write into esa_middleware (and esa_backend smart lock tables).

Features:
- OAuth2 client_credentials authentication with expiry-aware token refresh
- Cursor-based pagination for all list endpoints (with page ceiling)
- Upsert by deviceId (devices) and propertyId (properties)
- Auto-populate smart_lock_keypads / smart_lock_padlocks / smart_lock_bridges
- Field validation: truncation to column widths, battery range clamping

Modes (scope key):
  mode: 'auto' (default) | 'backfill'  — both do full sync; Igloo has no delta endpoint
"""

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.config import get_engine
from common.models import IglooProperty, IglooDevice

logger = logging.getLogger(__name__)

# =============================================================================
# Constants
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

    def __init__(self, client_id: str, client_secret: str, http_client):
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
# Igloo API Fetch Helpers
# =============================================================================

def fetch_paginated(
    http_client,
    auth: IglooAuth,
    endpoint: str,
    limit: int = DEFAULT_PAGE_LIMIT,
    params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch all records from a paginated Igloo API endpoint (cursor-based)."""
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

        items = data.get('payload', [])
        if not items:
            break

        all_items.extend(items)
        logger.info("%s page %d: %d items (total: %d)", endpoint, page, len(items), len(all_items))

        # nextCursor is null/absent for most endpoints, empty string "" for /properties
        cursor = data.get('nextCursor')
        if not cursor:
            break

    return all_items


def fetch_device_detail(
    http_client,
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
    props = item.get('properties') or []
    property_id = _truncate(props[0].get('id'), 50) if props else None
    property_name = _truncate(props[0].get('name'), 100) if props else None

    dept_id = None
    dept_name = None
    if item.get('departmentId'):
        dept_id = _truncate(item['departmentId'], 50)
        dept_info = departments.get(dept_id, {})
        dept_name = _truncate(dept_info.get('name'), 100)
    elif property_id:
        for d_id, d_info in departments.items():
            d_props = d_info.get('propertyRef', d_info.get('properties', []))
            if any(p.get('_id', p.get('id')) == property_id for p in d_props):
                dept_id = _truncate(d_id, 50)
                dept_name = _truncate(d_info.get('departmentName', d_info.get('name')), 100)
                break

    # Resolve site_id: 1) module-level igloo_property_id cache, 2) config map, 3) regex site code
    site_id = _igloo_prop_to_site.get(property_id) if property_id else None
    if not site_id and property_name:
        site_id = property_site_map.get(property_name)
    if not site_id and property_name:
        code_match = re.match(r'^([A-Z]\d{3,4})', property_name)
        if code_match:
            site_id = _site_code_to_id.get(code_match.group(1))

    # Auto-enrich: record newly discovered igloo_property_id mappings
    if site_id and property_id and property_id not in _igloo_prop_to_site:
        _new_igloo_mappings.append({
            'site_id': site_id,
            'prop_id': property_id,
            'dept_id': dept_id,
        })
        _igloo_prop_to_site[property_id] = site_id  # prevent duplicates within same run

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
# Smart Lock Auto-Population
# =============================================================================

def sync_devices_to_smart_locks(
    engine,
    device_records: List[Dict[str, Any]],
) -> Tuple[int, int, int]:
    """
    Auto-create/update smart_lock_keypads, smart_lock_padlocks, smart_lock_bridges
    from Igloo devices.

    Bridges inherit site_id from linked keypads/locks via linkedDevices.
    Uses deviceId as keypad_id/padlock_id/bridge_id.
    On first create: status = 'not_assigned'. On update: preserves existing status.
    """
    from sqlalchemy.orm import sessionmaker
    from web.models.smart_lock import SmartLockKeypad, SmartLockPadlock, SmartLockBridge

    Session = sessionmaker(bind=engine)
    session = Session()

    kp_count = 0
    pl_count = 0
    br_count = 0

    device_to_site = {
        r.get('deviceId'): r.get('site_id')
        for r in device_records
        if r.get('deviceId') and r.get('site_id')
    }

    try:
        for rec in device_records:
            device_id = rec.get('deviceId', '')
            device_type = rec.get('type', '')
            site_id = rec.get('site_id')

            if not site_id and device_type == 'Bridge':
                for ent in (rec.get('linkedDevices') or []):
                    linked_id = (ent or {}).get('deviceId') or (ent or {}).get('id')
                    if linked_id and device_to_site.get(linked_id):
                        site_id = device_to_site[linked_id]
                        break

            if not device_id or not site_id:
                continue

            if device_type == 'Keypad':
                existing = session.query(SmartLockKeypad).filter_by(keypad_id=device_id).first()
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
                existing = session.query(SmartLockPadlock).filter_by(padlock_id=device_id).first()
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

            elif device_type == 'Bridge':
                existing = session.query(SmartLockBridge).filter_by(bridge_id=device_id).first()
                if existing:
                    if existing.site_id != site_id:
                        existing.site_id = site_id
                        logger.info("Bridge %s site changed to %s", device_id, site_id)
                else:
                    session.add(SmartLockBridge(
                        bridge_id=device_id,
                        site_id=site_id,
                        status='not_assigned',
                        created_by='igloo_pipeline',
                    ))
                br_count += 1

        session.commit()
    except Exception:
        session.rollback()
        logger.exception("Failed to sync devices to smart locks")
    finally:
        session.close()

    return kp_count, pl_count, br_count


# =============================================================================
# Module-level caches (populated once per pipeline run)
# =============================================================================

_site_code_to_id: Dict[str, int] = {}          # SiteCode -> SiteID
_igloo_prop_to_site: Dict[str, int] = {}        # igloo_property_id -> SiteID
_new_igloo_mappings: List[Dict[str, Any]] = []  # auto-discovered mappings to persist


# =============================================================================
# Public run() orchestration function
# =============================================================================

def run(mode: str = 'auto', property_site_map: Optional[Dict[str, int]] = None,
        site_ids: Optional[List[int]] = None) -> Dict[str, Any]:
    """Fetch Igloo properties + devices and upsert to esa_middleware.

    Also auto-populates smart lock tables in esa_backend and enriches
    mw_siteinfo with newly discovered igloo_property_id mappings.

    Returns dict with keys: properties, devices, siteinfo_enriched,
    keypads_seen, padlocks_seen, bridges_seen.
    """
    from common.http_client import HTTPClient
    from common.secrets_vault import vault_config

    if property_site_map is None:
        property_site_map = {}

    client_id = vault_config('IGLOO_CLIENT_ID')
    client_secret = vault_config('IGLOO_CLIENT_SECRET')
    if not client_id or not client_secret:
        raise ValueError(
            "IGLOO_CLIENT_ID and IGLOO_CLIENT_SECRET not found. "
            "Set them in the vault via /admin/secrets."
        )

    mw_engine = get_engine('middleware')

    # Populate module-level site-lookup caches from mw_siteinfo
    global _site_code_to_id, _igloo_prop_to_site
    with mw_engine.connect() as conn:
        rows = conn.execute(text(
            'SELECT "SiteID", "SiteCode", igloo_property_id, igloo_department_id '
            'FROM mw_siteinfo'
        )).fetchall()
    _site_code_to_id = {r[1]: r[0] for r in rows if r[1]}
    _igloo_prop_to_site = {r[2]: r[0] for r in rows if r[2]}
    _new_igloo_mappings.clear()
    logger.info("Site lookups: %d site codes, %d igloo property mappings",
                len(_site_code_to_id), len(_igloo_prop_to_site))

    http = HTTPClient(default_timeout=60)
    auth = IglooAuth(client_id, client_secret, http)

    logger.info("igloo: fetching properties")
    raw_properties = fetch_paginated(http, auth, 'properties')
    logger.info("igloo: fetched %d properties", len(raw_properties))

    logger.info("igloo: fetching departments")
    raw_departments = fetch_paginated(http, auth, 'departments')
    logger.info("igloo: fetched %d departments", len(raw_departments))

    departments = {d.get('id'): d for d in raw_departments if d.get('id')}

    logger.info("igloo: fetching devices")
    raw_devices = fetch_paginated(http, auth, 'devices')
    logger.info("igloo: fetched %d devices", len(raw_devices))

    logger.info("igloo: fetching device details")
    detailed_devices: List[Dict[str, Any]] = []
    for d in raw_devices:
        did = d.get('deviceId')
        if did:
            detail = fetch_device_detail(http, auth, did)
            detailed_devices.append(detail or d)
        else:
            detailed_devices.append(d)

    property_records = [transform_property(p, property_site_map) for p in raw_properties]
    device_records = [transform_device(d, departments, property_site_map) for d in detailed_devices]

    if site_ids:
        site_ids_set = set(site_ids)
        property_records = [r for r in property_records if r.get('site_id') in site_ids_set]
        device_records = [r for r in device_records if r.get('site_id') in site_ids_set]

    # Dedup by unique key
    property_records = list({r['propertyId']: r for r in property_records}.values())
    device_records = list({r['deviceId']: r for r in device_records}.values())

    logger.info("igloo: %d unique properties, %d unique devices",
                len(property_records), len(device_records))

    # Write to middleware via SQLAlchemy Core on_conflict
    with mw_engine.begin() as conn:
        if property_records:
            stmt = pg_insert(IglooProperty.__table__).values(property_records)
            stmt = stmt.on_conflict_do_update(
                index_elements=['propertyId'],
                set_={c: getattr(stmt.excluded, c) for c in property_records[0].keys()
                      if c != 'propertyId'},
            )
            conn.execute(stmt)

        if device_records:
            stmt = pg_insert(IglooDevice.__table__).values(device_records)
            stmt = stmt.on_conflict_do_update(
                index_elements=['deviceId'],
                set_={c: getattr(stmt.excluded, c) for c in device_records[0].keys()
                      if c != 'deviceId'},
            )
            conn.execute(stmt)

    logger.info("igloo: upserted %d properties, %d devices to middleware",
                len(property_records), len(device_records))

    # Persist auto-discovered igloo_property_id / igloo_department_id to mw_siteinfo
    enriched = 0
    if _new_igloo_mappings:
        with mw_engine.begin() as conn:
            for m in _new_igloo_mappings:
                conn.execute(text("""
                    UPDATE mw_siteinfo
                    SET igloo_property_id = COALESCE(igloo_property_id, :prop_id),
                        igloo_department_id = COALESCE(igloo_department_id, :dept_id)
                    WHERE "SiteID" = :site_id
                """), m)
        enriched = len(_new_igloo_mappings)
        logger.info("igloo: persisted %d new Igloo mappings to mw_siteinfo", enriched)

    # Auto-populate smart lock keypads/padlocks/bridges in esa_backend
    backend_engine = get_engine('backend')
    kp_added, pl_added, br_added = sync_devices_to_smart_locks(backend_engine, device_records)
    logger.info("igloo: keypads=%d padlocks=%d bridges=%d synced to esa_backend",
                kp_added, pl_added, br_added)

    return {
        'properties': len(property_records),
        'devices': len(device_records),
        'siteinfo_enriched': enriched,
        'keypads_seen': kp_added,
        'padlocks_seen': pl_added,
        'bridges_seen': br_added,
    }


# =============================================================================
# Pipeline class
# =============================================================================

class IglooPipeline(BasePipeline):
    """Sync Igloo properties + devices → esa_middleware."""

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode: str = scope.get('mode', 'auto')
        property_site_map: Dict[str, int] = scope.get('property_site_map') or {}
        site_ids = scope.get('site_ids')

        result = run(mode=mode, property_site_map=property_site_map, site_ids=site_ids)

        return RunResult(
            status='refreshed',
            records=result['properties'] + result['devices'],
            scope=scope,
            metadata=result,
        )
