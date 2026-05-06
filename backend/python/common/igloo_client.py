"""
Igloo Smart Lock API Client

Reusable client for the Igloo Works API (v2). Handles OAuth2 authentication,
cursor-based pagination, and all device/property/department endpoints.

Credentials loaded from vault: IGLOO_CLIENT_ID, IGLOO_CLIENT_SECRET

Usage:
    from common.igloo_client import IglooClient, PIN_TYPE_DURATION

    client = IglooClient()
    devices = client.list_devices()
    device = client.get_device("ABC123")
    client.create_pin_via_bridge("ABC123", "1234", "ESA-27525-12345",
                                 pin_type=PIN_TYPE_DURATION,
                                 start_dt="2026-04-01T00:00:00+00:00",
                                 end_dt="2026-04-30T23:59:59+00:00")
"""

import logging
import re
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from common.http_client import HTTPClient
from common.secrets_vault import vault_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

API_BASE_URL = 'https://api.iglooworks.co/v2'
AUTH_URL = 'https://auth.iglooworks.co/oauth2/token'
DEFAULT_PAGE_LIMIT = 300
MAX_PAGES = 500  # Safety ceiling: ~150,000 records at limit=300
DEFAULT_TIMEOUT = 60

# Bridge-proxied job types (verified live 2026-04-28; see project_igloo_bridge_api.md)
BRIDGE_JOB_LOCK            = 1
BRIDGE_JOB_UNLOCK          = 2
BRIDGE_JOB_CREATE_PIN      = 4
BRIDGE_JOB_DELETE_PIN      = 5
BRIDGE_JOB_BATTERY_LEVEL   = 9
BRIDGE_JOB_DEVICE_STATUS   = 10
BRIDGE_JOB_ACTIVITY_LOGS   = 15

# Bridge create-pin pinType integer enum
PIN_TYPE_OTP        = 1
PIN_TYPE_PERMANENT  = 2
PIN_TYPE_DURATION   = 4

BRIDGE_POLL_TIMEOUT  = 90   # seconds — battery/status often >30s; expiryDate is ~120s
BRIDGE_POLL_INTERVAL = 2

# deviceId format: alphanumeric + hyphens/underscores, max 30 chars
_DEVICE_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,30}$')
# accessId / departmentId: alphanumeric + hyphens/underscores, max 50 chars
_ACCESS_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,50}$')
_DEPT_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,50}$')
# PIN format for bridge create-pin: 4–6 digits (Igloo bridge limit, verified
# live: longer PINs return 400 "'pin' length is 4-6 digits")
_PIN_RE_INTERNAL = re.compile(r'^\d{4,6}$')


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class IglooAPIError(Exception):
    """Raised when an Igloo API operation fails.

    The message is safe to surface to callers (no internal details).
    """
    pass


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class IglooClient:
    """Thread-safe Igloo API v2 client with OAuth2 token management."""

    def __init__(self, http_client: Optional[HTTPClient] = None):
        """Initialise client, loading credentials from vault.

        Args:
            http_client: Optional pre-configured HTTPClient. A new one with
                         60 s default timeout is created when omitted.

        Raises:
            IglooAPIError: If vault credentials are missing.
        """
        self._client_id = vault_config('IGLOO_CLIENT_ID')
        self._client_secret = vault_config('IGLOO_CLIENT_SECRET')
        if not self._client_id or not self._client_secret:
            raise IglooAPIError(
                "Igloo credentials not configured. "
                "Set IGLOO_CLIENT_ID and IGLOO_CLIENT_SECRET in the vault."
            )

        self._http = http_client or HTTPClient(default_timeout=DEFAULT_TIMEOUT)

        # Token state (guarded by lock for thread safety)
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Auth helpers (private)
    # ------------------------------------------------------------------

    def _ensure_auth(self) -> None:
        """Refresh the OAuth2 token if expired or about to expire (30 s buffer)."""
        now = datetime.now(timezone.utc)
        with self._lock:
            if self._token and self._expires_at and now < self._expires_at - timedelta(seconds=30):
                return  # token still valid

            logger.info("Authenticating with Igloo API (client_credentials)")
            try:
                response = self._http.post(
                    AUTH_URL,
                    data={
                        'grant_type': 'client_credentials',
                        'client_id': self._client_id,
                        'client_secret': self._client_secret,
                    },
                    headers={'Content-Type': 'application/x-www-form-urlencoded'},
                )
                response.raise_for_status()
                token_data = response.json()
                self._token = token_data['access_token']
                ttl = int(token_data.get('expires_in', 86400))
                self._expires_at = now + timedelta(seconds=ttl)
                logger.info("Igloo API authenticated (token expires in %ds)", ttl)
            except Exception:
                logger.exception("Igloo API authentication failed")
                raise IglooAPIError("Failed to authenticate with Igloo API")

    def _auth_headers(self) -> Dict[str, str]:
        """Return headers dict with current Bearer token."""
        self._ensure_auth()
        return {
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json',
        }

    # ------------------------------------------------------------------
    # Pagination helper (private)
    # ------------------------------------------------------------------

    def _fetch_paginated(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
    ) -> List[Dict[str, Any]]:
        """Fetch all records from a cursor-paginated Igloo endpoint.

        Args:
            endpoint: Relative path under API_BASE_URL (e.g. "devices").
            params: Extra query parameters merged into every request.
            limit: Page size (max items per request).

        Returns:
            Aggregated list of payload dicts across all pages.

        Raises:
            IglooAPIError: On HTTP or parsing failure.
        """
        all_items: List[Dict[str, Any]] = []
        cursor = None
        page = 0

        while True:
            page += 1
            if page > MAX_PAGES:
                logger.error(
                    "_fetch_paginated: hit page ceiling (%d) for %s — aborting",
                    MAX_PAGES, endpoint,
                )
                break

            req_params: Dict[str, Any] = {'limit': limit}
            if params:
                req_params.update(params)
            if cursor:
                req_params['cursor'] = cursor

            try:
                resp = self._http.get(
                    f"{API_BASE_URL}/{endpoint}",
                    headers=self._auth_headers(),
                    params=req_params,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                logger.exception("Igloo API request failed: %s (page %d)", endpoint, page)
                raise IglooAPIError(f"Failed to fetch {endpoint}")

            items = data.get('payload', [])
            if not items:
                break

            all_items.extend(items)
            logger.info(
                "  %s page %d: %d items (total: %d)",
                endpoint, page, len(items), len(all_items),
            )

            # nextCursor is null/absent when done, empty string "" for /properties
            cursor = data.get('nextCursor')
            if not cursor:
                break

        return all_items

    # ------------------------------------------------------------------
    # Validation helpers (private)
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_device_id(device_id: str) -> None:
        """Raise IglooAPIError if device_id format is invalid."""
        if not _DEVICE_ID_RE.match(device_id):
            raise IglooAPIError("Invalid device ID format")

    @staticmethod
    def _validate_access_id(access_id: str) -> None:
        """Raise IglooAPIError if access_id format is invalid."""
        if not _ACCESS_ID_RE.match(access_id):
            raise IglooAPIError("Invalid access ID format")

    @staticmethod
    def _validate_dept_id(dept_id: str) -> None:
        """Raise IglooAPIError if dept_id format is invalid."""
        if not _DEPT_ID_RE.match(dept_id):
            raise IglooAPIError("Invalid department ID format")

    @staticmethod
    def _resolve_department_id(device_id: str) -> Optional[str]:
        """Resolve departmentId for a device.

        Order: igloo_devices.departmentId → igloo_devices.site_id →
        mw_siteinfo.igloo_department_id. Keypads/bridges typically have null
        departmentId at the device level, so the siteinfo fallback is the
        reliable path.
        """
        try:
            from sqlalchemy import create_engine, text
            from common.config_loader import get_database_url
            engine = create_engine(get_database_url('middleware'))
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        'SELECT "departmentId", site_id FROM igloo_devices '
                        'WHERE "deviceId" = :did'
                    ),
                    {'did': device_id},
                ).fetchone()
                if row and row[0]:
                    engine.dispose()
                    return row[0]
                if row and row[1]:
                    site_row = conn.execute(
                        text(
                            'SELECT igloo_department_id FROM mw_siteinfo '
                            'WHERE "SiteID" = :sid'
                        ),
                        {'sid': row[1]},
                    ).fetchone()
                    engine.dispose()
                    return site_row[0] if site_row and site_row[0] else None
            engine.dispose()
            return None
        except Exception:
            logger.exception("Failed to resolve departmentId for %s", device_id)
            return None

    @staticmethod
    def _resolve_bridge_id(device_id: str) -> Optional[str]:
        """Find the Bridge paired to a device via igloo_devices.linkedAccessories
        (keypads link to a bridge there) or linkedDevices (locks). Returns the
        first Bridge deviceId found, or None.
        """
        try:
            from sqlalchemy import create_engine, text
            from common.config_loader import get_database_url
            engine = create_engine(get_database_url('middleware'))
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        'SELECT "linkedAccessories", "linkedDevices" '
                        'FROM igloo_devices WHERE "deviceId" = :did'
                    ),
                    {'did': device_id},
                ).fetchone()
            engine.dispose()
            if not row:
                return None
            for blob in (row[0], row[1]):
                if not blob:
                    continue
                items = blob if isinstance(blob, list) else []
                for ent in items:
                    if isinstance(ent, dict) and (ent.get('type') or '').lower() == 'bridge':
                        return ent.get('deviceId') or ent.get('id')
            return None
        except Exception:
            logger.exception("Failed to resolve bridgeId for %s", device_id)
            return None

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def list_devices(self, department_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all devices, optionally filtered by department.

        Args:
            department_id: Optional department ID to filter by.

        Returns:
            List of device dicts.
        """
        params = {}
        if department_id:
            self._validate_dept_id(department_id)
            params['departmentId'] = department_id
        return self._fetch_paginated('devices', params=params)

    def get_device(self, device_id: str) -> Dict[str, Any]:
        """Get expanded detail for a single device.

        Args:
            device_id: Igloo device ID (alphanumeric, max 30 chars).

        Returns:
            Device detail dict.
        """
        self._validate_device_id(device_id)
        try:
            resp = self._http.get(
                f"{API_BASE_URL}/devices/{device_id}",
                headers=self._auth_headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get('payload', data)
        except IglooAPIError:
            raise
        except Exception:
            logger.exception("Failed to fetch device %s", device_id)
            raise IglooAPIError("Failed to fetch device details")

    def list_device_access(self, device_id: str) -> List[Dict[str, Any]]:
        """List access entries for a device.

        Args:
            device_id: Igloo device ID.

        Returns:
            List of access dicts.
        """
        self._validate_device_id(device_id)
        return self._fetch_paginated(f'devices/{device_id}/access')

    def list_device_activity(
        self, device_id: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """List recent activity for a device.

        Args:
            device_id: Igloo device ID.
            limit: Max items per page (default 50).

        Returns:
            List of activity dicts.
        """
        self._validate_device_id(device_id)
        return self._fetch_paginated(f'devices/{device_id}/activity', limit=limit)

    def list_device_jobs(self, device_id: str) -> List[Dict[str, Any]]:
        """List pending/completed jobs for a device.

        Args:
            device_id: Igloo device ID.

        Returns:
            List of job dicts.
        """
        self._validate_device_id(device_id)
        return self._fetch_paginated(f'devices/{device_id}/jobs')

    def list_departments(self) -> List[Dict[str, Any]]:
        """List all departments.

        Returns:
            List of department dicts.
        """
        return self._fetch_paginated('departments')

    def list_department_access(self, dept_id: str) -> List[Dict[str, Any]]:
        """List access entries for a department.

        Args:
            dept_id: Department ID.

        Returns:
            List of access dicts.
        """
        self._validate_dept_id(dept_id)
        return self._fetch_paginated(f'departments/{dept_id}/access')

    def list_properties(self) -> List[Dict[str, Any]]:
        """List all properties.

        Returns:
            List of property dicts.
        """
        return self._fetch_paginated('properties')

    # ------------------------------------------------------------------
    # Write operations — PIN management
    # ------------------------------------------------------------------

    # PIN management is via bridge only — see create_pin_via_bridge() and
    # delete_pin_via_bridge() further below. Direct /devices/{id}/jobs and
    # /algopin/* are not used (the bridge executes BLE commands on-site, the
    # direct queues just wait for the next sync).

    def create_ekey(
        self,
        device_id: str,
        name: str,
        start_dt: Optional[str] = None,
        end_dt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create an eKey for a device.

        Args:
            device_id: Igloo device ID.
            name: Human-readable name/label for the eKey.
            start_dt: Optional ISO-8601 start datetime.
            end_dt: Optional ISO-8601 end datetime.

        Returns:
            eKey response dict.
        """
        self._validate_device_id(device_id)
        payload: Dict[str, Any] = {'name': name}
        if start_dt:
            payload['startDate'] = start_dt
        if end_dt:
            payload['endDate'] = end_dt

        try:
            resp = self._http.post(
                f"{API_BASE_URL}/devices/{device_id}/ekey",
                headers=self._auth_headers(),
                json=payload,
            )
            resp.raise_for_status()
            return resp.json()
        except IglooAPIError:
            raise
        except Exception:
            logger.exception("Failed to create eKey on device %s", device_id)
            raise IglooAPIError("Failed to create eKey")

    def revoke_access(self, device_id: str, access_id: str) -> Dict[str, Any]:
        """Revoke an access entry from a device (DEPRECATED — direct API).

        Use delete_pin_via_bridge() instead. This direct DELETE queues a job
        for the next BLE sync rather than executing immediately via the
        on-site bridge.
        """
        self._validate_device_id(device_id)
        self._validate_access_id(access_id)
        try:
            resp = self._http.delete(
                f"{API_BASE_URL}/devices/{device_id}/access/{access_id}",
                headers=self._auth_headers(),
            )
            resp.raise_for_status()
            return resp.json()
        except IglooAPIError:
            raise
        except Exception:
            logger.exception(
                "Failed to revoke access %s on device %s", access_id, device_id
            )
            raise IglooAPIError("Failed to revoke access")

    # ------------------------------------------------------------------
    # Bridge-proxied operations
    # ------------------------------------------------------------------

    def _create_bridge_job(
        self,
        device_id: str,
        bridge_id: str,
        department_id: str,
        job_type: int,
        job_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Submit a bridge-proxied job. Returns the jobId."""
        self._validate_device_id(device_id)
        self._validate_device_id(bridge_id)
        self._validate_dept_id(department_id)
        body: Dict[str, Any] = {
            'jobType': int(job_type),
            'departmentId': department_id,
        }
        if job_data is not None:
            body['jobData'] = job_data
        try:
            resp = self._http.post(
                f"{API_BASE_URL}/devices/{device_id}/jobs/bridges/{bridge_id}",
                headers=self._auth_headers(),
                json=body,
            )
            resp.raise_for_status()
            data = resp.json() or {}
            job_id = data.get('jobId')
            if not job_id:
                raise IglooAPIError("Bridge job submitted but no jobId returned")
            return job_id
        except IglooAPIError:
            raise
        except Exception as exc:
            resp_body = ''
            api_err = ''
            if hasattr(exc, 'response') and exc.response is not None:
                try:
                    resp_body = exc.response.text[:500]
                    api_err = (exc.response.json() or {}).get('error') or ''
                except Exception:
                    pass
            logger.error(
                "Bridge job submit failed device=%s bridge=%s jobType=%s body=%s",
                device_id, bridge_id, job_type, resp_body,
            )
            msg = f"Bridge job rejected: {api_err}" if api_err else "Failed to submit bridge job"
            raise IglooAPIError(msg)

    def _poll_bridge_job(
        self,
        job_id: str,
        timeout: int = BRIDGE_POLL_TIMEOUT,
        interval: int = BRIDGE_POLL_INTERVAL,
    ) -> Dict[str, Any]:
        """Poll /bridge/jobs/{jobId} until completed or timeout.

        Success = `completed=true && jobResponse.jobStatus == 0`.
        Raises IglooAPIError on non-success completions or timeout.
        """
        import time
        deadline = time.time() + timeout
        url = f"{API_BASE_URL}/bridge/jobs/{job_id}"
        headers = self._auth_headers()
        last: Dict[str, Any] = {}
        while time.time() < deadline:
            try:
                resp = self._http.get(url, headers=headers)
                resp.raise_for_status()
                last = resp.json() or {}
            except Exception:
                logger.exception("Bridge poll error job=%s", job_id)
                time.sleep(interval)
                continue
            if last.get('completed'):
                status_code = ((last.get('jobResponse') or {}).get('jobStatus'))
                if status_code == 0:
                    return last
                logger.warning(
                    "Bridge job %s completed with non-zero jobStatus=%s response=%s",
                    job_id, status_code, last.get('jobResponse'),
                )
                raise IglooAPIError(
                    f"Bridge job failed (jobStatus={status_code})"
                )
            time.sleep(interval)
        logger.warning("Bridge job %s timed out after %ds: %s", job_id, timeout, last)
        raise IglooAPIError("Bridge job timed out")

    def create_pin_via_bridge(
        self,
        device_id: str,
        pin: str,
        access_name: str,
        *,
        bridge_id: Optional[str] = None,
        department_id: Optional[str] = None,
        pin_type: int = PIN_TYPE_DURATION,
        start_dt: Optional[str] = None,
        end_dt: Optional[str] = None,
        timeout: int = BRIDGE_POLL_TIMEOUT,
    ) -> Dict[str, Any]:
        """Program a PIN on a keypad/lock via its paired bridge.

        Args:
            device_id: Lock or keypad ID (the device receiving the PIN).
            pin: 4–10 digit PIN value.
            access_name: Label stored on the access entry. Use ESA-{site}-{unit}
                for sync-pipeline ownership.
            bridge_id: Bridge ID; auto-resolved from linkedAccessories if omitted.
            department_id: Igloo dept ID; auto-resolved via siteinfo if omitted.
            pin_type: 1=otp, 2=permanent, 4=duration.
            start_dt: ISO-8601 with offset, e.g. "2026-04-28T10:00:00+00:00".
            end_dt: ISO-8601; required for pin_type=4 (duration).
            timeout: Poll deadline in seconds.

        Returns the final job-status payload on success.
        """
        from datetime import datetime, timezone, timedelta
        if not _PIN_RE_INTERNAL.match(pin or ''):
            raise IglooAPIError("Invalid PIN format (must be 4–6 digits)")

        bridge_id = bridge_id or self._resolve_bridge_id(device_id)
        if not bridge_id:
            raise IglooAPIError("No bridge paired to device")
        department_id = department_id or self._resolve_department_id(device_id)
        if not department_id:
            raise IglooAPIError("Cannot resolve departmentId for device")

        if pin_type == PIN_TYPE_DURATION and not end_dt:
            raise IglooAPIError("end_dt required for duration PIN")

        if not start_dt:
            now = datetime.now(timezone.utc)
            start_dt = now.replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%S+00:00')

        job_data: Dict[str, Any] = {
            'accessName': access_name,
            'pin': pin,
            'pinType': int(pin_type),
            'startDate': start_dt,
        }
        if pin_type == PIN_TYPE_DURATION:
            job_data['endDate'] = end_dt

        job_id = self._create_bridge_job(
            device_id, bridge_id, department_id,
            BRIDGE_JOB_CREATE_PIN, job_data,
        )
        return self._poll_bridge_job(job_id, timeout=timeout)

    def delete_pin_via_bridge(
        self,
        device_id: str,
        access_id: str,
        *,
        bridge_id: Optional[str] = None,
        department_id: Optional[str] = None,
        timeout: int = BRIDGE_POLL_TIMEOUT,
    ) -> Dict[str, Any]:
        """Revoke a PIN by accessId via its paired bridge."""
        self._validate_access_id(access_id)
        bridge_id = bridge_id or self._resolve_bridge_id(device_id)
        if not bridge_id:
            raise IglooAPIError("No bridge paired to device")
        department_id = department_id or self._resolve_department_id(device_id)
        if not department_id:
            raise IglooAPIError("Cannot resolve departmentId for device")

        job_id = self._create_bridge_job(
            device_id, bridge_id, department_id,
            BRIDGE_JOB_DELETE_PIN, {'accessId': access_id},
        )
        return self._poll_bridge_job(job_id, timeout=timeout)
