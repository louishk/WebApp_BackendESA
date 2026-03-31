"""
Igloo Smart Lock API Client

Reusable client for the Igloo Works API (v2). Handles OAuth2 authentication,
cursor-based pagination, and all device/property/department endpoints.

Credentials loaded from vault: IGLOO_CLIENT_ID, IGLOO_CLIENT_SECRET

Usage:
    from common.igloo_client import IglooClient

    client = IglooClient()
    devices = client.list_devices()
    device = client.get_device("ABC123")
    client.create_custom_pin("ABC123", "1234", "Tenant A",
                             start_dt="2026-04-01T00:00:00Z",
                             end_dt="2026-04-30T23:59:59Z")
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

# deviceId format: alphanumeric + hyphens/underscores, max 30 chars
_DEVICE_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,30}$')
# accessId / departmentId: alphanumeric + hyphens/underscores, max 50 chars
_ACCESS_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,50}$')
_DEPT_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,50}$')


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
        """Look up departmentId from igloo_devices table in esa_backend DB."""
        try:
            from sqlalchemy import create_engine, text
            from common.config_loader import get_database_url
            engine = create_engine(get_database_url('backend'))
            with engine.connect() as conn:
                row = conn.execute(
                    text('SELECT "departmentId" FROM igloo_devices WHERE "deviceId" = :did'),
                    {'did': device_id},
                ).fetchone()
            engine.dispose()
            return row[0] if row and row[0] else None
        except Exception:
            logger.exception("Failed to resolve departmentId for %s", device_id)
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

    def create_custom_pin(
        self,
        device_id: str,
        pin: str,
        name: str,
        start_dt: Optional[str] = None,
        end_dt: Optional[str] = None,
        pin_type: str = 'permanent',
        department_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a custom PIN on a device via a job.

        Args:
            device_id: Igloo device ID.
            pin: PIN code to set.
            name: Human-readable name/label for the PIN.
            start_dt: ISO-8601 start datetime (required for 'duration' pinType).
            end_dt: ISO-8601 end datetime (required for 'duration' pinType).
            pin_type: 'permanent' or 'duration'.
            department_id: Igloo department ID (required by API).

        Returns:
            Job response dict.
        """
        self._validate_device_id(device_id)

        # Resolve departmentId: try DB first, then API departments list
        if not department_id:
            department_id = self._resolve_department_id(device_id)
        if not department_id:
            # Fallback: fetch first department from API
            depts = self.list_departments()
            if depts:
                department_id = depts[0].get('id')
        if not department_id:
            raise IglooAPIError("Cannot determine departmentId for device")

        now = datetime.now(timezone.utc)
        if not start_dt:
            start_dt = now.strftime('%Y-%m-%dT%H:%M:%S+00:00')

        payload: Dict[str, Any] = {
            'customPin': pin,
            'pinType': pin_type,
            'startDateTime': start_dt,
            'departmentId': department_id,
        }
        # endDate only for duration PINs (permanent PINs don't accept it)
        if pin_type == 'duration':
            if not end_dt:
                end_dt = (now + timedelta(days=365)).strftime('%Y-%m-%dT%H:%M:%S+00:00')
            payload['endDate'] = end_dt

        try:
            resp = self._http.post(
                f"{API_BASE_URL}/devices/{device_id}/jobs",
                headers=self._auth_headers(),
                json=payload,
            )
            resp.raise_for_status()
            return resp.json()
        except IglooAPIError:
            raise
        except Exception as exc:
            # Extract response body from HTTPError for debugging
            resp_body = ''
            if hasattr(exc, 'response') and exc.response is not None:
                try:
                    resp_body = exc.response.text[:500]
                except Exception:
                    pass
            logger.error(
                "Failed to create custom PIN on device %s — payload: %s — response: %s",
                device_id, payload, resp_body,
            )
            raise IglooAPIError("Failed to create custom PIN")

    def create_permanent_pin(self, device_id: str) -> Dict[str, Any]:
        """Generate a permanent algorithmic PIN for a device.

        Args:
            device_id: Igloo device ID.

        Returns:
            PIN response dict.
        """
        self._validate_device_id(device_id)
        try:
            resp = self._http.post(
                f"{API_BASE_URL}/devices/{device_id}/algopin/permanent",
                headers=self._auth_headers(),
            )
            resp.raise_for_status()
            return resp.json()
        except IglooAPIError:
            raise
        except Exception:
            logger.exception("Failed to create permanent PIN on device %s", device_id)
            raise IglooAPIError("Failed to create permanent PIN")

    def create_daily_pin(self, device_id: str) -> Dict[str, Any]:
        """Generate a daily algorithmic PIN for a device.

        Args:
            device_id: Igloo device ID.

        Returns:
            PIN response dict.
        """
        self._validate_device_id(device_id)
        try:
            resp = self._http.post(
                f"{API_BASE_URL}/devices/{device_id}/algopin/daily",
                headers=self._auth_headers(),
            )
            resp.raise_for_status()
            return resp.json()
        except IglooAPIError:
            raise
        except Exception:
            logger.exception("Failed to create daily PIN on device %s", device_id)
            raise IglooAPIError("Failed to create daily PIN")

    def create_hourly_pin(self, device_id: str) -> Dict[str, Any]:
        """Generate an hourly algorithmic PIN for a device.

        Args:
            device_id: Igloo device ID.

        Returns:
            PIN response dict.
        """
        self._validate_device_id(device_id)
        try:
            resp = self._http.post(
                f"{API_BASE_URL}/devices/{device_id}/algopin/hourly",
                headers=self._auth_headers(),
            )
            resp.raise_for_status()
            return resp.json()
        except IglooAPIError:
            raise
        except Exception:
            logger.exception("Failed to create hourly PIN on device %s", device_id)
            raise IglooAPIError("Failed to create hourly PIN")

    def create_otp_pin(self, device_id: str) -> Dict[str, Any]:
        """Generate a one-time algorithmic PIN for a device.

        Args:
            device_id: Igloo device ID.

        Returns:
            PIN response dict.
        """
        self._validate_device_id(device_id)
        try:
            resp = self._http.post(
                f"{API_BASE_URL}/devices/{device_id}/algopin/onetime",
                headers=self._auth_headers(),
            )
            resp.raise_for_status()
            return resp.json()
        except IglooAPIError:
            raise
        except Exception:
            logger.exception("Failed to create OTP PIN on device %s", device_id)
            raise IglooAPIError("Failed to create one-time PIN")

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
        """Revoke an access entry from a device.

        Args:
            device_id: Igloo device ID.
            access_id: Access entry ID to revoke.

        Returns:
            Deletion response dict.
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
