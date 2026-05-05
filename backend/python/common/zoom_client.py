"""
Zoom Phone API Client

Thread-safe client for Zoom Phone REST API (v2). Handles S2S OAuth authentication,
token-based pagination, and External Contacts / Call Log endpoints.

Credentials loaded from vault: ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET

Usage:
    from common.zoom_client import ZoomClient

    client = ZoomClient()
    contacts = client.list_all_external_contacts()
    call_logs = client.get_all_call_logs(from_date, to_date)
"""

import base64
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from common.http_client import HTTPClient
from common.outbound_stats import track_outbound_api
from common.secrets_vault import vault_config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_zoom_config():
    """Load non-sensitive Zoom config from apis.yaml, with sensible defaults."""
    try:
        from common.config_loader import get_config
        cfg = get_config().apis.zoom
        return {
            'base_url': getattr(cfg, 'base_url', 'https://api.zoom.us/v2'),
            'auth_url': getattr(cfg, 'auth_url', 'https://zoom.us/oauth/token'),
            'timeout': getattr(cfg, 'timeout', 60),
        }
    except Exception:
        return {
            'base_url': 'https://api.zoom.us/v2',
            'auth_url': 'https://zoom.us/oauth/token',
            'timeout': 60,
        }


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_PAGE_SIZE = 100
MAX_PAGES = 500  # Safety ceiling


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ZoomAPIError(Exception):
    """Raised when a Zoom API operation fails.

    The message is safe to surface to callers (no internal details).
    """
    pass


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class ZoomClient:
    """Thread-safe Zoom Phone API v2 client with S2S OAuth token management."""

    def __init__(self, http_client: Optional[HTTPClient] = None):
        """Initialise client, loading credentials from vault.

        Args:
            http_client: Optional pre-configured HTTPClient. A new one with
                         configured timeout is created when omitted.

        Raises:
            ZoomAPIError: If vault credentials are missing.
        """
        cfg = _load_zoom_config()
        self._base_url = cfg['base_url'].rstrip('/')
        self._auth_url = cfg['auth_url']
        self._timeout = cfg['timeout']

        self._account_id = vault_config('ZOOM_ACCOUNT_ID')
        self._client_id = vault_config('ZOOM_CLIENT_ID')
        self._client_secret = vault_config('ZOOM_CLIENT_SECRET')
        if not all([self._account_id, self._client_id, self._client_secret]):
            raise ZoomAPIError(
                "Zoom credentials not configured. "
                "Set ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET in the vault."
            )

        self._http = http_client or HTTPClient(default_timeout=self._timeout)

        # Token state (guarded by lock for thread safety)
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Auth helpers (private)
    # ------------------------------------------------------------------

    def _ensure_auth(self) -> None:
        """Refresh the S2S OAuth token if expired or about to expire (30 s buffer)."""
        now = datetime.now(timezone.utc)
        with self._lock:
            if self._token and self._expires_at and now < self._expires_at - timedelta(seconds=30):
                return  # token still valid

            logger.info("Authenticating with Zoom API (account_credentials)")
            credentials = base64.b64encode(
                f"{self._client_id}:{self._client_secret}".encode()
            ).decode()
            try:
                response = self._http.post(
                    self._auth_url,
                    data={
                        'grant_type': 'account_credentials',
                        'account_id': self._account_id,
                    },
                    headers={
                        'Authorization': f'Basic {credentials}',
                        'Content-Type': 'application/x-www-form-urlencoded',
                    },
                )
                response.raise_for_status()
                token_data = response.json()
                self._token = token_data['access_token']
                ttl = int(token_data.get('expires_in', 3600))
                self._expires_at = now + timedelta(seconds=ttl)
                logger.info("Zoom API authenticated (token expires in %ds)", ttl)
            except Exception:
                logger.exception("Zoom API authentication failed")
                raise ZoomAPIError("Failed to authenticate with Zoom API")

    def _auth_headers(self) -> Dict[str, str]:
        """Return headers dict with current Bearer token."""
        self._ensure_auth()
        return {
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json',
        }

    # ------------------------------------------------------------------
    # Core request (with outbound tracking)
    # ------------------------------------------------------------------

    @track_outbound_api(
        service_name="zoom_phone",
        endpoint_extractor=lambda args, kwargs: kwargs.get('endpoint', args[2] if len(args) > 2 else 'unknown'),
    )
    def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        """Execute an authenticated Zoom API request.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE).
            endpoint: Relative path under base_url (e.g. 'phone/external_contacts').
            **kwargs: Passed through to HTTPClient.request().

        Returns:
            requests.Response object.

        Raises:
            ZoomAPIError: On HTTP or network failure.
        """
        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        headers = kwargs.pop('headers', {})
        headers.update(self._auth_headers())
        try:
            resp = self._http.request(method, url, headers=headers, **kwargs)
            return resp
        except Exception:
            logger.exception("Zoom API request failed: %s %s", method, endpoint)
            raise ZoomAPIError(f"Failed to call Zoom API: {method} {endpoint}")

    # ------------------------------------------------------------------
    # Pagination helper (private)
    # ------------------------------------------------------------------

    def _fetch_paginated(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        items_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all records from a next_page_token-paginated endpoint.

        Args:
            endpoint: Relative API path.
            params: Extra query parameters merged into every request.
            page_size: Items per page (default 100).
            items_key: JSON key containing the items list. Auto-detected if None.

        Returns:
            Aggregated list of item dicts across all pages.
        """
        all_items: List[Dict[str, Any]] = []
        next_page_token = None
        page = 0

        while True:
            page += 1
            if page > MAX_PAGES:
                logger.error(
                    "_fetch_paginated: hit page ceiling (%d) for %s",
                    MAX_PAGES, endpoint,
                )
                break

            req_params: Dict[str, Any] = {'page_size': page_size}
            if params:
                req_params.update(params)
            if next_page_token:
                req_params['next_page_token'] = next_page_token

            resp = self._request('GET', endpoint, params=req_params)
            data = resp.json()

            # Auto-detect items key from response
            key = items_key
            if not key:
                for candidate in ['external_contacts', 'call_logs', 'users',
                                  'recordings', 'meetings']:
                    if candidate in data:
                        key = candidate
                        break
            if not key:
                # Try first list-valued key
                for k, v in data.items():
                    if isinstance(v, list):
                        key = k
                        break

            items = data.get(key, []) if key else []
            if not items:
                break

            all_items.extend(items)
            logger.info(
                "  %s page %d: %d items (total: %d)",
                endpoint, page, len(items), len(all_items),
            )

            next_page_token = data.get('next_page_token', '')
            if not next_page_token:
                break

        return all_items

    # ------------------------------------------------------------------
    # External Contacts
    # ------------------------------------------------------------------

    def list_all_external_contacts(self) -> List[Dict[str, Any]]:
        """Fetch all external contacts from Zoom Phone."""
        return self._fetch_paginated('phone/external_contacts', items_key='external_contacts')

    def create_external_contact(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new external contact.

        Args:
            data: Contact payload (name, phone_numbers, email, description, etc.).

        Returns:
            Created contact dict.
        """
        resp = self._request('POST', 'phone/external_contacts', json=data)
        return resp.json()

    def update_external_contact(self, contact_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing external contact.

        Args:
            contact_id: Zoom external contact ID.
            data: Fields to update.

        Returns:
            Empty dict on 204 success, or response body.
        """
        resp = self._request('PATCH', f'phone/external_contacts/{contact_id}', json=data)
        # Zoom returns 204 on success for updates
        if resp.status_code == 204:
            return {}
        return resp.json() if resp.content else {}

    def delete_external_contact(self, contact_id: str) -> bool:
        """Delete an external contact.

        Args:
            contact_id: Zoom external contact ID.

        Returns:
            True if deletion succeeded (200 or 204).
        """
        resp = self._request('DELETE', f'phone/external_contacts/{contact_id}')
        return resp.status_code in (200, 204)

    # ------------------------------------------------------------------
    # Call Logs
    # ------------------------------------------------------------------

    def get_all_call_logs(
        self,
        from_date: datetime,
        to_date: datetime,
    ) -> List[Dict[str, Any]]:
        """Fetch call logs, handling 1-month max window by splitting into chunks.

        Args:
            from_date: Start of date range.
            to_date: End of date range.

        Returns:
            Aggregated list of call log dicts.
        """
        all_logs: List[Dict[str, Any]] = []
        current_start = from_date

        while current_start < to_date:
            # Max 30-day window per Zoom API constraint
            chunk_end = min(current_start + timedelta(days=30), to_date)
            params = {
                'from': current_start.strftime('%Y-%m-%d'),
                'to': chunk_end.strftime('%Y-%m-%d'),
            }
            logs = self._fetch_paginated(
                'phone/call_history', params=params, items_key='call_logs',
            )
            all_logs.extend(logs)
            current_start = chunk_end

        return all_logs

    def get_call_path(self, call_log_id: str) -> Dict[str, Any]:
        """Get detailed call path for a specific call log entry.

        Args:
            call_log_id: Zoom call log ID.

        Returns:
            Call path detail dict.
        """
        resp = self._request('GET', f'phone/call_history/{call_log_id}')
        return resp.json()

    # ------------------------------------------------------------------
    # Recordings
    # ------------------------------------------------------------------

    def get_recording_transcript(self, recording_id: str) -> Optional[str]:
        """Download transcript for a recording.

        Args:
            recording_id: Zoom recording ID.

        Returns:
            Transcript text, or None on failure.
        """
        try:
            resp = self._request(
                'GET', f'phone/recording/download/{recording_id}',
                params={'file_type': 'transcript'},
            )
            return resp.text
        except Exception:
            logger.warning("Failed to download transcript for recording %s", recording_id)
            return None

    # ------------------------------------------------------------------
    # Users
    # ------------------------------------------------------------------

    def list_phone_users(self) -> List[Dict[str, Any]]:
        """Fetch all Zoom Phone users."""
        return self._fetch_paginated('phone/users', items_key='users')
