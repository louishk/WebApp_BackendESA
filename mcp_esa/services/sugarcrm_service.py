"""
SugarCRM Service Module

Self-contained REST v11 client for the ESA SugarCRM tenant.
Handles OAuth2 password-grant auth, token refresh, request retries,
and exposes CRUD + relationship + Studio methods consumed by MCP tools.
"""
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass
class SugarCRMConfig:
    url: str
    username: str
    password: str
    client_id: str = "sugar"
    client_secret: str = ""
    platform: str = "mcp_esa"
    timeout: int = 30

    def __post_init__(self):
        self.url = self.url.rstrip("/")

    @property
    def api_base(self) -> str:
        return f"{self.url}/rest/v11_20"


class SugarCRMAPIError(Exception):
    """Raised when the SugarCRM API returns an error."""

    def __init__(self, message: str, code: Optional[str] = None, details: Optional[dict] = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


class SugarCRMService:
    """Thin REST v11 wrapper used by the MCP SugarCRM tools."""

    def __init__(self, config: SugarCRMConfig):
        self.config = config
        self._client = httpx.Client(timeout=config.timeout)
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0

    # ---------------- Auth ----------------

    def _ensure_token(self) -> None:
        now = time.time()
        if self._access_token and now < self._token_expires_at - 30:
            return
        payload = {
            "grant_type": "password",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "username": self.config.username,
            "password": self.config.password,
            "platform": self.config.platform,
        }
        resp = self._client.post(f"{self.config.api_base}/oauth2/token", json=payload)
        if resp.status_code != 200:
            logger.error("SugarCRM auth failed: status=%s body=%s", resp.status_code, resp.text)
            raise SugarCRMAPIError("SugarCRM authentication failed", code="auth_failed")
        data = resp.json()
        self._access_token = data.get("access_token")
        self._token_expires_at = now + int(data.get("expires_in", 3600))

    def _headers(self) -> Dict[str, str]:
        return {"OAuth-Token": self._access_token or "", "Content-Type": "application/json"}

    def _request(self, method: str, path: str, params: Optional[dict] = None,
                 json_body: Optional[dict] = None, _retry: bool = True) -> Any:
        self._ensure_token()
        url = f"{self.config.api_base}{path}"
        try:
            resp = self._client.request(method, url, params=params, json=json_body, headers=self._headers())
        except httpx.RequestError as e:
            logger.exception("SugarCRM network error on %s %s", method, path)
            raise SugarCRMAPIError("SugarCRM network error", code="network_error") from e

        if resp.status_code == 401 and _retry:
            # token may have expired server-side; force refresh and retry once
            self._access_token = None
            self._token_expires_at = 0.0
            return self._request(method, path, params=params, json_body=json_body, _retry=False)

        if resp.status_code >= 400:
            try:
                body = resp.json()
                code = body.get("error")
                detail = body.get("error_message") or body.get("error_description")
            except Exception:
                code, detail = None, resp.text[:200]
            logger.error("SugarCRM API error %s on %s %s: code=%s detail=%s",
                         resp.status_code, method, path, code, detail)
            raise SugarCRMAPIError("SugarCRM API error", code=code or f"http_{resp.status_code}",
                                   details={"http_status": resp.status_code})

        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()

    def close(self):
        try:
            self._client.close()
        except Exception:
            pass
