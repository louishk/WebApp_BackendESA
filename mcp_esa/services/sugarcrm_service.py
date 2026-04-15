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
    platform: str = "mobile"
    api_version: str = "v11"
    timeout: int = 30

    def __post_init__(self):
        self.url = self.url.rstrip("/")

    @property
    def api_base(self) -> str:
        return f"{self.url}/rest/{self.api_version}"


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

    # ---------------- Module / id validation ----------------
    # Prevents URL-path injection from tool parameters.

    @staticmethod
    def _validate_module(module: str) -> str:
        import re
        if not isinstance(module, str) or not re.match(r'^[A-Za-z][A-Za-z0-9_]{0,63}$', module):
            raise SugarCRMAPIError("Invalid module name", code="bad_module")
        return module

    @staticmethod
    def _validate_id(record_id: str) -> str:
        import re
        if not isinstance(record_id, str) or not re.match(r'^[A-Za-z0-9_\-]{1,64}$', record_id):
            raise SugarCRMAPIError("Invalid record id", code="bad_id")
        return record_id

    @staticmethod
    def _normalize_fields(fields) -> Optional[str]:
        """Accept list[str], comma-separated str, or JSON-encoded list str; return comma-separated str or None."""
        if fields is None or fields == "":
            return None
        if isinstance(fields, list):
            return ",".join(str(f).strip() for f in fields if f)
        if isinstance(fields, str):
            s = fields.strip()
            if s.startswith("[") and s.endswith("]"):
                import json
                try:
                    parsed = json.loads(s)
                    if isinstance(parsed, list):
                        return ",".join(str(f).strip() for f in parsed if f)
                except ValueError:
                    pass
            return s
        raise SugarCRMAPIError("fields must be a list or comma-separated string", code="bad_fields")

    # ---------------- Record CRUD ----------------

    def get_record(self, module: str, record_id: str, fields=None) -> dict:
        module = self._validate_module(module)
        record_id = self._validate_id(record_id)
        params: Dict[str, Any] = {}
        f = self._normalize_fields(fields)
        if f:
            params["fields"] = f
        return self._request("GET", f"/{module}/{record_id}", params=params)

    def list_records(self, module: str, filter=None, fields=None, limit: int = 20,
                     offset: int = 0, order_by: Optional[str] = None) -> dict:
        """POST {module}/filter — Sugar v11 filter endpoint (matches common/sugarcrm_client)."""
        module = self._validate_module(module)
        params: Dict[str, Any] = {"max_num": int(limit), "offset": int(offset)}
        f = self._normalize_fields(fields)
        if f:
            params["fields"] = f
        if order_by:
            params["order_by"] = order_by
        body: Dict[str, Any] = {"deleted": False}
        if filter:
            # accept either a list of clauses or a JSON-encoded string
            if isinstance(filter, str):
                import json
                try:
                    filter = json.loads(filter)
                except ValueError:
                    raise SugarCRMAPIError("filter must be a list or JSON-encoded list", code="bad_filter")
            body["filter"] = filter
        return self._request("POST", f"/{module}/filter", params=params, json_body=body)

    def search(self, module: str, q: str, fields=None, limit: int = 20) -> dict:
        module = self._validate_module(module)
        params: Dict[str, Any] = {"q": q, "module_list": module, "max_num": int(limit)}
        f = self._normalize_fields(fields)
        if f:
            params["fields"] = f
        return self._request("GET", "/search", params=params)

    def create_record(self, module: str, data: dict) -> dict:
        module = self._validate_module(module)
        if not isinstance(data, dict) or not data:
            raise SugarCRMAPIError("data must be a non-empty dict", code="bad_data")
        return self._request("POST", f"/{module}", json_body=data)

    def update_record(self, module: str, record_id: str, data: dict) -> dict:
        module = self._validate_module(module)
        record_id = self._validate_id(record_id)
        if not isinstance(data, dict) or not data:
            raise SugarCRMAPIError("data must be a non-empty dict", code="bad_data")
        return self._request("PUT", f"/{module}/{record_id}", json_body=data)

    def delete_record(self, module: str, record_id: str) -> dict:
        module = self._validate_module(module)
        record_id = self._validate_id(record_id)
        return self._request("DELETE", f"/{module}/{record_id}")

    # ---------------- Relationships ----------------

    @staticmethod
    def _validate_link_name(link: str) -> str:
        import re
        if not isinstance(link, str) or not re.match(r'^[A-Za-z][A-Za-z0-9_]{0,63}$', link):
            raise SugarCRMAPIError("Invalid link name", code="bad_link")
        return link

    def get_related(self, module: str, record_id: str, link_name: str,
                    limit: int = 20, offset: int = 0, fields=None) -> dict:
        module = self._validate_module(module)
        record_id = self._validate_id(record_id)
        link_name = self._validate_link_name(link_name)
        params: Dict[str, Any] = {"max_num": int(limit), "offset": int(offset)}
        f = self._normalize_fields(fields)
        if f:
            params["fields"] = f
        return self._request("GET", f"/{module}/{record_id}/link/{link_name}", params=params)

    def link_records(self, module: str, record_id: str, link_name: str, related_id: str) -> dict:
        module = self._validate_module(module)
        record_id = self._validate_id(record_id)
        link_name = self._validate_link_name(link_name)
        related_id = self._validate_id(related_id)
        return self._request("POST", f"/{module}/{record_id}/link/{link_name}/{related_id}")

    def unlink_records(self, module: str, record_id: str, link_name: str, related_id: str) -> dict:
        module = self._validate_module(module)
        record_id = self._validate_id(record_id)
        link_name = self._validate_link_name(link_name)
        related_id = self._validate_id(related_id)
        return self._request("DELETE", f"/{module}/{record_id}/link/{link_name}/{related_id}")

    # ---------------- Studio / admin ----------------

    def list_modules(self) -> dict:
        return self._request("GET", "/metadata", params={"type_filter": "modules"})

    def list_fields(self, module: str) -> dict:
        """Fetch per-module metadata via /metadata?type_filter=modules&module_filter=X and return the module slice."""
        module = self._validate_module(module)
        result = self._request("GET", "/metadata",
                               params={"type_filter": "modules", "module_filter": module})
        return result.get("modules", {}).get(module, result)

    def get_field(self, module: str, field_name: str) -> dict:
        module = self._validate_module(module)
        field_name = self._validate_link_name(field_name)
        mod = self.list_fields(module)
        fields = mod.get("fields", {})
        if field_name not in fields:
            raise SugarCRMAPIError(f"field {field_name} not found on {module}", code="not_found")
        return fields[field_name]

    def create_field(self, module: str, spec: dict) -> dict:
        module = self._validate_module(module)
        if not isinstance(spec, dict) or not spec.get("name") or not spec.get("type"):
            raise SugarCRMAPIError("spec must include name and type", code="bad_spec")
        return self._request("POST", f"/Administration/fields/{module}", json_body=spec)

    def update_field(self, module: str, field_name: str, spec: dict) -> dict:
        module = self._validate_module(module)
        field_name = self._validate_link_name(field_name)
        if not isinstance(spec, dict) or not spec:
            raise SugarCRMAPIError("spec must be a non-empty dict", code="bad_spec")
        return self._request("PUT", f"/Administration/fields/{module}/{field_name}", json_body=spec)

    def delete_field(self, module: str, field_name: str) -> dict:
        module = self._validate_module(module)
        field_name = self._validate_link_name(field_name)
        return self._request("DELETE", f"/Administration/fields/{module}/{field_name}")

    def list_dropdowns(self) -> dict:
        return self._request("GET", "/Administration/dropdowns")

    def get_dropdown(self, name: str) -> dict:
        name = self._validate_link_name(name)
        return self._request("GET", f"/Administration/dropdowns/{name}")

    def update_dropdown(self, name: str, values: list) -> dict:
        name = self._validate_link_name(name)
        if not isinstance(values, list):
            raise SugarCRMAPIError("values must be a list", code="bad_values")
        return self._request("PUT", f"/Administration/dropdowns/{name}", json_body={"values": values})

    def create_relationship(self, spec: dict) -> dict:
        if not isinstance(spec, dict):
            raise SugarCRMAPIError("spec must be a dict", code="bad_spec")
        required = ("lhs_module", "rhs_module", "relationship_type")
        if not all(spec.get(k) for k in required):
            raise SugarCRMAPIError("spec must include lhs_module, rhs_module, relationship_type",
                                   code="bad_spec")
        self._validate_module(spec["lhs_module"])
        self._validate_module(spec["rhs_module"])
        return self._request("POST", "/Administration/relationships", json_body=spec)

    def delete_relationship(self, rel_name: str) -> dict:
        rel_name = self._validate_link_name(rel_name)
        return self._request("DELETE", f"/Administration/relationships/{rel_name}")

    def get_layout(self, module: str, view: str) -> dict:
        module = self._validate_module(module)
        view = self._validate_link_name(view)
        return self._request("GET", f"/metadata/modules/{module}/layouts/{view}")

    def update_layout(self, module: str, view: str, spec: dict) -> dict:
        module = self._validate_module(module)
        view = self._validate_link_name(view)
        if not isinstance(spec, dict) or not spec:
            raise SugarCRMAPIError("spec must be a non-empty dict", code="bad_spec")
        return self._request("PUT", f"/Administration/layouts/{module}/{view}", json_body=spec)

    def studio_deploy(self) -> dict:
        return self._request("POST", "/Administration/rebuild")

    # ---------------- Convenience ----------------

    def convert_lead(self, lead_id: str, convert_data: dict) -> dict:
        lead_id = self._validate_id(lead_id)
        if not isinstance(convert_data, dict) or not convert_data:
            raise SugarCRMAPIError("convert_data must be a non-empty dict", code="bad_data")
        return self._request("POST", f"/Leads/{lead_id}/convert", json_body=convert_data)
