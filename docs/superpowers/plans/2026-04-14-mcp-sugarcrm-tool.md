# MCP SugarCRM Tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add SugarCRM operations (record CRUD + relationships + Studio admin) to the independent `mcp_esa/` MCP server, following the existing Google Ads pattern.

**Architecture:** A self-contained `SugarCRMService` class wraps the REST v11 API (no dependency on Flask's `common/sugarcrm_client.py`). A tools module registers ~31 MCP tool functions grouped into three conventional scope tiers (`sugarcrm_read`, `sugarcrm_write`, `sugarcrm_admin`). RBAC is enforced by the existing per-tool `mcp_tools` allowlist on `api_keys`; the scope tiers serve as documented grant bundles. Config mirrors Google Ads — non-secret fields in `mcp.yaml`, secrets resolved from the DB vault.

**Tech Stack:** Python 3, `httpx` (already in MCP deps), `dataclasses`, SugarCRM REST v11, MCP SDK (`mcp.server.Server`), existing `common.config_loader` vault resolver.

**Spec:** `docs/superpowers/specs/2026-04-14-mcp-sugarcrm-tool-design.md`

---

## File Structure

**Create:**
- `mcp_esa/services/sugarcrm_service.py` — service class, config dataclass, error type, ~22 methods
- `mcp_esa/tools/sugarcrm_tools.py` — 31 tool functions + `register_sugarcrm_tools(server, app)`
- `mcp_esa/tests/__init__.py` (if missing)
- `mcp_esa/tests/test_sugarcrm_service.py` — unit tests with mocked httpx
- `mcp_esa/tests/smoke_sugarcrm.py` — manual smoke test script

**Modify:**
- `backend/python/config/mcp.yaml` — add `features.sugarcrm: true` and a `sugarcrm:` section
- `mcp_esa/config/settings.py` — add `sugarcrm_*` properties
- `mcp_esa/server/mcp_server.py` — register sugarcrm tools behind the feature flag
- `docs/api/` — add `sugarcrm_mcp.md` documentation entry

---

## Task 1: Add config section and settings properties

**Files:**
- Modify: `backend/python/config/mcp.yaml`
- Modify: `mcp_esa/config/settings.py`

- [ ] **Step 1: Add sugarcrm feature flag and config section to mcp.yaml**

Edit `backend/python/config/mcp.yaml`. Find the `features:` block and add `sugarcrm: true`. Then add a new section after the Google Ads block:

```yaml
# Add this line to the existing features: block
features:
  database: true
  google_ads: true
  revenue: true
  naver_searchad: true
  sugarcrm: true   # <-- new

# =============================================================================
# SugarCRM REST v11 API
# =============================================================================
# Non-secret fields live here; password + client_secret resolved from vault.
sugarcrm:
  url: "https://extraspaceasia.sugarondemand.com"
  username: "mcp_service"
  client_id: "sugar"
  platform: "mcp_esa"
  timeout: 30
  password_vault: "SUGARCRM_PASSWORD"
  client_secret_vault: "SUGARCRM_CLIENT_SECRET"
```

Note: the `username` value may need to be the actual service account in the tenant — the user will update the placeholder post-deploy via `/admin/secrets` or by editing mcp.yaml.

- [ ] **Step 2: Add sugarcrm properties to Settings class**

Edit `mcp_esa/config/settings.py`. In `Settings.__init__`, after `self._gads = ...`, add:

```python
        self._sugarcrm = self._mcp.get('sugarcrm', {})
```

Then add these properties after the Google Ads block (keep the same style as existing properties):

```python
    # SugarCRM feature flag
    @property
    def sugarcrm_enabled(self) -> bool:
        return self._features.get('sugarcrm', False)

    # SugarCRM (non-secret fields)
    @property
    def sugarcrm_url(self) -> str:
        return self._sugarcrm.get('url', '')

    @property
    def sugarcrm_username(self) -> str:
        return self._sugarcrm.get('username', '')

    @property
    def sugarcrm_client_id(self) -> str:
        return self._sugarcrm.get('client_id', 'sugar')

    @property
    def sugarcrm_platform(self) -> str:
        return self._sugarcrm.get('platform', 'mcp_esa')

    @property
    def sugarcrm_timeout(self) -> int:
        return int(self._sugarcrm.get('timeout', 30))

    # SugarCRM secrets (resolved from vault)
    @property
    def sugarcrm_password(self) -> str:
        vault_key = self._sugarcrm.get('password_vault', 'SUGARCRM_PASSWORD')
        return self._config.get_secret(vault_key) or ''

    @property
    def sugarcrm_client_secret(self) -> str:
        vault_key = self._sugarcrm.get('client_secret_vault', 'SUGARCRM_CLIENT_SECRET')
        return self._config.get_secret(vault_key) or ''
```

- [ ] **Step 3: Verify settings load**

Run:
```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -c "
from mcp_esa.config.settings import get_settings
s = get_settings()
print('enabled:', s.sugarcrm_enabled)
print('url:', s.sugarcrm_url)
print('username:', s.sugarcrm_username)
print('has password:', bool(s.sugarcrm_password))
print('has client_secret:', bool(s.sugarcrm_client_secret))
"
```

Expected: `enabled: True`, `url: https://...`, `has password: True`, `has client_secret: True`.

- [ ] **Step 4: Commit**

```bash
git add backend/python/config/mcp.yaml mcp_esa/config/settings.py
git commit -m "feat(mcp): add sugarcrm config section and settings properties"
```

---

## Task 2: Service skeleton — config, errors, auth

**Files:**
- Create: `mcp_esa/services/sugarcrm_service.py`
- Create: `mcp_esa/tests/__init__.py` (if missing)
- Create: `mcp_esa/tests/test_sugarcrm_service.py`

- [ ] **Step 1: Create tests/__init__.py if missing**

```bash
touch /home/louis/PycharmProjects/WebApp_BackendESA/mcp_esa/tests/__init__.py
```

- [ ] **Step 2: Write failing auth test**

Create `mcp_esa/tests/test_sugarcrm_service.py`:

```python
"""Unit tests for SugarCRMService. Uses mocked httpx to avoid real API calls."""
import pytest
from unittest.mock import MagicMock, patch

from mcp_esa.services.sugarcrm_service import (
    SugarCRMService, SugarCRMConfig, SugarCRMAPIError
)


def _make_config():
    return SugarCRMConfig(
        url="https://sugar.example.com",
        username="u",
        password="p",
        client_id="sugar",
        client_secret="",
        platform="mcp_esa",
        timeout=5,
    )


def _mock_response(json_body, status=200):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_body
    r.text = str(json_body)
    return r


def test_ensure_token_calls_oauth_and_caches():
    svc = SugarCRMService(_make_config())
    with patch.object(svc._client, 'post') as post:
        post.return_value = _mock_response({
            "access_token": "AT", "expires_in": 3600, "refresh_token": "RT"
        })
        svc._ensure_token()
        svc._ensure_token()  # cached, should not call again
    assert post.call_count == 1
    assert svc._access_token == "AT"
```

- [ ] **Step 3: Run test to confirm it fails**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -m pytest mcp_esa/tests/test_sugarcrm_service.py::test_ensure_token_calls_oauth_and_caches -v
```

Expected: FAIL with `ModuleNotFoundError: mcp_esa.services.sugarcrm_service`.

- [ ] **Step 4: Create the service skeleton**

Create `mcp_esa/services/sugarcrm_service.py`:

```python
"""
SugarCRM Service Module

Self-contained REST v11 client for the ESA SugarCRM tenant.
Handles OAuth2 password-grant auth, token refresh, request retries,
and exposes CRUD + relationship + Studio methods consumed by MCP tools.
"""
import logging
import time
from dataclasses import dataclass, field
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
        self._refresh_token: Optional[str] = None
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
        self._refresh_token = data.get("refresh_token")
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
```

- [ ] **Step 5: Run test to confirm it passes**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -m pytest mcp_esa/tests/test_sugarcrm_service.py::test_ensure_token_calls_oauth_and_caches -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add mcp_esa/services/sugarcrm_service.py mcp_esa/tests/__init__.py mcp_esa/tests/test_sugarcrm_service.py
git commit -m "feat(mcp): add SugarCRMService skeleton with oauth2 auth"
```

---

## Task 3: Service — record CRUD methods

**Files:**
- Modify: `mcp_esa/services/sugarcrm_service.py`
- Modify: `mcp_esa/tests/test_sugarcrm_service.py`

- [ ] **Step 1: Write failing test for list/get/create/update/delete**

Append to `mcp_esa/tests/test_sugarcrm_service.py`:

```python
def _service_with_token():
    svc = SugarCRMService(_make_config())
    svc._access_token = "AT"
    svc._token_expires_at = time.time() + 3600
    return svc


import time


def test_get_record_hits_correct_path():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"id": "abc", "name": "Acme"})
        out = svc.get_record("Accounts", "abc", fields=["name"])
    args, kwargs = req.call_args
    assert args[0] == "GET"
    assert args[1].endswith("/Accounts/abc")
    assert kwargs["params"] == {"fields": "name"}
    assert out["name"] == "Acme"


def test_list_records_passes_filter_and_paging():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"records": [], "next_offset": -1})
        svc.list_records("Leads", filter=[{"status": "New"}], limit=50, offset=0,
                         fields=["first_name", "last_name"], order_by="date_entered:desc")
    _, kwargs = req.call_args
    p = kwargs["params"]
    assert p["max_num"] == 50
    assert p["offset"] == 0
    assert p["fields"] == "first_name,last_name"
    assert p["order_by"] == "date_entered:desc"


def test_create_record_posts_json():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"id": "new1"})
        out = svc.create_record("Contacts", {"first_name": "A"})
    args, kwargs = req.call_args
    assert args[0] == "POST"
    assert args[1].endswith("/Contacts")
    assert kwargs["json"] == {"first_name": "A"}
    assert out["id"] == "new1"


def test_delete_record_issues_delete():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({}, status=204)
        svc.delete_record("Leads", "L1")
    args, _ = req.call_args
    assert args[0] == "DELETE"
    assert args[1].endswith("/Leads/L1")


def test_search_uses_global_endpoint():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"records": []})
        svc.search("Contacts", q="john@example.com", limit=10)
    _, kwargs = req.call_args
    p = kwargs["params"]
    assert p["q"] == "john@example.com"
    assert p["module_list"] == "Contacts"
    assert p["max_num"] == 10
```

- [ ] **Step 2: Run and confirm failures**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -m pytest mcp_esa/tests/test_sugarcrm_service.py -v
```

Expected: 5 FAIL with `AttributeError: 'SugarCRMService' object has no attribute 'get_record'`, etc.

- [ ] **Step 3: Add module-name validation helper and CRUD methods**

Append to `mcp_esa/services/sugarcrm_service.py` (inside the `SugarCRMService` class):

```python
    # ---------------- Module validation ----------------

    _MODULE_NAME_RE = None  # lazy compiled

    @staticmethod
    def _validate_module(module: str) -> str:
        """Reject anything not a valid SugarCRM module identifier."""
        import re
        if not module or not re.match(r'^[A-Za-z][A-Za-z0-9_]{0,63}$', module):
            raise SugarCRMAPIError("Invalid module name", code="bad_module")
        return module

    @staticmethod
    def _validate_id(record_id: str) -> str:
        import re
        if not record_id or not re.match(r'^[A-Za-z0-9_\-]{1,64}$', record_id):
            raise SugarCRMAPIError("Invalid record id", code="bad_id")
        return record_id

    # ---------------- Record CRUD ----------------

    def get_record(self, module: str, record_id: str, fields: Optional[List[str]] = None) -> dict:
        module = self._validate_module(module)
        record_id = self._validate_id(record_id)
        params = {}
        if fields:
            params["fields"] = ",".join(fields)
        return self._request("GET", f"/{module}/{record_id}", params=params)

    def list_records(self, module: str, filter: Optional[List[dict]] = None,
                     fields: Optional[List[str]] = None, limit: int = 20, offset: int = 0,
                     order_by: Optional[str] = None) -> dict:
        module = self._validate_module(module)
        params: Dict[str, Any] = {"max_num": int(limit), "offset": int(offset)}
        if fields:
            params["fields"] = ",".join(fields)
        if order_by:
            params["order_by"] = order_by
        if filter:
            # SugarCRM v11 filter format — caller supplies a list of filter clauses
            for i, clause in enumerate(filter):
                for k, v in clause.items():
                    params[f"filter[{i}][{k}]"] = v
        return self._request("GET", f"/{module}", params=params)

    def search(self, module: str, q: str, fields: Optional[List[str]] = None, limit: int = 20) -> dict:
        module = self._validate_module(module)
        params: Dict[str, Any] = {"q": q, "module_list": module, "max_num": int(limit)}
        if fields:
            params["fields"] = ",".join(fields)
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
```

- [ ] **Step 4: Run tests**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -m pytest mcp_esa/tests/test_sugarcrm_service.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add mcp_esa/services/sugarcrm_service.py mcp_esa/tests/test_sugarcrm_service.py
git commit -m "feat(mcp): add SugarCRM record CRUD + search methods"
```

---

## Task 4: Service — relationships

**Files:**
- Modify: `mcp_esa/services/sugarcrm_service.py`
- Modify: `mcp_esa/tests/test_sugarcrm_service.py`

- [ ] **Step 1: Write failing tests**

Append to `mcp_esa/tests/test_sugarcrm_service.py`:

```python
def test_get_related_uses_link_path():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"records": []})
        svc.get_related("Accounts", "A1", "contacts", limit=5)
    args, kwargs = req.call_args
    assert args[0] == "GET"
    assert args[1].endswith("/Accounts/A1/link/contacts")
    assert kwargs["params"]["max_num"] == 5


def test_link_records_posts_related_id():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({})
        svc.link_records("Accounts", "A1", "contacts", "C1")
    args, kwargs = req.call_args
    assert args[0] == "POST"
    assert args[1].endswith("/Accounts/A1/link/contacts/C1")


def test_unlink_records_deletes():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({})
        svc.unlink_records("Accounts", "A1", "contacts", "C1")
    args, _ = req.call_args
    assert args[0] == "DELETE"
    assert args[1].endswith("/Accounts/A1/link/contacts/C1")
```

- [ ] **Step 2: Run tests to confirm failures**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -m pytest mcp_esa/tests/test_sugarcrm_service.py -v -k related or link
```

Expected: FAIL on all three new tests.

- [ ] **Step 3: Add relationship methods**

Append to `mcp_esa/services/sugarcrm_service.py` inside `SugarCRMService`:

```python
    # ---------------- Relationships ----------------

    @staticmethod
    def _validate_link_name(link: str) -> str:
        import re
        if not link or not re.match(r'^[A-Za-z][A-Za-z0-9_]{0,63}$', link):
            raise SugarCRMAPIError("Invalid link name", code="bad_link")
        return link

    def get_related(self, module: str, record_id: str, link_name: str,
                    limit: int = 20, offset: int = 0,
                    fields: Optional[List[str]] = None) -> dict:
        module = self._validate_module(module)
        record_id = self._validate_id(record_id)
        link_name = self._validate_link_name(link_name)
        params: Dict[str, Any] = {"max_num": int(limit), "offset": int(offset)}
        if fields:
            params["fields"] = ",".join(fields)
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
```

- [ ] **Step 4: Run tests**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -m pytest mcp_esa/tests/test_sugarcrm_service.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add mcp_esa/services/sugarcrm_service.py mcp_esa/tests/test_sugarcrm_service.py
git commit -m "feat(mcp): add SugarCRM relationship methods"
```

---

## Task 5: Service — Studio / admin methods

**Files:**
- Modify: `mcp_esa/services/sugarcrm_service.py`
- Modify: `mcp_esa/tests/test_sugarcrm_service.py`

- [ ] **Step 1: Write failing tests**

Append to `mcp_esa/tests/test_sugarcrm_service.py`:

```python
def test_list_modules_hits_metadata():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"modules": {"Accounts": {}}})
        svc.list_modules()
    args, kwargs = req.call_args
    assert args[0] == "GET"
    assert args[1].endswith("/metadata")
    assert kwargs["params"]["type_filter"] == "modules"


def test_list_fields_hits_module_fields():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"fields": {}})
        svc.list_fields("Accounts")
    args, _ = req.call_args
    assert args[1].endswith("/metadata/modules/Accounts")


def test_create_field_posts_spec():
    svc = _service_with_token()
    spec = {"name": "c_score_c", "type": "int", "label": "Score"}
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"name": "c_score_c"})
        svc.create_field("Leads", spec)
    args, kwargs = req.call_args
    assert args[0] == "POST"
    assert args[1].endswith("/Administration/fields/Leads")
    assert kwargs["json"] == spec


def test_studio_deploy_calls_rebuild():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"success": True})
        svc.studio_deploy()
    args, _ = req.call_args
    assert args[0] == "POST"
    assert args[1].endswith("/Administration/rebuild")
```

- [ ] **Step 2: Run tests, confirm failures**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -m pytest mcp_esa/tests/test_sugarcrm_service.py -v
```

Expected: 4 new tests FAIL.

- [ ] **Step 3: Add Studio methods**

> **Note:** SugarCRM's Studio REST endpoints are lightly documented outside Sugar Cloud tenants. Paths used below match the `/Administration/*` pattern the SMD tenant exposes. If the tenant rejects any endpoint, the implementer should check `/rest/v11_20/help` on the tenant for the correct path and update in-place — this is an acceptable deviation from the plan.

Append to `mcp_esa/services/sugarcrm_service.py` inside `SugarCRMService`:

```python
    # ---------------- Studio / admin ----------------

    def list_modules(self) -> dict:
        return self._request("GET", "/metadata", params={"type_filter": "modules"})

    def list_fields(self, module: str) -> dict:
        module = self._validate_module(module)
        return self._request("GET", f"/metadata/modules/{module}")

    def get_field(self, module: str, field_name: str) -> dict:
        module = self._validate_module(module)
        field_name = self._validate_link_name(field_name)
        return self._request("GET", f"/Administration/fields/{module}/{field_name}")

    def create_field(self, module: str, spec: dict) -> dict:
        module = self._validate_module(module)
        if not isinstance(spec, dict) or not spec.get("name") or not spec.get("type"):
            raise SugarCRMAPIError("spec must include name and type", code="bad_spec")
        return self._request("POST", f"/Administration/fields/{module}", json_body=spec)

    def update_field(self, module: str, field_name: str, spec: dict) -> dict:
        module = self._validate_module(module)
        field_name = self._validate_link_name(field_name)
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
        required = ("lhs_module", "rhs_module", "relationship_type")
        if not all(spec.get(k) for k in required):
            raise SugarCRMAPIError(f"spec must include {required}", code="bad_spec")
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
        return self._request("PUT", f"/Administration/layouts/{module}/{view}", json_body=spec)

    def studio_deploy(self) -> dict:
        return self._request("POST", "/Administration/rebuild")
```

- [ ] **Step 4: Run tests**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -m pytest mcp_esa/tests/test_sugarcrm_service.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add mcp_esa/services/sugarcrm_service.py mcp_esa/tests/test_sugarcrm_service.py
git commit -m "feat(mcp): add SugarCRM Studio/admin methods"
```

---

## Task 6: Tools registration scaffold + read tier (14 tools)

**Files:**
- Create: `mcp_esa/tools/sugarcrm_tools.py`

- [ ] **Step 1: Look at an existing tool module for the registration pattern**

Read `mcp_esa/tools/google_ads_tools.py` — note how `register_google_ads_tools(server, app)` is structured, how each tool is a nested function decorated with `@server.call_tool()` (or the current pattern), and how errors are returned.

```bash
head -120 /home/louis/PycharmProjects/WebApp_BackendESA/mcp_esa/tools/google_ads_tools.py
```

- [ ] **Step 2: Create the sugarcrm tools module with registration + read tier**

Create `mcp_esa/tools/sugarcrm_tools.py`:

```python
"""
MCP SugarCRM Tools

Registers ~31 tool functions against the MCP server. Grouped into three
conventional scope tiers — enforcement is handled by the existing per-tool
mcp_tools allowlist on api_keys (transport-level filtering). The tier
grouping is documentation for admins granting bundles of tools.

Tiers:
  sugarcrm_read  — SC_get_*, SC_list_*, SC_search*
  sugarcrm_write — SC_create_*, SC_update_*, SC_delete_* (records), SC_link_*, SC_unlink_*
  sugarcrm_admin — SC_*_field, SC_update_dropdown, SC_*_relationship, SC_update_layout, SC_studio_deploy
"""
import logging
from typing import Any, Dict, List, Optional

from mcp_esa.services.sugarcrm_service import (
    SugarCRMService, SugarCRMConfig, SugarCRMAPIError,
)

logger = logging.getLogger(__name__)

_service: Optional[SugarCRMService] = None


def _get_service(app) -> SugarCRMService:
    global _service
    if _service is not None:
        return _service
    s = app.settings
    cfg = SugarCRMConfig(
        url=s.sugarcrm_url,
        username=s.sugarcrm_username,
        password=s.sugarcrm_password,
        client_id=s.sugarcrm_client_id,
        client_secret=s.sugarcrm_client_secret,
        platform=s.sugarcrm_platform,
        timeout=s.sugarcrm_timeout,
    )
    _service = SugarCRMService(cfg)
    return _service


def _ok(data: Any) -> Dict[str, Any]:
    return {"status": "success", "data": data}


def _err(msg: str, code: Optional[str] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {"status": "error", "error": msg}
    if code:
        out["code"] = code
    return out


def _safe(fn):
    """Wrap a tool body: catch SugarCRMAPIError and return generic error."""
    def wrapper(*args, **kwargs):
        try:
            return _ok(fn(*args, **kwargs))
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM tool error: %s (code=%s details=%s)", e, e.code, e.details)
            return _err("SugarCRM operation failed", code=e.code)
        except Exception as e:
            logger.exception("Unexpected error in SugarCRM tool")
            return _err("Unexpected error")
    wrapper.__name__ = fn.__name__
    return wrapper


def register_sugarcrm_tools(server, app) -> None:
    """Register all SugarCRM tools with the MCP server."""

    # ==========================================================
    # Read tier (sugarcrm_read)
    # ==========================================================

    @server.tool(name="SC_get_record", description="Get a single SugarCRM record by module and id.")
    @_safe
    def SC_get_record(module: str, record_id: str, fields: Optional[List[str]] = None):
        return _get_service(app).get_record(module, record_id, fields)

    @server.tool(name="SC_list_records", description="List records in a SugarCRM module with optional filter and paging.")
    @_safe
    def SC_list_records(module: str, filter: Optional[List[dict]] = None,
                        fields: Optional[List[str]] = None, limit: int = 20,
                        offset: int = 0, order_by: Optional[str] = None):
        return _get_service(app).list_records(module, filter, fields, limit, offset, order_by)

    @server.tool(name="SC_search", description="Full-text search across a SugarCRM module.")
    @_safe
    def SC_search(module: str, q: str, fields: Optional[List[str]] = None, limit: int = 20):
        return _get_service(app).search(module, q, fields, limit)

    @server.tool(name="SC_get_related", description="Get related records via a link name (e.g. Accounts.contacts).")
    @_safe
    def SC_get_related(module: str, record_id: str, link_name: str,
                       limit: int = 20, offset: int = 0,
                       fields: Optional[List[str]] = None):
        return _get_service(app).get_related(module, record_id, link_name, limit, offset, fields)

    @server.tool(name="SC_list_modules", description="List all SugarCRM modules (stock + custom).")
    @_safe
    def SC_list_modules():
        return _get_service(app).list_modules()

    @server.tool(name="SC_list_fields", description="List all fields for a SugarCRM module.")
    @_safe
    def SC_list_fields(module: str):
        return _get_service(app).list_fields(module)

    @server.tool(name="SC_get_field", description="Get metadata for a single field on a module.")
    @_safe
    def SC_get_field(module: str, field_name: str):
        return _get_service(app).get_field(module, field_name)

    @server.tool(name="SC_list_dropdowns", description="List all global dropdown lists.")
    @_safe
    def SC_list_dropdowns():
        return _get_service(app).list_dropdowns()

    @server.tool(name="SC_get_dropdown", description="Get values for a single dropdown list.")
    @_safe
    def SC_get_dropdown(name: str):
        return _get_service(app).get_dropdown(name)

    @server.tool(name="SC_get_layout", description="Get a module layout (view: edit, detail, list, search).")
    @_safe
    def SC_get_layout(module: str, view: str):
        return _get_service(app).get_layout(module, view)

    # Convenience read tools — thin wrappers for common modules
    @server.tool(name="SC_get_lead", description="Shortcut: get a Lead by id.")
    @_safe
    def SC_get_lead(record_id: str, fields: Optional[List[str]] = None):
        return _get_service(app).get_record("Leads", record_id, fields)

    @server.tool(name="SC_get_contact", description="Shortcut: get a Contact by id.")
    @_safe
    def SC_get_contact(record_id: str, fields: Optional[List[str]] = None):
        return _get_service(app).get_record("Contacts", record_id, fields)

    @server.tool(name="SC_get_account", description="Shortcut: get an Account by id.")
    @_safe
    def SC_get_account(record_id: str, fields: Optional[List[str]] = None):
        return _get_service(app).get_record("Accounts", record_id, fields)

    @server.tool(name="SC_search_by_email", description="Search Contacts and Leads for a given email address.")
    @_safe
    def SC_search_by_email(email: str, limit: int = 20):
        svc = _get_service(app)
        return {
            "contacts": svc.search("Contacts", q=email, limit=limit),
            "leads": svc.search("Leads", q=email, limit=limit),
        }

    logger.info("SugarCRM read-tier tools registered (14)")
```

> **Note:** If `@server.tool(name=..., description=...)` is NOT the actual decorator signature in this MCP SDK version, match the pattern used in `google_ads_tools.py`. Use whatever is there. Do not invent a different API.

- [ ] **Step 3: Sanity import**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -c "from mcp_esa.tools import sugarcrm_tools; print('ok')"
```

Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add mcp_esa/tools/sugarcrm_tools.py
git commit -m "feat(mcp): add SugarCRM read-tier tools (14 tools)"
```

---

## Task 7: Tools — write tier (8 tools)

**Files:**
- Modify: `mcp_esa/tools/sugarcrm_tools.py`

- [ ] **Step 1: Append write-tier tools**

Add the following block to `mcp_esa/tools/sugarcrm_tools.py` inside `register_sugarcrm_tools`, just before the final `logger.info(...)`:

```python
    # ==========================================================
    # Write tier (sugarcrm_write)
    # ==========================================================

    @server.tool(name="SC_create_record", description="Create a new record in a SugarCRM module.")
    @_safe
    def SC_create_record(module: str, data: dict):
        return _get_service(app).create_record(module, data)

    @server.tool(name="SC_update_record", description="Update fields on a SugarCRM record.")
    @_safe
    def SC_update_record(module: str, record_id: str, data: dict):
        return _get_service(app).update_record(module, record_id, data)

    @server.tool(name="SC_delete_record", description="Delete a SugarCRM record. Requires confirm=True.")
    @_safe
    def SC_delete_record(module: str, record_id: str, confirm: bool = False):
        if not confirm:
            raise SugarCRMAPIError("confirm=True required for delete", code="confirm_required")
        return _get_service(app).delete_record(module, record_id)

    @server.tool(name="SC_link_records", description="Link a related record via a link name.")
    @_safe
    def SC_link_records(module: str, record_id: str, link_name: str, related_id: str):
        return _get_service(app).link_records(module, record_id, link_name, related_id)

    @server.tool(name="SC_unlink_records", description="Unlink a related record.")
    @_safe
    def SC_unlink_records(module: str, record_id: str, link_name: str, related_id: str):
        return _get_service(app).unlink_records(module, record_id, link_name, related_id)

    @server.tool(name="SC_create_lead", description="Shortcut: create a Lead.")
    @_safe
    def SC_create_lead(data: dict):
        return _get_service(app).create_record("Leads", data)

    @server.tool(name="SC_convert_lead", description="Convert a Lead into a Contact + Account via the convert endpoint.")
    @_safe
    def SC_convert_lead(lead_id: str, convert_data: dict):
        # Sugar v11 lead conversion — uses a dedicated endpoint
        svc = _get_service(app)
        return svc._request("POST", f"/Leads/{svc._validate_id(lead_id)}/convert", json_body=convert_data)

    @server.tool(name="SC_log_call", description="Shortcut: log a Call related to an Account/Contact/Lead.")
    @_safe
    def SC_log_call(data: dict):
        return _get_service(app).create_record("Calls", data)

    logger.info("SugarCRM write-tier tools registered (8)")
```

Also update the final `logger.info("SugarCRM read-tier tools registered (14)")` line — the read-tier log message stays where it was; the write-tier message is added in this task.

- [ ] **Step 2: Sanity import**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -c "from mcp_esa.tools import sugarcrm_tools; print('ok')"
```

Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add mcp_esa/tools/sugarcrm_tools.py
git commit -m "feat(mcp): add SugarCRM write-tier tools (8 tools)"
```

---

## Task 8: Tools — admin tier (9 tools)

**Files:**
- Modify: `mcp_esa/tools/sugarcrm_tools.py`

- [ ] **Step 1: Append admin-tier tools**

Add this block inside `register_sugarcrm_tools`, after the write-tier block:

```python
    # ==========================================================
    # Admin tier (sugarcrm_admin) — Studio operations
    # ==========================================================

    @server.tool(name="SC_create_field", description="Create a custom field on a SugarCRM module. spec: {name, type, label, ...}")
    @_safe
    def SC_create_field(module: str, spec: dict):
        return _get_service(app).create_field(module, spec)

    @server.tool(name="SC_update_field", description="Update a custom field's spec.")
    @_safe
    def SC_update_field(module: str, field_name: str, spec: dict):
        return _get_service(app).update_field(module, field_name, spec)

    @server.tool(name="SC_delete_field", description="Delete a custom field. Requires confirm=True.")
    @_safe
    def SC_delete_field(module: str, field_name: str, confirm: bool = False):
        if not confirm:
            raise SugarCRMAPIError("confirm=True required for delete", code="confirm_required")
        return _get_service(app).delete_field(module, field_name)

    @server.tool(name="SC_update_dropdown", description="Replace the values of a global dropdown list.")
    @_safe
    def SC_update_dropdown(name: str, values: list):
        return _get_service(app).update_dropdown(name, values)

    @server.tool(name="SC_create_relationship", description="Create a module relationship. spec: {lhs_module, rhs_module, relationship_type, label}")
    @_safe
    def SC_create_relationship(spec: dict):
        return _get_service(app).create_relationship(spec)

    @server.tool(name="SC_delete_relationship", description="Delete a module relationship by name. Requires confirm=True.")
    @_safe
    def SC_delete_relationship(rel_name: str, confirm: bool = False):
        if not confirm:
            raise SugarCRMAPIError("confirm=True required for delete", code="confirm_required")
        return _get_service(app).delete_relationship(rel_name)

    @server.tool(name="SC_update_layout", description="Update a module layout. view: edit, detail, list, search.")
    @_safe
    def SC_update_layout(module: str, view: str, spec: dict):
        return _get_service(app).update_layout(module, view, spec)

    @server.tool(name="SC_studio_deploy", description="Rebuild metadata to deploy pending Studio changes. Requires confirm=True.")
    @_safe
    def SC_studio_deploy(confirm: bool = False):
        if not confirm:
            raise SugarCRMAPIError("confirm=True required for studio rebuild", code="confirm_required")
        return _get_service(app).studio_deploy()

    @server.tool(name="SC_list_fields_admin", description="Admin-scoped field listing, same data as SC_list_fields — included in admin tier for key scoping.")
    @_safe
    def SC_list_fields_admin(module: str):
        return _get_service(app).list_fields(module)

    logger.info("SugarCRM admin-tier tools registered (9)")
```

- [ ] **Step 2: Sanity import**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -c "from mcp_esa.tools import sugarcrm_tools; print('ok')"
```

Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add mcp_esa/tools/sugarcrm_tools.py
git commit -m "feat(mcp): add SugarCRM admin-tier tools (9 tools, Studio ops)"
```

---

## Task 9: Wire tools into MCP server startup

**Files:**
- Modify: `mcp_esa/server/mcp_server.py`

- [ ] **Step 1: Register sugarcrm tools behind the feature flag**

Edit `mcp_esa/server/mcp_server.py`. In `create_mcp_server()`, after the Revenue Management block, add:

```python
    # Register SugarCRM tools
    if settings.sugarcrm_enabled:
        try:
            from mcp_esa.tools.sugarcrm_tools import register_sugarcrm_tools
            register_sugarcrm_tools(server, app)
            logger.info("SugarCRM tools registered")
        except Exception as e:
            logger.warning(f"Failed to register SugarCRM tools: {e}")
```

- [ ] **Step 2: Start the MCP server briefly to verify registration**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && timeout 5 python3 mcp_esa/main.py 2>&1 | grep -i "sugarcrm\|tool" | head -20
```

Expected: log lines showing `SugarCRM read-tier tools registered (14)`, `SugarCRM write-tier tools registered (8)`, `SugarCRM admin-tier tools registered (9)`, `SugarCRM tools registered`, and a final tool count.

If registration fails with a decorator-related error, update the tools module to match the decorator pattern used in `mcp_esa/tools/google_ads_tools.py` (same SDK version, same pattern).

- [ ] **Step 3: Commit**

```bash
git add mcp_esa/server/mcp_server.py
git commit -m "feat(mcp): register SugarCRM tools in server startup"
```

---

## Task 10: Smoke test script

**Files:**
- Create: `mcp_esa/tests/smoke_sugarcrm.py`

- [ ] **Step 1: Create smoke script**

```python
"""
Manual smoke test for SugarCRM MCP service.

NOT RUN IN CI — this hits the live tenant. Run manually against a dev/staging
tenant with a read-only account first, then against write/admin with an account
whose changes can be rolled back.

Usage:
    python3 mcp_esa/tests/smoke_sugarcrm.py read
    python3 mcp_esa/tests/smoke_sugarcrm.py write
    python3 mcp_esa/tests/smoke_sugarcrm.py admin
"""
import sys
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

from mcp_esa.config.settings import get_settings
from mcp_esa.services.sugarcrm_service import SugarCRMService, SugarCRMConfig


def _svc():
    s = get_settings()
    cfg = SugarCRMConfig(
        url=s.sugarcrm_url,
        username=s.sugarcrm_username,
        password=s.sugarcrm_password,
        client_id=s.sugarcrm_client_id,
        client_secret=s.sugarcrm_client_secret,
        platform=s.sugarcrm_platform,
        timeout=s.sugarcrm_timeout,
    )
    return SugarCRMService(cfg)


def read_smoke():
    svc = _svc()
    print("--- list_modules ---")
    mods = svc.list_modules()
    print(f"modules returned: {len(mods.get('modules', {}))}")
    print("--- list_records Accounts limit=3 ---")
    accts = svc.list_records("Accounts", limit=3, fields=["name"])
    print(json.dumps(accts, indent=2)[:500])
    print("--- list_fields Leads ---")
    f = svc.list_fields("Leads")
    print(f"Leads field count: {len(f.get('fields', {}))}")


def write_smoke():
    svc = _svc()
    print("--- create_record Leads ---")
    new = svc.create_record("Leads", {
        "first_name": "MCP", "last_name": "SmokeTest",
        "status": "New", "description": "mcp_esa smoke test — safe to delete",
    })
    lead_id = new.get("id")
    print(f"created lead id: {lead_id}")
    print("--- update_record ---")
    svc.update_record("Leads", lead_id, {"description": "updated by smoke test"})
    print("--- delete_record ---")
    svc.delete_record("Leads", lead_id)
    print("cleaned up")


def admin_smoke():
    svc = _svc()
    print("--- list_dropdowns ---")
    dd = svc.list_dropdowns()
    print(f"dropdown count: {len(dd)}")
    # Do NOT create/delete fields in smoke — leave that for manual QA.


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "read"
    {"read": read_smoke, "write": write_smoke, "admin": admin_smoke}[mode]()
```

- [ ] **Step 2: Run read smoke (optional, requires real tenant access)**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 mcp_esa/tests/smoke_sugarcrm.py read
```

Expected: module count > 0, some accounts printed, Leads field count > 0. If SugarCRM credentials in the vault are stale, this is the moment to find out.

- [ ] **Step 3: Commit**

```bash
git add mcp_esa/tests/smoke_sugarcrm.py
git commit -m "test(mcp): add SugarCRM smoke test script"
```

---

## Task 11: Documentation

**Files:**
- Create: `docs/api/sugarcrm_mcp.md`

- [ ] **Step 1: Write the docs page**

Create `docs/api/sugarcrm_mcp.md`:

```markdown
# SugarCRM MCP Tools

The MCP server (`mcp_esa/`) exposes 31 tools for interacting with the ESA SugarCRM tenant via the REST v11 API.

## Tool Tiers

Tools are grouped into three conventional scope tiers. Enforcement is via the existing per-tool allowlist on `api_keys.mcp_tools`; the tiers document recommended grant bundles.

### sugarcrm_read (14)
- SC_get_record, SC_list_records, SC_search, SC_get_related
- SC_list_modules, SC_list_fields, SC_get_field
- SC_list_dropdowns, SC_get_dropdown, SC_get_layout
- SC_get_lead, SC_get_contact, SC_get_account, SC_search_by_email

### sugarcrm_write (8)
- SC_create_record, SC_update_record, SC_delete_record
- SC_link_records, SC_unlink_records
- SC_create_lead, SC_convert_lead, SC_log_call

### sugarcrm_admin (9)
- SC_create_field, SC_update_field, SC_delete_field
- SC_update_dropdown
- SC_create_relationship, SC_delete_relationship
- SC_update_layout, SC_studio_deploy
- SC_list_fields_admin

## Destructive Guardrails
`SC_delete_record`, `SC_delete_field`, `SC_delete_relationship`, and `SC_studio_deploy` require an explicit `confirm=True` argument.

## Config
In `backend/python/config/mcp.yaml`:

```yaml
features:
  sugarcrm: true

sugarcrm:
  url: https://extraspaceasia.sugarondemand.com
  username: <service account>
  client_id: sugar
  platform: mcp_esa
  timeout: 30
  password_vault: SUGARCRM_PASSWORD
  client_secret_vault: SUGARCRM_CLIENT_SECRET
```

Secrets resolved from the DB vault (`app_secrets` table).

## Granting Access
In `/admin/api-keys`, edit a key and add the desired `SC_*` tool names to its `mcp_tools` array.

## Smoke Test
```
python3 mcp_esa/tests/smoke_sugarcrm.py read
```
```

- [ ] **Step 2: Commit**

```bash
git add docs/api/sugarcrm_mcp.md
git commit -m "docs(mcp): add SugarCRM tools documentation"
```

---

## Task 12: Security audit

**Files:** (review only, no writes)
- `mcp_esa/services/sugarcrm_service.py`
- `mcp_esa/tools/sugarcrm_tools.py`
- `mcp_esa/config/settings.py` (diff)
- `mcp_esa/server/mcp_server.py` (diff)
- `backend/python/config/mcp.yaml` (diff)

- [ ] **Step 1: Dispatch pentest-code-reviewer agent**

Run the `pentest-code-reviewer` agent against all files listed above. Audit focus:

1. **Error leakage** — is raw API response text ever returned to the MCP client? It must be logged but not surfaced.
2. **Module-name injection** — every `module`, `field_name`, `link_name`, and `record_id` is interpolated into a URL. Are they all validated via the regex helpers in Task 3/4/5?
3. **Credential handling** — password and client_secret only loaded from vault, never logged, never returned by any tool.
4. **Destructive guardrails** — confirm=True enforced on all delete/rebuild tools.
5. **Auth retry loop** — `_request`'s 401→reauth→retry path cannot loop indefinitely.
6. **Timeout** — all HTTP calls bounded by the `timeout` config.

- [ ] **Step 2: Fix any findings inline**

Address findings directly in the source files. Re-run the tests:

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && python3 -m pytest mcp_esa/tests/test_sugarcrm_service.py -v
```

Expected: all PASS.

- [ ] **Step 3: Commit fixes (if any)**

```bash
git add mcp_esa/
git commit -m "security(mcp): address SugarCRM pentest findings"
```

- [ ] **Step 4: Final import sanity**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA && timeout 5 python3 mcp_esa/main.py 2>&1 | grep -iE "sugarcrm|tool count|registered" | head -20
```

Expected: SugarCRM tier logs + non-zero tool count.

---

## Summary of Deliverables

| # | File | Type |
|---|------|------|
| 1 | `backend/python/config/mcp.yaml` | modified — add `sugarcrm:` section + feature flag |
| 2 | `mcp_esa/config/settings.py` | modified — add `sugarcrm_*` properties |
| 3 | `mcp_esa/services/sugarcrm_service.py` | new — ~350 LOC |
| 4 | `mcp_esa/tools/sugarcrm_tools.py` | new — ~450 LOC |
| 5 | `mcp_esa/server/mcp_server.py` | modified — register sugarcrm tools |
| 6 | `mcp_esa/tests/__init__.py` | new (if missing) |
| 7 | `mcp_esa/tests/test_sugarcrm_service.py` | new — mocked unit tests |
| 8 | `mcp_esa/tests/smoke_sugarcrm.py` | new — manual smoke |
| 9 | `docs/api/sugarcrm_mcp.md` | new — admin + dev docs |

Total: 31 MCP tools (14 read + 8 write + 9 admin).
