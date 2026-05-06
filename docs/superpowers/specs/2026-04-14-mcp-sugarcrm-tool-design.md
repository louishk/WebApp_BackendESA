# MCP SugarCRM Tool — Design Spec

**Date:** 2026-04-14
**Target:** `mcp_esa/` (independent MCP server, Streamable HTTP)
**Status:** Approved for implementation planning

## Purpose

Give MCP clients full operational access to the ESA SugarCRM tenant: CRUD on all record modules (stock + custom), relationship management, and Studio/admin operations (custom fields, dropdowns, relationships, layouts). Matches the existing MCP tool conventions established by Google Ads and Database tools.

## Scope

**In scope**
- All SugarCRM modules: Leads, Contacts, Accounts, Opportunities, Cases, Tasks, Calls, Meetings, plus any custom modules present in the tenant (module list is discovered at runtime, not hardcoded).
- Full CRUD per record: get, list/search, create, update, delete.
- Relationship management: get related, link, unlink (e.g. Contact↔Account).
- Studio admin: list modules/fields, create/update/delete custom fields, manage dropdown lists, create module relationships (1:M, M:M), read/update layouts, deploy Studio changes.

**Out of scope (initial release)**
- Workflow/BPM definitions
- Reports module management
- Email template CRUD
- Bulk import/export
- File attachments (may be added later)

## Architecture

Follows the Google Ads pattern already established in `mcp_esa/`:
- Self-contained service class in `mcp_esa/services/` (no dependency on the Flask app's `common/sugarcrm_client.py`)
- Thin tool wrappers in `mcp_esa/tools/` that validate, call the service, and format responses
- Config loaded from `backend/config/mcp.yaml`
- Registered in `mcp_esa/main.py` alongside existing tool groups

```
mcp_esa/
  services/
    sugarcrm_service.py      # REST v11 client
  tools/
    sugarcrm_tools.py        # ~31 MCP tool functions
  config/
    settings.py              # + sugarcrm config loader
  tests/
    test_sugarcrm.py         # manual smoke test script
backend/config/mcp.yaml      # + sugarcrm: section
```

## Service Layer — `sugarcrm_service.py`

One `SugarCRMService` class wrapping the SugarCRM REST v11 API.

**Config dataclass**
```python
@dataclass
class SugarCRMConfig:
    url: str
    username: str
    password: str
    client_id: str = "sugar"
    client_secret: str = ""
    platform: str = "mcp_esa"
    timeout: int = 30
```

**Auth**
- OAuth2 password grant on first call
- Token cached in-memory with expiry tracking
- `_ensure_token()` refreshes when expired
- `_request()` wraps all HTTP calls, handles 401 with single reauth + retry, exponential backoff on 5xx

**Record methods (module-parameterized, generic)**
- `get_record(module, id, fields=None)`
- `list_records(module, filter=None, fields=None, limit=20, offset=0, order_by=None)`
- `search(module, q, fields=None, limit=20)`
- `create_record(module, data)`
- `update_record(module, id, data)`
- `delete_record(module, id)`

**Relationships**
- `get_related(module, id, link_name, limit=20, offset=0)`
- `link_records(module, id, link_name, related_id)`
- `unlink_records(module, id, link_name, related_id)`

**Studio / admin**
- `list_modules()` — returns modules with display labels + type (stock/custom)
- `list_fields(module)` — field metadata
- `get_field(module, field_name)`
- `create_field(module, spec)` — spec dict: name, type, label, len, default, required, etc.
- `update_field(module, field_name, spec)`
- `delete_field(module, field_name)`
- `list_dropdowns()` / `get_dropdown(name)`
- `update_dropdown(name, values)`
- `create_relationship(spec)` — lhs_module, rhs_module, type (one-to-many, many-to-many), label
- `delete_relationship(rel_name)`
- `get_layout(module, view)` — view: edit, detail, list, search
- `update_layout(module, view, spec)`
- `studio_deploy()` — push pending Studio metadata changes

**Errors**
- Custom `SugarCRMAPIError(message, code, details)` exception
- Never leak raw API error text to MCP client
- Generic message returned, full detail logged via `logging.getLogger(__name__)`

## Tools Layer — `sugarcrm_tools.py`

~31 tool functions, prefix `SC_`, grouped into three scope tiers. Each tool:
1. Validates args (type, required fields)
2. Looks up `SugarCRMService` singleton
3. Calls the corresponding service method
4. Returns `{"status": "success", "data": ...}` or `{"error": "..."}`
5. Catches `SugarCRMAPIError`, returns generic error + logs full

**`sugarcrm_read` scope (14 tools)**
- `SC_get_record`, `SC_list_records`, `SC_search`, `SC_get_related`
- `SC_list_modules`, `SC_list_fields`, `SC_get_field`, `SC_list_dropdowns`, `SC_get_dropdown`, `SC_get_layout`
- Convenience: `SC_get_lead`, `SC_get_contact`, `SC_get_account`, `SC_search_by_email`

**`sugarcrm_write` scope (8 tools)**
- `SC_create_record`, `SC_update_record`, `SC_delete_record`
- `SC_link_records`, `SC_unlink_records`
- Convenience: `SC_create_lead`, `SC_convert_lead`, `SC_log_call`

**`sugarcrm_admin` scope (9 tools)**
- `SC_create_field`, `SC_update_field`, `SC_delete_field`
- `SC_update_dropdown`
- `SC_create_relationship`, `SC_delete_relationship`
- `SC_update_layout`, `SC_studio_deploy`
- Destructive tools (`SC_delete_*`) require an explicit `confirm=True` arg to succeed; otherwise return an error explaining the guardrail.

## RBAC

Three new scopes in the MCP scope whitelist: `sugarcrm_read`, `sugarcrm_write`, `sugarcrm_admin`. Keys are granted tools individually via existing `mcp_tools` column on `api_keys`; scopes document the intended grant tiers and are enforced at the tool level.

Write tools also require read grant, admin also requires write (checked in decorator). This prevents partial grants that would leave keys unable to verify their own writes.

## Config

Add to `backend/python/config/mcp.yaml` (the actual path loaded by `common.config_loader`):

```yaml
sugarcrm:
  url: https://extraspaceasia.sugarondemand.com
  username: <non-secret service account username>
  client_id: sugar
  platform: mcp_esa
  timeout: 30
  password_vault: SUGARCRM_PASSWORD
  client_secret_vault: SUGARCRM_CLIENT_SECRET
```

Non-secret fields (url, username, client_id, platform, timeout) live in yaml. Secrets are resolved from the DB vault (`app_secrets` table) via `config_loader.get_secret()`. `SUGARCRM_PASSWORD` and `SUGARCRM_CLIENT_SECRET` are already present in the vault. This matches the existing Google Ads pattern in `settings.py`.

Add a `sugarcrm` feature flag under `features:` in mcp.yaml so the service can be disabled independently. If disabled or the section is missing, tools are not registered.

New `Settings` properties:
- `sugarcrm_enabled`, `sugarcrm_url`, `sugarcrm_username`, `sugarcrm_client_id`, `sugarcrm_platform`, `sugarcrm_timeout`
- `sugarcrm_password` (vault), `sugarcrm_client_secret` (vault)

## Testing

- Manual smoke test script: `mcp_esa/tests/test_sugarcrm.py`
- Hits each tier against a dev Sugar instance (or prod with a read-only key for the read tier)
- No mocked unit tests — matches the project's light test posture for MCP tools

## Security Audit

After implementation, run the `pentest-code-reviewer` agent on all new files:
- `mcp_esa/services/sugarcrm_service.py`
- `mcp_esa/tools/sugarcrm_tools.py`
- `mcp_esa/config/settings.py` diff
- `mcp_esa/main.py` diff

Audit focus: error leakage, credential handling, input validation on generic `module` parameter (prevent module-name injection), Studio tool guardrails.

## Deliverables Checklist

1. `mcp_esa/services/sugarcrm_service.py` — service + config + errors
2. `mcp_esa/tools/sugarcrm_tools.py` — 31 tool functions grouped by scope
3. `mcp_esa/config/settings.py` — sugarcrm config loader
4. `backend/config/mcp.yaml` — `sugarcrm:` section (placeholders)
5. `mcp_esa/main.py` — tool registration
6. MCP scope whitelist — add `sugarcrm_read/write/admin`
7. `mcp_esa/tests/test_sugarcrm.py` — smoke test
8. Project documentation entry (Project Documentation folder)
9. Security audit pass via `pentest-code-reviewer` agent

## Open Questions / Risks

- **Custom relationships in Studio**: the REST API's metadata endpoints for creating M:M relationships are lightly documented. Implementation may need to fall back to the `/Administration/RelationshipsApi` endpoint — verify in the dev tenant during implementation.
- **`studio_deploy` behavior**: some Studio changes auto-deploy; others require an explicit rebuild. The tool wraps `POST /Administration/Rebuild` — confirm which changes need it during testing.
- **Module-name injection**: generic `module` parameter must be validated against the runtime-discovered module list before being interpolated into URL paths.
