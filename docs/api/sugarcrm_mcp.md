# SugarCRM MCP Tools

The MCP server (`mcp_esa/`) exposes 31 tools for interacting with the ESA SugarCRM tenant via the REST v11 API. Self-contained service — does not depend on the Flask app's `common/sugarcrm_client.py`.

## Tool Tiers

Tiers are a convention for granting bundles. Actual enforcement is the existing per-tool `api_keys.mcp_tools` allowlist — add the `SC_*` tool names you want the key to have.

### sugarcrm_read (14)
- `SC_get_record`, `SC_list_records`, `SC_search`, `SC_get_related`
- `SC_list_modules`, `SC_list_fields`, `SC_get_field`
- `SC_list_dropdowns`, `SC_get_dropdown`, `SC_get_layout`
- `SC_get_lead`, `SC_get_contact`, `SC_get_account`, `SC_search_by_email`

### sugarcrm_write (8)
- `SC_create_record`, `SC_update_record`, `SC_delete_record`
- `SC_link_records`, `SC_unlink_records`
- `SC_create_lead`, `SC_convert_lead`, `SC_log_call`

### sugarcrm_admin (9)
- `SC_create_field`, `SC_update_field`, `SC_delete_field`
- `SC_update_dropdown`
- `SC_create_relationship`, `SC_delete_relationship`
- `SC_update_layout`, `SC_studio_deploy`
- `SC_list_fields_admin`

## Destructive Guardrails

`SC_delete_record`, `SC_delete_field`, `SC_delete_relationship`, and `SC_studio_deploy` require `confirm=True`. Without it they return a refusal string.

## Sugar Cloud Studio limitations

The ESA SugarCRM tenant is hosted on Sugar Cloud (extraspace.sugarondemand.com, v11). Even though the `api_data` user is an admin (`type: admin`), Sugar Cloud has selectively **disabled the direct Studio write endpoints** at the routing layer. They appear in the auto-generated `/rest/v11/help` API inventory, but invoking them returns `no_method` (HTTP 404).

Verified-not-working on this tenant:
- `POST /<module>/customfield` (listed in /help — 404)
- `DELETE /<module>/customfield/:field` (404)
- `GET /Administration/dropdownEditor/:dropdownName` (404)
- `PUT /Administration/dropdownEditor/:dropdownName` (404)
- `POST /Administration/dropdownEditor/create` (404)
- `GET /Administration/adminPanelDefs` (404)

The following MCP tools therefore cannot succeed against this tenant:
- `SC_create_field`, `SC_update_field`, `SC_delete_field`
- `SC_update_dropdown`
- `SC_create_relationship`, `SC_delete_relationship` (no REST routes exist at all)
- `SC_update_layout` (no REST routes exist at all)
- `SC_studio_deploy` (Sugar handles rebuild internally on package install)

Read-side Studio tools work fine — metadata is readable, writes are blocked:
- `SC_list_modules`, `SC_list_fields`, `SC_get_field`, `SC_list_dropdowns`, `SC_get_dropdown`, `SC_get_layout`, `SC_list_fields_admin` — all ✅

### Working path for schema changes via REST: Module Loader

Sugar Cloud DOES expose the **Module Loader** API, which is the official supported way to make schema changes from outside the UI. Verified working on this tenant:
- `GET /Administration/packages/installed` — list installed packages ✅
- `GET /Administration/packages/staged` — list staged packages
- `POST /Administration/packages` — upload a package zip
- `GET /Administration/packages/:id/install` — install a staged package
- `GET /Administration/packages/:id/uninstall` — uninstall
- `GET /Administration/packages/:id/installation-status` — check install state
- `POST /Administration/package/customizations` — package up the tenant's current Studio customizations

A schema change via Module Loader requires:
1. A package zip with `manifest.php` + `installdefs` declaring the field/relationship/layout changes
2. Upload via `POST /Administration/packages`
3. Trigger install via `GET /Administration/packages/:id/install`

This is more involved than a single REST call but is the only programmatic path on Sugar Cloud. If schema-change automation becomes a real requirement, a follow-up tool set (`SC_upload_package`, `SC_install_package`, `SC_list_packages`) wrapping these endpoints can be added.

### Recommendation

For now: leave the direct Studio tools registered as a forward-compatibility placeholder (they will start working on Sugar on-prem or any tenant where Sugar re-enables direct Studio routes), and use the Sugar Studio UI for schema changes. If automation is needed, add Module Loader tools.

## Config

In `backend/python/config/mcp.yaml`:

```yaml
features:
  sugarcrm: true

sugarcrm:
  url: "https://extraspaceasia.sugarondemand.com"
  username: "<service account>"
  client_id: "sugar"
  platform: "mcp_esa"
  timeout: 30
  password_vault: "SUGARCRM_PASSWORD"
  client_secret_vault: "SUGARCRM_CLIENT_SECRET"
```

Secrets resolved from the DB vault (`app_secrets` table) via `common.config_loader.get_secret`. Non-secret fields live in yaml.

## Granting Access to an API Key

In `/admin/api-keys`, edit a key and add the desired `SC_*` tool names to its `mcp_tools` JSON array. Example: read-only access for an analyst key:

```json
["SC_get_record", "SC_list_records", "SC_search", "SC_search_by_email"]
```

## Security Notes

- All `module`, `record_id`, `field_name`, and `link_name` values are validated with strict regex before being interpolated into URL paths — prevents path injection.
- Raw SugarCRM API error text is never returned to the MCP client; errors are logged in full and clients receive a generic message plus the error code.
- Credentials are loaded from the DB vault only, never logged, never returned by any tool.
- Destructive operations require explicit `confirm=True`.

## Smoke Test

```
python3 mcp_esa/tests/smoke_sugarcrm.py read
python3 mcp_esa/tests/smoke_sugarcrm.py write    # creates+deletes a throwaway Lead
python3 mcp_esa/tests/smoke_sugarcrm.py admin
```

## Implementation

- Service: `mcp_esa/services/sugarcrm_service.py`
- Tools: `mcp_esa/tools/sugarcrm_tools.py`
- Tests: `mcp_esa/tests/test_sugarcrm_service.py` (unit, mocked httpx)
- Registration: `mcp_esa/server/mcp_server.py` (feature-flagged)
