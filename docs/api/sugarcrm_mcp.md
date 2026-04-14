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
