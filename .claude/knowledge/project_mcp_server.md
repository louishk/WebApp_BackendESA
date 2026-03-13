# ESA MCP Server

Independent MCP server (`mcp_esa/`) providing database and Google Ads tools over Streamable HTTP transport. Runs separately from the Flask app on port 8002.

- **Config**: `backend/config/mcp.yaml` (loaded via `config_loader`, secrets via `_vault` suffix)
- **Entry point**: `mcp_esa/main.py`
- **Transport**: Streamable HTTP at `http://127.0.0.1:8002`
- **Features flag**: `features.database` and `features.google_ads` in `mcp.yaml`

---

## Database Tools (DB_*)

Tools for connecting to and querying configured database presets. All queries are read-only (SELECT, SHOW, DESCRIBE, EXPLAIN, WITH only).

| Tool | Description |
|---|---|
| `DB_list_database_presets` | List all configured presets from `mcp.yaml` |
| `DB_connect_preset` | Connect to a preset by name |
| `DB_connect_multiple_presets` | Connect to multiple presets at once (comma-separated names) |
| `DB_execute_query` | Execute a SQL query on an active connection |
| `DB_list_tables` | List tables in a connected database (optional schema filter) |
| `DB_describe_table` | Show columns and types for a specific table |
| `DB_list_connections` | Show all currently active connections |
| `DB_disconnect_database` | Close and remove a connection |

### Typical workflow
```
DB_connect_preset("esa_pbi")
DB_list_tables("esa_pbi")
DB_execute_query("esa_pbi", "SELECT site_code, COUNT(*) FROM rent_roll GROUP BY site_code")
```

### Configured presets (from `mcp.yaml`)

| Preset | Type | Database |
|---|---|---|
| `esa_pbi` | PostgreSQL | esa_pbi (Azure) |
| `esa_backend` | PostgreSQL | backend (Azure) |
| `rudderstack` | PostgreSQL | rudderstack (Azure) |
| `bigquery` | BigQuery | planar-beach-485003-v9, region: asia-southeast1 |
| `kinsta_esa_sg` | MySQL | extraspaceasiasg (requires SSH tunnel, port 13306) |
| `kinsta_esa_my` | MySQL | extraspaceasiamy (requires SSH tunnel, port 13307) |
| `kinsta_esa_kr` | MySQL | extraspaceasiakr (requires SSH tunnel, port 13308) |
| `kinsta_esa_hk` | MySQL | extraspaceasiahk (requires SSH tunnel, port 13309) |

---

## Google Ads Tools (ga_*)

Tools for the Google Ads API. Credentials come from `mcp.yaml` + vault secrets. Requires `google-ads>=25.0.0`.

### Utility / Account
| Tool | Description |
|---|---|
| `ga_test_connection` | Test API connection and check credentials |
| `ga_list_accessible_customers` | List all accessible Google Ads accounts |
| `ga_get_account_info` | Get name, currency, timezone for a specific account |

### Campaign Management
| Tool | Description |
|---|---|
| `ga_list_campaigns` | List campaigns (optional: include metrics, filter by status) |
| `ga_get_campaign` | Full details + metrics for a specific campaign |
| `ga_create_campaign` | Create a new campaign (defaults to PAUSED) |
| `ga_update_campaign` | Update name, status, or daily budget |
| `ga_set_campaign_status` | Change status: ENABLED, PAUSED, or REMOVED |

### Ad Group Management
| Tool | Description |
|---|---|
| `ga_list_ad_groups` | List ad groups (optional: filter by campaign) |
| `ga_create_ad_group` | Create a new ad group in a campaign |
| `ga_update_ad_group` | Update name, status, or CPC bid |

### Reporting & Analytics
| Tool | Description |
|---|---|
| `ga_query` | Execute a raw GAQL query (most flexible) |
| `ga_get_campaign_performance` | Campaign metrics for a date range |
| `ga_get_account_performance` | Account-level metric summary |
| `ga_get_keyword_performance` | Keyword metrics (optional campaign/ad group filter) |

### AI-Powered Analysis (requires LLM service)
These tools gather data from the API then pass it through the LLM manager (`services/llm/llm_manager.py`) for analysis. If LLM is unavailable, they return the raw data.

| Tool | Description |
|---|---|
| `ga_audit_account` | Comprehensive account audit: structure, keywords, budget, conversions |
| `ga_analyze_keywords` | Keyword strategy: top performers, underperformers, cannibalization, QS issues |
| `ga_analyze_search_terms` | Search term intent analysis: wasted spend, new keyword opportunities, negative suggestions |
| `ga_suggest_negative_keywords` | Specific negative keyword recommendations with match types |
| `ga_analyze_competitors` | Competitor analysis from auction insights |
| `ga_analyze_quality_scores` | QS analysis by component (CTR, ad relevance, landing page) |
| `ga_analyze_trends` | Daily/device performance trends with forecasting |
| `ga_analyze_audiences` | Geographic and device targeting efficiency |
| `ga_optimize_budget` | Budget reallocation recommendations by campaign ROAS |
| `ga_generate_report` | AI-generated report: types `executive`, `detailed`, or `optimization` |

### Token Management
| Tool | Description |
|---|---|
| `ga_start_token_refresh` | Start background OAuth token refresh |

---

## Configuration Reference (`backend/config/mcp.yaml`)

```yaml
server:
  host: "127.0.0.1"
  port: 8002

features:
  database: true
  google_ads: true

google_ads:
  client_id: "..."
  login_customer_id: "3494417856"
  client_secret_vault: "GOOGLE_ADS_CLIENT_SECRET"     # vault key
  developer_token_vault: "GOOGLE_ADS_DEVELOPER_TOKEN" # vault key
  refresh_token_vault: "GOOGLE_ADS_REFRESH_TOKEN"     # vault key

databases:
  <preset_name>:
    type: postgresql | mysql | bigquery
    host: ...
    port: ...
    database: ...
    user: ...
    password_vault: <VAULT_KEY>   # traditional DBs
    ssl: true/false
    # BigQuery only:
    project_id: ...
    location: ...
    credentials_json_vault: <VAULT_KEY>
```

Passwords are never stored in plain text — the `password_vault` and `credentials_json_vault` keys reference entries in the `app_secrets` table, resolved by `common/secrets_vault.py`.

---

## Security

### Database queries
- **Whitelist-only**: only `SELECT`, `SHOW`, `DESCRIBE`/`DESC`, `EXPLAIN`, and `WITH` are accepted
- **Comment stripping**: leading `--` and `/* */` comments are stripped before the prefix check to prevent bypass
- **Length limit**: queries over 10,000 characters are rejected
- **Excessive UNION guard**: more than 5 UNION statements in one query is rejected
- **No DDL/DML possible**: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE` are all blocked

### Google Ads
- `customer_id` is passed directly to the Google Ads client library; the client library handles validation
- `ga_set_campaign_status` validates the status value against a fixed allowlist (`ENABLED`, `PAUSED`, `REMOVED`) before calling the API
- All tool exceptions are caught — the raw exception is logged server-side only; the MCP response returns a generic "Check server logs" message

### Credentials
- All secrets (DB passwords, Google Ads tokens, BigQuery service account JSON) are pulled from the DB vault at runtime, not stored in `mcp.yaml`
- `DatabaseConfig.to_connection_string()` masks the password by default (pass `include_password=True` only for debugging)
