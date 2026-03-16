# MCP ESA Server — Proposal

## Overview

Build an MCP server (`backend/python/mcp_server.py`) using Python SDK (`FastMCP`) that exposes the existing ESA Backend capabilities to AI agents and Claude. Reuses existing modules — no new business logic, just an MCP layer on top.

**SDK**: `mcp[cli]` (Python SDK v1.25+)
**Transport**: Streamable HTTP (spec 2025-03-26+) for remote access, stdio for local
**Location**: `backend/python/mcp_server.py` + `backend/python/mcp/` package

---

## Proposed Tools (12)

### Operations Tools

| # | Tool Name | Source | Annotations | Description |
|---|-----------|--------|-------------|-------------|
| 1 | `get_sites` | `GET /api/sites` | `readOnlyHint=true` | List all sites grouped by country |
| 2 | `get_billing_day_tenants` | `GET /api/billing-day/<site_id>` | `readOnlyHint=true` | Get tenants & billing day for a site |
| 3 | `update_billing_day` | `POST /api/billing-day/update` | `destructiveHint=true` | Change tenant billing day to 1st (SOAP) |
| 4 | `get_discount_plans` | `GET /api/discount-plans` | `readOnlyHint=true` | List all discount plans with details |
| 5 | `get_sitelink_discounts` | `GET /api/cc-discount-plans/<site_id>` | `readOnlyHint=true` | Get Sitelink discount plans for a site |
| 6 | `update_sitelink_discounts` | `POST /api/cc-discount-plans/update-simple` | `destructiveHint=true` | Enable/disable discount plans |
| 7 | `get_inventory_units` | `GET /api/inventory/units` | `readOnlyHint=true` | List units with type/climate/shape info |
| 8 | `update_inventory_overrides` | `PUT /api/inventory/overrides` | `destructiveHint=true` | Update unit attribute overrides |
| 9 | `publish_inventory_labels` | `POST /api/inventory/publish-labels` | `destructiveHint=true` | Publish labels to Sitelink SOAP API |

### Pipeline & Monitoring Tools

| # | Tool Name | Source | Annotations | Description |
|---|-----------|--------|-------------|-------------|
| 10 | `get_data_freshness` | `GET /api/data-freshness` | `readOnlyHint=true` | Check last extract dates per pipeline |
| 11 | `run_pipeline` | `POST /api/jobs/<pipeline>/run-async` | `destructiveHint=true` | Trigger a data pipeline |
| 12 | `get_pipeline_status` | `GET /api/jobs` + `GET /api/history` | `readOnlyHint=true` | List pipelines, schedules, recent history |

### Why These Tools

- **Tools 1-9**: Direct equivalents of the three tool pages (billing date changer, discount plan changer, inventory checker). These are the most common operator tasks.
- **Tools 10-12**: Pipeline monitoring — lets an agent check if data is stale and trigger refreshes.
- **Not included**: Admin/user management, secrets, config editing — too sensitive for MCP exposure. Statistics endpoints are read-only telemetry, low value as MCP tools.

---

## Proposed Resources (4)

MCP Resources expose read-only data that agents can pull into context.

| # | Resource URI | Source | Description |
|---|-------------|--------|-------------|
| 1 | `esa://sites` | `esa_pbi.site_info` | All sites with country, address, timezone |
| 2 | `esa://sites/{site_id}/rent-roll` | `esa_pbi.rent_roll` | Current rent roll for a site (units, occupancy, rates) |
| 3 | `esa://discount-plans` | `esa_backend.discount_plans` | All discount plans with linked concessions |
| 4 | `esa://pipelines` | `config/pipelines.yaml` | Pipeline definitions, schedules, dependencies |

---

## Elicitation (Spec 2025-06-18 Feature)

Use MCP elicitation for destructive operations — the server pauses and asks the user for confirmation before proceeding.

**Where to use:**
- `update_billing_day` → "Change billing day for {count} tenants at {site_name} to 1st? (y/n)"
- `update_sitelink_discounts` → "Enable/disable {count} plans at {site_name}?"
- `publish_inventory_labels` → "Publish labels for {count} units to Sitelink?"
- `run_pipeline` → "Run pipeline '{name}'? Estimated duration: {est_duration}"

This leverages the elicitation capability so the agent doesn't blindly execute write operations.

---

## Structured Output (Spec 2025-06-18 Feature)

Return typed JSON alongside human-readable text for all tools. Example:

```
get_sites → {
  "countries": [
    {"country": "Singapore", "sites": [{"id": 1, "name": "Kallang Way"}, ...]},
    ...
  ],
  "total_sites": 42
}
```

This lets downstream agents parse structured data without scraping text.

---

## Tool Annotations (Spec 2025-03-26 Feature)

Every tool gets explicit annotations:

```python
@mcp.tool(annotations={
    "title": "Update Billing Day",
    "readOnlyHint": False,
    "destructiveHint": True,
    "idempotentHint": True,   # safe to retry
    "openWorldHint": False,   # only touches our SOAP API
})
```

This tells the agent which tools are safe to call without confirmation and which need care.

---

## Architecture

### File Structure
```
backend/python/
  mcp_server.py              # Entry point: FastMCP app definition
  mcp/
    __init__.py
    tools/
      __init__.py
      operations.py           # Tools 1-9 (sites, billing, discounts, inventory)
      pipelines.py            # Tools 10-12 (data freshness, run pipeline, status)
    resources/
      __init__.py
      sites.py                # Resources 1-2 (sites, rent roll)
      plans.py                # Resource 3 (discount plans)
      pipelines.py            # Resource 4 (pipeline configs)
    auth.py                   # MCP auth → reuses jwt_auth.py + api_key verification
    db.py                     # DB session helpers → reuses config_loader + session.py
```

### Reuse Strategy

| Existing Module | MCP Usage |
|----------------|-----------|
| `common/config_loader.py` | DB connection strings, YAML configs |
| `common/session.py` | `SessionManager.session_scope()` for esa_pbi queries |
| `common/soap_client.py` | Billing day updates, inventory publish (direct SOAP calls) |
| `common/secrets_vault.py` | API keys, SOAP credentials |
| `web/auth/jwt_auth.py` | Validate incoming MCP requests via API key |
| `web/utils/audit.py` | `audit_log()` every MCP tool invocation |
| `web/models/*` | ORM models for discount plans, inventory, users |
| `common/models.py` | RentRoll, SiteInfo, CCDiscount for PBI queries |

### Authentication

MCP requests authenticated via ESA API keys (same `esa_<key_id>.<secret>` format). The MCP auth layer:
1. Extracts API key from MCP request metadata
2. Validates using existing `ApiKey.verify_secret()`
3. Checks scopes (e.g., `inventory:write` for publish operations)
4. Sets user context for audit logging

New API scope needed: `mcp:access` — gates MCP server access itself.

### No Flask Dependency

The MCP server runs as a **standalone process** (not inside Flask). It imports from `common/` and `web/models/` directly, creates its own DB sessions via `config_loader` + `SessionManager`. This avoids coupling to the Flask app lifecycle.

---

## Transport & Deployment

| Mode | Transport | Use Case |
|------|-----------|----------|
| Local dev | stdio | Claude Code / Claude Desktop on developer machines |
| Remote | Streamable HTTP | Production — runs alongside esa-backend on the VM |

**Production deployment**: New systemd service `esa-mcp` running `mcp_server.py` on a dedicated port (e.g., 8001). Add to `deploy_to_vm.py` pipeline.

---

## New Dependency

Only one new pip package:

```
mcp[cli]>=1.25.0
```

(Pulls in `httpx`, `pydantic`, `sse-starlette`, `uvicorn` — all lightweight)

---

## What's NOT Included (and Why)

| Capability | Reason |
|-----------|--------|
| User/role management | Too sensitive — admin-only, low automation value |
| Secrets/config editing | Security risk — vault should not be MCP-accessible |
| ECRI batch operations | Complex multi-step workflow, needs dedicated UI review |
| Translation service | Low standalone value, already embedded in discount plan workflow |
| Statistics/telemetry | Read-only dashboards, better consumed via the web UI |
| Direct SQL queries | Security risk — only expose curated ORM queries |

---

## Implementation Order

1. **Phase 1**: Scaffolding — `mcp_server.py`, auth, DB helpers, `mcp/` package structure
2. **Phase 2**: Read-only tools — `get_sites`, `get_billing_day_tenants`, `get_discount_plans`, `get_sitelink_discounts`, `get_inventory_units`, `get_data_freshness`, `get_pipeline_status`
3. **Phase 3**: Resources — all 4 resources
4. **Phase 4**: Write tools with elicitation — `update_billing_day`, `update_sitelink_discounts`, `update_inventory_overrides`, `publish_inventory_labels`, `run_pipeline`
5. **Phase 5**: Deployment config — systemd service, `deploy_to_vm.py` update, `.ai/mcp/mcp.json` config
