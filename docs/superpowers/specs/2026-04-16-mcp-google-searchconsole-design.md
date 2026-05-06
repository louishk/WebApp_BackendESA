# MCP Google Search Console Tools — Design Spec

**Date:** 2026-04-16
**Purpose:** Add Google Search Console (GSC) tools to the ESA MCP server for keyword analysis, URL inspection, sitemap management, and index coverage.
**Primary use case:** Keyword learning — understanding what search queries drive traffic across ESA's multi-country properties.

## Architecture

Follows the established MCP tool pattern:

```
mcp.yaml (config + vault refs)
  → config/settings.py (typed properties)
    → services/google_searchconsole_service.py (API client)
      → tools/google_searchconsole_tools.py (MCP tool registration)
```

### API Client

Uses `google-api-python-client` with OAuth2 user credentials (same pattern as GA4). The Search Console API is a REST discovery API, not a gRPC client like GA4/Ads — so we use `googleapiclient.discovery.build('searchconsole', 'v1', credentials=...)`.

Library: `google-api-python-client` (already in ecosystem, lighter than a dedicated client).

## Auth

- **OAuth app:** Reuses existing Google Cloud project / client ID (`387700327426-...`)
- **Scope:** `https://www.googleapis.com/auth/webmasters` (read + write, needed for sitemap submit/delete)
- **Refresh token:** New vault secret `GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN`, minted via `scripts/mint_gsc_refresh_token.py`
- **Config section:** `google_searchconsole` in `mcp.yaml`
- **Feature flag:** `features.google_searchconsole: true` in `mcp.yaml`

## Tools (8 total, prefix: `GSC_`)

All tools take `site_url` as a required parameter (multiple properties across countries).

### 1. `GSC_test_connection`

Validate credentials and confirm API access.

- **Params:** none
- **Returns:** status, list of accessible site URLs, credential validity

### 2. `GSC_list_sites`

List all verified Search Console properties.

- **Params:** none
- **Returns:** array of `{site_url, permission_level}`

### 3. `GSC_analyze_keywords` (primary tool)

Search performance data grouped by query keyword.

- **Params:**
  - `site_url` (required) — the SC property URL (e.g. `sc-domain:extraspace.com.sg`)
  - `start_date` (required) — YYYY-MM-DD
  - `end_date` (required) — YYYY-MM-DD
  - `country` (optional) — 3-letter ISO code filter (e.g. `SGP`, `MYS`, `KOR`)
  - `device` (optional) — `DESKTOP`, `MOBILE`, `TABLET`
  - `page_filter` (optional) — URL substring to filter pages
  - `query_filter` (optional) — keyword substring to filter queries
  - `row_limit` (optional, default 100, max 25000) — number of rows
  - `sort_by` (optional, default `clicks`) — `clicks`, `impressions`, `ctr`, `position`
- **Returns:** array of `{query, clicks, impressions, ctr, position}` sorted by `sort_by`
- **API method:** `searchanalytics.query` with `dimensions: ["query"]`

### 4. `GSC_inspect_url`

Get index status, crawl info, and rich result status for a specific URL.

- **Params:**
  - `site_url` (required) — the SC property
  - `inspection_url` (required) — the full URL to inspect
- **Returns:** index status (verdict, coverage state, crawl time, robots info, indexing state), rich results, mobile usability
- **API method:** `urlInspection.index.inspect`

### 5. `GSC_list_sitemaps`

List sitemaps registered for a property.

- **Params:**
  - `site_url` (required)
- **Returns:** array of `{path, type, is_pending, last_submitted, last_downloaded, warnings, errors}`
- **API method:** `sitemaps.list`

### 6. `GSC_submit_sitemap`

Submit a new sitemap to a property.

- **Params:**
  - `site_url` (required)
  - `sitemap_url` (required) — full URL of the sitemap
- **Returns:** success/error status
- **API method:** `sitemaps.submit`

### 7. `GSC_delete_sitemap`

Remove a sitemap from a property.

- **Params:**
  - `site_url` (required)
  - `sitemap_url` (required)
- **Returns:** success/error status
- **API method:** `sitemaps.delete`

### 8. `GSC_get_coverage`

Index coverage summary — how many pages are indexed, errored, warned, excluded.

- **Params:**
  - `site_url` (required)
  - `start_date` (optional, default last 28 days)
  - `end_date` (optional, default today)
- **Returns:** summary counts and breakdown by category
- **Implementation note:** The Coverage API is part of the `searchanalytics.query` endpoint using `type: "discover"` or the URL Inspection API bulk approach. Since there's no dedicated coverage endpoint, this will use `searchanalytics.query` with `type: "web"` and `dimensions: ["page"]` to get page-level indexing signals, or fall back to aggregating URL inspection results. The exact approach will be determined during implementation based on API capabilities.

## Files to Create

| File | Purpose |
|---|---|
| `mcp_esa/services/google_searchconsole_service.py` | GSC API client (OAuth2 credentials, search analytics, URL inspection, sitemaps) |
| `mcp_esa/tools/google_searchconsole_tools.py` | MCP tool registration (8 tools) |
| `scripts/mint_gsc_refresh_token.py` | One-time script to mint OAuth refresh token with `webmasters` scope |

## Files to Modify

| File | Change |
|---|---|
| `backend/python/config/mcp.yaml` | Add `google_searchconsole` config section + feature flag |
| `mcp_esa/config/settings.py` | Add `google_searchconsole_*` properties |
| `mcp_esa/server/mcp_server.py` | Register GSC tools block |
| `mcp_esa/requirements.txt` | Add `google-api-python-client` if not present |

## Config (mcp.yaml addition)

```yaml
google_searchconsole:
  client_id: "387700327426-cmsrklgau8vqh9mnjvl95cc4vvenqj77.apps.googleusercontent.com"
  client_secret_vault: "GOOGLE_ADS_CLIENT_SECRET"
  refresh_token_vault: "GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN"
```

## Service Layer (`GSCService`)

```
GSCConfig (dataclass):
  - client_id: str
  - client_secret: str
  - refresh_token: str

GSCService:
  - __init__(config) → builds OAuth2 credentials, creates discovery client
  - test_connection() → list sites to verify creds
  - list_sites() → sitemaps API sites().list()
  - analyze_keywords(site_url, start_date, end_date, **filters) → searchanalytics.query
  - inspect_url(site_url, inspection_url) → urlInspection.index.inspect
  - list_sitemaps(site_url) → sitemaps.list
  - submit_sitemap(site_url, sitemap_url) → sitemaps.submit
  - delete_sitemap(site_url, sitemap_url) → sitemaps.delete
  - get_coverage(site_url, start_date, end_date) → searchanalytics.query with page dimension

GSCAPIError(Exception):
  - message: str
  - details: dict
```

## Error Handling

Same pattern as GA4/Google Ads:
- `GSCAPIError` for API-level errors (auth failures, quota, invalid property)
- Generic error messages to MCP client, detailed logging server-side
- Graceful import check (`GSC_AVAILABLE` flag) for missing `google-api-python-client`

## Vault Secret

One new vault entry needed:
- **Key:** `GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN`
- **Value:** OAuth refresh token minted with `webmasters` scope
- **Stored via:** `/admin/secrets` UI after running `scripts/mint_gsc_refresh_token.py`
