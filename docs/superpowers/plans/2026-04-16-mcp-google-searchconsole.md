# MCP Google Search Console Tools — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 8 Google Search Console MCP tools (GSC_ prefix) to the ESA MCP server, with keyword analysis as the primary use case.

**Architecture:** Follows the established service/tools pattern — `GSCConfig` dataclass → `GSCService` API client using `google-api-python-client` → `register_google_searchconsole_tools()` registered in `mcp_server.py`. Auth uses OAuth2 user credentials with a dedicated refresh token stored in the DB vault.

**Tech Stack:** Python 3, `google-api-python-client`, `google-auth`, `google-auth-oauthlib` (token minting only)

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `mcp_esa/services/google_searchconsole_service.py` | Create | API client: OAuth2 creds, search analytics, URL inspection, sitemaps |
| `mcp_esa/tools/google_searchconsole_tools.py` | Create | MCP tool registration (8 tools with input schemas) |
| `scripts/mint_gsc_refresh_token.py` | Create | One-time script to mint OAuth refresh token |
| `backend/python/config/mcp.yaml` | Modify | Add `google_searchconsole` config section + feature flag |
| `mcp_esa/config/settings.py` | Modify | Add `google_searchconsole_*` properties |
| `mcp_esa/server/mcp_server.py` | Modify | Register GSC tools block |
| `mcp_esa/requirements.txt` | Modify | Add `google-api-python-client` |

---

### Task 1: Config — mcp.yaml + settings.py + requirements.txt

**Files:**
- Modify: `backend/python/config/mcp.yaml`
- Modify: `mcp_esa/config/settings.py`
- Modify: `mcp_esa/requirements.txt`

- [ ] **Step 1: Add google_searchconsole section to mcp.yaml**

Add after the `google_analytics4` block (line 65) in `backend/python/config/mcp.yaml`:

```yaml
# =============================================================================
# Google Search Console API
# =============================================================================
# Reuses the Google Ads OAuth client_id/client_secret. Requires a NEW
# refresh token minted with the webmasters scope.
# Use scripts/mint_gsc_refresh_token.py.
google_searchconsole:
  client_id: "387700327426-cmsrklgau8vqh9mnjvl95cc4vvenqj77.apps.googleusercontent.com"
  client_secret_vault: "GOOGLE_ADS_CLIENT_SECRET"
  refresh_token_vault: "GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN"
```

- [ ] **Step 2: Add feature flag to mcp.yaml**

In the `features:` block, add:

```yaml
  google_searchconsole: true
```

- [ ] **Step 3: Add settings properties to settings.py**

In `mcp_esa/config/settings.py`, add `self._gsc` to `__init__` alongside the other config sections:

```python
self._gsc = self._mcp.get('google_searchconsole', {})
```

Then add these properties after the `google_analytics4_refresh_token` property (around line 189):

```python
# Google Search Console
@property
def google_searchconsole_enabled(self) -> bool:
    return self._features.get('google_searchconsole', False)

@property
def google_searchconsole_client_id(self) -> str:
    return self._gsc.get('client_id') or self._gads.get('client_id', '')

@property
def google_searchconsole_client_secret(self) -> str:
    vault_key = self._gsc.get('client_secret_vault', 'GOOGLE_ADS_CLIENT_SECRET')
    return self._config.get_secret(vault_key) or ''

@property
def google_searchconsole_refresh_token(self) -> str:
    vault_key = self._gsc.get('refresh_token_vault', 'GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN')
    return self._config.get_secret(vault_key) or ''
```

- [ ] **Step 4: Add google-api-python-client to requirements.txt**

Add to `mcp_esa/requirements.txt` after the Google Analytics 4 section:

```
# Google Search Console
google-api-python-client>=2.100.0
```

- [ ] **Step 5: Commit**

```bash
git add backend/python/config/mcp.yaml mcp_esa/config/settings.py mcp_esa/requirements.txt
git commit -m "feat(mcp): add Google Search Console config, settings, and dependency"
```

---

### Task 2: Service — google_searchconsole_service.py

**Files:**
- Create: `mcp_esa/services/google_searchconsole_service.py`

- [ ] **Step 1: Create the service file**

Create `mcp_esa/services/google_searchconsole_service.py`:

```python
"""
Google Search Console Service Module
Wraps the Search Console API using OAuth user credentials.
Uses google-api-python-client (discovery API).
"""

import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

try:
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GSC_AVAILABLE = True
except ImportError:
    GSC_AVAILABLE = False
    Credentials = None

logger = logging.getLogger(__name__)

GSC_SCOPES = ["https://www.googleapis.com/auth/webmasters"]
TOKEN_URI = "https://oauth2.googleapis.com/token"


@dataclass
class GSCConfig:
    """GSC OAuth user-credential config."""
    client_id: str
    client_secret: str
    refresh_token: str


class GSCAPIError(Exception):
    """GSC API error."""
    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.details = details or {}


class GSCService:
    """Service for Google Search Console API."""

    def __init__(self, config: GSCConfig):
        if not GSC_AVAILABLE:
            raise ImportError(
                "GSC libraries not installed. Run: "
                "pip install google-api-python-client google-auth"
            )
        self.config = config
        self._creds: Optional[Credentials] = None
        self._search_client = None
        self._webmasters_client = None

    # ------------------------------------------------------------------ creds
    def _get_credentials(self) -> Credentials:
        if self._creds is None:
            if not self.config.refresh_token:
                raise GSCAPIError("Search Console refresh token not configured")
            self._creds = Credentials(
                token=None,
                refresh_token=self.config.refresh_token,
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
                token_uri=TOKEN_URI,
                scopes=GSC_SCOPES,
            )
        return self._creds

    def _searchconsole(self):
        """Get the Search Console API v1 client (searchanalytics, urlInspection)."""
        if self._search_client is None:
            self._search_client = build(
                'searchconsole', 'v1',
                credentials=self._get_credentials(),
                cache_discovery=False,
            )
        return self._search_client

    def _webmasters(self):
        """Get the Webmasters API v3 client (sites, sitemaps)."""
        if self._webmasters_client is None:
            self._webmasters_client = build(
                'webmasters', 'v3',
                credentials=self._get_credentials(),
                cache_discovery=False,
            )
        return self._webmasters_client

    # --------------------------------------------------------------- sites
    async def list_sites(self) -> Dict[str, Any]:
        """List all verified Search Console properties."""
        try:
            result = self._webmasters().sites().list().execute()
            sites = []
            for entry in result.get('siteEntry', []):
                sites.append({
                    'site_url': entry.get('siteUrl', ''),
                    'permission_level': entry.get('permissionLevel', ''),
                })
            return {'status': 'success', 'sites': sites, 'count': len(sites)}
        except HttpError as e:
            raise GSCAPIError(f"list_sites failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"list_sites failed: {e}")

    async def test_connection(self) -> Dict[str, Any]:
        """Validate credentials by listing sites."""
        try:
            result = await self.list_sites()
            return {
                'status': 'success',
                'site_count': result['count'],
                'sites': [s['site_url'] for s in result['sites']],
            }
        except Exception as e:
            logger.error(f"GSC test_connection failed: {e}", exc_info=True)
            return {'status': 'error', 'message': str(e)}

    # --------------------------------------------------------------- search analytics
    async def analyze_keywords(
        self,
        site_url: str,
        start_date: str,
        end_date: str,
        country: Optional[str] = None,
        device: Optional[str] = None,
        page_filter: Optional[str] = None,
        query_filter: Optional[str] = None,
        row_limit: int = 100,
        sort_by: str = 'clicks',
    ) -> Dict[str, Any]:
        """Search performance grouped by query keyword."""
        sort_map = {
            'clicks': 'clicks',
            'impressions': 'impressions',
            'ctr': 'ctr',
            'position': 'position',
        }
        if sort_by not in sort_map:
            raise GSCAPIError(f"Invalid sort_by '{sort_by}'. Must be one of: {list(sort_map.keys())}")

        row_limit = min(max(1, row_limit), 25000)

        body = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['query'],
            'rowLimit': row_limit,
            'startRow': 0,
        }

        # Build dimension filters
        filters = []
        if country:
            filters.append({
                'dimension': 'country',
                'operator': 'equals',
                'expression': country.upper(),
            })
        if device:
            valid_devices = ('DESKTOP', 'MOBILE', 'TABLET')
            device_upper = device.upper()
            if device_upper not in valid_devices:
                raise GSCAPIError(f"Invalid device '{device}'. Must be one of: {valid_devices}")
            filters.append({
                'dimension': 'device',
                'operator': 'equals',
                'expression': device_upper,
            })
        if page_filter:
            filters.append({
                'dimension': 'page',
                'operator': 'contains',
                'expression': page_filter,
            })
        if query_filter:
            filters.append({
                'dimension': 'query',
                'operator': 'contains',
                'expression': query_filter,
            })

        if filters:
            body['dimensionFilterGroups'] = [{
                'groupType': 'and',
                'filters': filters,
            }]

        try:
            response = self._searchconsole().searchanalytics().query(
                siteUrl=site_url, body=body
            ).execute()
        except HttpError as e:
            raise GSCAPIError(f"analyze_keywords failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"analyze_keywords failed: {e}")

        rows = []
        for row in response.get('rows', []):
            keys = row.get('keys', [])
            rows.append({
                'query': keys[0] if keys else '',
                'clicks': row.get('clicks', 0),
                'impressions': row.get('impressions', 0),
                'ctr': round(row.get('ctr', 0), 4),
                'position': round(row.get('position', 0), 1),
            })

        # Sort
        reverse = sort_by != 'position'  # lower position = better
        rows.sort(key=lambda r: r.get(sort_by, 0), reverse=reverse)

        return {
            'status': 'success',
            'site_url': site_url,
            'date_range': {'start_date': start_date, 'end_date': end_date},
            'row_count': len(rows),
            'rows': rows,
        }

    # --------------------------------------------------------------- URL inspection
    async def inspect_url(self, site_url: str, inspection_url: str) -> Dict[str, Any]:
        """Get index status and rich result info for a specific URL."""
        body = {
            'inspectionUrl': inspection_url,
            'siteUrl': site_url,
        }
        try:
            response = self._searchconsole().urlInspection().index().inspect(
                body=body
            ).execute()
        except HttpError as e:
            raise GSCAPIError(f"inspect_url failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"inspect_url failed: {e}")

        result = response.get('inspectionResult', {})
        index_status = result.get('indexStatusResult', {})
        mobile = result.get('mobileUsabilityResult', {})
        rich = result.get('richResultsResult', {})

        return {
            'status': 'success',
            'inspection_url': inspection_url,
            'site_url': site_url,
            'index_status': {
                'verdict': index_status.get('verdict', ''),
                'coverage_state': index_status.get('coverageState', ''),
                'indexing_state': index_status.get('indexingState', ''),
                'last_crawl_time': index_status.get('lastCrawlTime', ''),
                'page_fetch_state': index_status.get('pageFetchState', ''),
                'robots_txt_state': index_status.get('robotsTxtState', ''),
                'crawled_as': index_status.get('crawledAs', ''),
                'referring_urls': index_status.get('referringUrls', []),
            },
            'mobile_usability': {
                'verdict': mobile.get('verdict', ''),
                'issues': mobile.get('issues', []),
            },
            'rich_results': {
                'verdict': rich.get('verdict', ''),
                'detected_items': rich.get('detectedItems', []),
            },
        }

    # --------------------------------------------------------------- sitemaps
    async def list_sitemaps(self, site_url: str) -> Dict[str, Any]:
        """List sitemaps for a property."""
        try:
            response = self._webmasters().sitemaps().list(siteUrl=site_url).execute()
        except HttpError as e:
            raise GSCAPIError(f"list_sitemaps failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"list_sitemaps failed: {e}")

        sitemaps = []
        for s in response.get('sitemap', []):
            sitemaps.append({
                'path': s.get('path', ''),
                'type': s.get('type', ''),
                'is_pending': s.get('isPending', False),
                'last_submitted': s.get('lastSubmitted', ''),
                'last_downloaded': s.get('lastDownloaded', ''),
                'warnings': int(s.get('warnings', 0)),
                'errors': int(s.get('errors', 0)),
            })

        return {'status': 'success', 'site_url': site_url, 'sitemaps': sitemaps, 'count': len(sitemaps)}

    async def submit_sitemap(self, site_url: str, sitemap_url: str) -> Dict[str, Any]:
        """Submit a new sitemap."""
        try:
            self._webmasters().sitemaps().submit(
                siteUrl=site_url, feedpath=sitemap_url
            ).execute()
            return {'status': 'success', 'site_url': site_url, 'sitemap_url': sitemap_url, 'message': 'Sitemap submitted'}
        except HttpError as e:
            raise GSCAPIError(f"submit_sitemap failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"submit_sitemap failed: {e}")

    async def delete_sitemap(self, site_url: str, sitemap_url: str) -> Dict[str, Any]:
        """Delete a sitemap."""
        try:
            self._webmasters().sitemaps().delete(
                siteUrl=site_url, feedpath=sitemap_url
            ).execute()
            return {'status': 'success', 'site_url': site_url, 'sitemap_url': sitemap_url, 'message': 'Sitemap deleted'}
        except HttpError as e:
            raise GSCAPIError(f"delete_sitemap failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"delete_sitemap failed: {e}")

    # --------------------------------------------------------------- coverage
    async def get_coverage(
        self,
        site_url: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Index coverage summary via searchanalytics with page dimension."""
        from datetime import datetime, timedelta

        if not end_date:
            end_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        body = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['page'],
            'rowLimit': 25000,
            'startRow': 0,
        }

        try:
            response = self._searchconsole().searchanalytics().query(
                siteUrl=site_url, body=body
            ).execute()
        except HttpError as e:
            raise GSCAPIError(f"get_coverage failed: {e.reason}", {'status_code': e.resp.status})
        except Exception as e:
            raise GSCAPIError(f"get_coverage failed: {e}")

        rows = response.get('rows', [])
        total_clicks = sum(r.get('clicks', 0) for r in rows)
        total_impressions = sum(r.get('impressions', 0) for r in rows)

        return {
            'status': 'success',
            'site_url': site_url,
            'date_range': {'start_date': start_date, 'end_date': end_date},
            'pages_with_data': len(rows),
            'total_clicks': total_clicks,
            'total_impressions': total_impressions,
            'note': 'Pages appearing in search results during the date range. Use GSC_inspect_url for detailed index status of specific URLs.',
        }
```

- [ ] **Step 2: Commit**

```bash
git add mcp_esa/services/google_searchconsole_service.py
git commit -m "feat(mcp): add Google Search Console service layer"
```

---

### Task 3: Tools — google_searchconsole_tools.py

**Files:**
- Create: `mcp_esa/tools/google_searchconsole_tools.py`

- [ ] **Step 1: Create the tools file**

Create `mcp_esa/tools/google_searchconsole_tools.py`:

```python
"""
Google Search Console (GSC) Tools Module
MCP tools for Search Console API operations.
"""

import logging
import json
from typing import Optional, Dict, TYPE_CHECKING

from mcp.server import Server

from mcp_esa.services.google_searchconsole_service import (
    GSCService,
    GSCConfig,
    GSCAPIError,
    GSC_AVAILABLE,
)
from mcp_esa.config.settings import get_settings

if TYPE_CHECKING:
    from mcp_esa.server.mcp_server import MCPServerApp

logger = logging.getLogger(__name__)


async def get_gsc_config() -> Optional[GSCConfig]:
    """Get GSC configuration from mcp.yaml + vault secrets."""
    settings = get_settings()
    if not settings.google_searchconsole_enabled:
        return None
    return GSCConfig(
        client_id=settings.google_searchconsole_client_id,
        client_secret=settings.google_searchconsole_client_secret,
        refresh_token=settings.google_searchconsole_refresh_token,
    )


def _json(payload) -> str:
    return json.dumps(payload, default=str, ensure_ascii=False)


def register_google_searchconsole_tools(server: Server, app: 'MCPServerApp') -> None:
    """Register all GSC tools with the MCP server."""

    if not hasattr(server, '_tool_handlers'):
        server._tool_handlers = {}

    logger.info("Registering Google Search Console tools")

    async def _service() -> GSCService:
        if not GSC_AVAILABLE:
            raise GSCAPIError(
                "GSC libraries not installed. Run: pip install google-api-python-client google-auth"
            )
        config = await get_gsc_config()
        if not config:
            raise GSCAPIError("Google Search Console not enabled in mcp.yaml")
        return GSCService(config)

    # =========================================================================
    # CONNECTION / DISCOVERY
    # =========================================================================

    async def gsc_test_connection(auth_context: Optional[Dict] = None) -> str:
        """Test Search Console API connectivity and list accessible properties."""
        try:
            svc = await _service()
            result = await svc.test_connection()
            return _json(result)
        except GSCAPIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GSC test_connection failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "Connection test failed. Check server logs."})

    async def gsc_list_sites(auth_context: Optional[Dict] = None) -> str:
        """List all verified Search Console properties with permission levels."""
        try:
            svc = await _service()
            return _json(await svc.list_sites())
        except GSCAPIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GSC list_sites failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "list_sites failed. Check server logs."})

    # =========================================================================
    # KEYWORD ANALYSIS (primary tool)
    # =========================================================================

    async def gsc_analyze_keywords(
        auth_context: Optional[Dict] = None,
        site_url: str = None,
        start_date: str = None,
        end_date: str = None,
        country: Optional[str] = None,
        device: Optional[str] = None,
        page_filter: Optional[str] = None,
        query_filter: Optional[str] = None,
        row_limit: int = 100,
        sort_by: str = 'clicks',
    ) -> str:
        """Analyze search keywords — clicks, impressions, CTR, position grouped by query."""
        try:
            if not site_url:
                return _json({"status": "error", "message": "site_url is required"})
            if not start_date or not end_date:
                return _json({"status": "error", "message": "start_date and end_date are required (YYYY-MM-DD)"})
            svc = await _service()
            return _json(await svc.analyze_keywords(
                site_url=site_url,
                start_date=start_date,
                end_date=end_date,
                country=country,
                device=device,
                page_filter=page_filter,
                query_filter=query_filter,
                row_limit=row_limit,
                sort_by=sort_by,
            ))
        except GSCAPIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GSC analyze_keywords failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "analyze_keywords failed. Check server logs."})

    # =========================================================================
    # URL INSPECTION
    # =========================================================================

    async def gsc_inspect_url(
        auth_context: Optional[Dict] = None,
        site_url: str = None,
        inspection_url: str = None,
    ) -> str:
        """Inspect a URL for index status, crawl info, mobile usability, and rich results."""
        try:
            if not site_url:
                return _json({"status": "error", "message": "site_url is required"})
            if not inspection_url:
                return _json({"status": "error", "message": "inspection_url is required"})
            svc = await _service()
            return _json(await svc.inspect_url(
                site_url=site_url,
                inspection_url=inspection_url,
            ))
        except GSCAPIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GSC inspect_url failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "inspect_url failed. Check server logs."})

    # =========================================================================
    # SITEMAPS
    # =========================================================================

    async def gsc_list_sitemaps(
        auth_context: Optional[Dict] = None,
        site_url: str = None,
    ) -> str:
        """List sitemaps registered for a Search Console property."""
        try:
            if not site_url:
                return _json({"status": "error", "message": "site_url is required"})
            svc = await _service()
            return _json(await svc.list_sitemaps(site_url=site_url))
        except GSCAPIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GSC list_sitemaps failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "list_sitemaps failed. Check server logs."})

    async def gsc_submit_sitemap(
        auth_context: Optional[Dict] = None,
        site_url: str = None,
        sitemap_url: str = None,
    ) -> str:
        """Submit a new sitemap to a Search Console property."""
        try:
            if not site_url:
                return _json({"status": "error", "message": "site_url is required"})
            if not sitemap_url:
                return _json({"status": "error", "message": "sitemap_url is required"})
            svc = await _service()
            return _json(await svc.submit_sitemap(site_url=site_url, sitemap_url=sitemap_url))
        except GSCAPIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GSC submit_sitemap failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "submit_sitemap failed. Check server logs."})

    async def gsc_delete_sitemap(
        auth_context: Optional[Dict] = None,
        site_url: str = None,
        sitemap_url: str = None,
    ) -> str:
        """Delete a sitemap from a Search Console property."""
        try:
            if not site_url:
                return _json({"status": "error", "message": "site_url is required"})
            if not sitemap_url:
                return _json({"status": "error", "message": "sitemap_url is required"})
            svc = await _service()
            return _json(await svc.delete_sitemap(site_url=site_url, sitemap_url=sitemap_url))
        except GSCAPIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GSC delete_sitemap failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "delete_sitemap failed. Check server logs."})

    # =========================================================================
    # COVERAGE
    # =========================================================================

    async def gsc_get_coverage(
        auth_context: Optional[Dict] = None,
        site_url: str = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> str:
        """Index coverage summary — pages appearing in search, total clicks/impressions."""
        try:
            if not site_url:
                return _json({"status": "error", "message": "site_url is required"})
            svc = await _service()
            return _json(await svc.get_coverage(
                site_url=site_url,
                start_date=start_date,
                end_date=end_date,
            ))
        except GSCAPIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GSC get_coverage failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "get_coverage failed. Check server logs."})

    # =========================================================================
    # INPUT SCHEMAS
    # =========================================================================

    _no_params = {"type": "object", "properties": {}, "required": []}
    _site_only = {
        "type": "object",
        "properties": {
            "site_url": {"type": "string", "description": "Search Console property URL (e.g. 'sc-domain:extraspace.com.sg' or 'https://www.extraspace.com.sg/')"},
        },
        "required": ["site_url"],
    }
    _site_and_sitemap = {
        "type": "object",
        "properties": {
            "site_url": {"type": "string", "description": "Search Console property URL"},
            "sitemap_url": {"type": "string", "description": "Full URL of the sitemap (e.g. 'https://www.extraspace.com.sg/sitemap.xml')"},
        },
        "required": ["site_url", "sitemap_url"],
    }

    gsc_test_connection._input_schema = _no_params
    gsc_list_sites._input_schema = _no_params

    gsc_analyze_keywords._input_schema = {
        "type": "object",
        "properties": {
            "site_url": {"type": "string", "description": "Search Console property URL (e.g. 'sc-domain:extraspace.com.sg')"},
            "start_date": {"type": "string", "description": "Start date YYYY-MM-DD"},
            "end_date": {"type": "string", "description": "End date YYYY-MM-DD"},
            "country": {"type": "string", "description": "3-letter ISO country code filter (e.g. 'SGP', 'MYS', 'KOR')"},
            "device": {"type": "string", "description": "Device filter: DESKTOP, MOBILE, or TABLET"},
            "page_filter": {"type": "string", "description": "URL substring to filter pages"},
            "query_filter": {"type": "string", "description": "Keyword substring to filter queries"},
            "row_limit": {"type": "integer", "description": "Max rows (1-25000)", "default": 100},
            "sort_by": {"type": "string", "description": "Sort by: clicks, impressions, ctr, position", "default": "clicks"},
        },
        "required": ["site_url", "start_date", "end_date"],
    }

    gsc_inspect_url._input_schema = {
        "type": "object",
        "properties": {
            "site_url": {"type": "string", "description": "Search Console property URL"},
            "inspection_url": {"type": "string", "description": "Full URL to inspect for index status"},
        },
        "required": ["site_url", "inspection_url"],
    }

    gsc_list_sitemaps._input_schema = _site_only
    gsc_submit_sitemap._input_schema = _site_and_sitemap
    gsc_delete_sitemap._input_schema = _site_and_sitemap

    gsc_get_coverage._input_schema = {
        "type": "object",
        "properties": {
            "site_url": {"type": "string", "description": "Search Console property URL"},
            "start_date": {"type": "string", "description": "Start date YYYY-MM-DD (default: 30 days ago)"},
            "end_date": {"type": "string", "description": "End date YYYY-MM-DD (default: 2 days ago)"},
        },
        "required": ["site_url"],
    }

    # =========================================================================
    # REGISTER
    # =========================================================================

    tools = {
        "GSC_test_connection": gsc_test_connection,
        "GSC_list_sites": gsc_list_sites,
        "GSC_analyze_keywords": gsc_analyze_keywords,
        "GSC_inspect_url": gsc_inspect_url,
        "GSC_list_sitemaps": gsc_list_sitemaps,
        "GSC_submit_sitemap": gsc_submit_sitemap,
        "GSC_delete_sitemap": gsc_delete_sitemap,
        "GSC_get_coverage": gsc_get_coverage,
    }

    for name, handler in tools.items():
        server._tool_handlers[name] = handler

    logger.info(f"Registered {len(tools)} Google Search Console tools")
```

- [ ] **Step 2: Commit**

```bash
git add mcp_esa/tools/google_searchconsole_tools.py
git commit -m "feat(mcp): add Google Search Console MCP tools (8 tools)"
```

---

### Task 4: Server Registration — mcp_server.py

**Files:**
- Modify: `mcp_esa/server/mcp_server.py`

- [ ] **Step 1: Add GSC registration block**

In `mcp_esa/server/mcp_server.py`, add after the SugarCRM registration block (after line 102):

```python
    # Register Google Search Console tools
    if settings.google_searchconsole_enabled:
        try:
            from mcp_esa.tools.google_searchconsole_tools import register_google_searchconsole_tools
            register_google_searchconsole_tools(server, app)
            logger.info("Google Search Console tools registered")
        except Exception as e:
            logger.warning(f"Failed to register Google Search Console tools: {e}")
```

- [ ] **Step 2: Commit**

```bash
git add mcp_esa/server/mcp_server.py
git commit -m "feat(mcp): register Google Search Console tools in server startup"
```

---

### Task 5: Refresh Token Minting Script

**Files:**
- Create: `scripts/mint_gsc_refresh_token.py`

- [ ] **Step 1: Create the minting script**

Create `scripts/mint_gsc_refresh_token.py`:

```python
#!/usr/bin/env python3
"""
Mint a Google Search Console OAuth refresh token.

One-time helper to obtain a refresh token scoped for the Search Console API.
Reuses the existing Google Ads OAuth client_id/client_secret (same Cloud project),
but the resulting refresh token is scope-locked to webmasters and stored
separately in the vault as GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN.

Usage:
    python scripts/mint_gsc_refresh_token.py

After consenting in the browser, the refresh token is printed. Save it via the
/admin/secrets UI under the key GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN.

Requirements:
    pip install google-auth-oauthlib
"""

import os
import sys
from pathlib import Path

# Make backend/python importable for vault access
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "backend" / "python"))

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print("ERROR: google-auth-oauthlib not installed.")
    print("Run: pip install google-auth-oauthlib")
    sys.exit(1)

SCOPES = ["https://www.googleapis.com/auth/webmasters"]
DEFAULT_CLIENT_ID = "387700327426-cmsrklgau8vqh9mnjvl95cc4vvenqj77.apps.googleusercontent.com"


def _load_secret_from_vault(key: str) -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv(REPO_ROOT / ".env")
        from common.config_loader import get_config
        return get_config().get_secret(key) or ""
    except Exception as e:
        print(f"WARN: vault lookup for {key} failed: {e}")
        return ""


def main() -> int:
    client_id = (
        os.environ.get("GSC_OAUTH_CLIENT_ID")
        or DEFAULT_CLIENT_ID
    )
    client_secret = (
        os.environ.get("GSC_OAUTH_CLIENT_SECRET")
        or _load_secret_from_vault("GOOGLE_ADS_CLIENT_SECRET")
        or input("OAuth client_secret: ").strip()
    )

    if not client_id or not client_secret:
        print("client_id and client_secret are required")
        return 1

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }

    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(port=0, prompt="consent", access_type="offline")

    if not creds.refresh_token:
        print("ERROR: No refresh token returned. Make sure you consented with prompt=consent.")
        return 1

    print("\n" + "=" * 70)
    print("SUCCESS — Search Console refresh token obtained.")
    print("=" * 70)
    print(f"\nRefresh token:\n{creds.refresh_token}\n")
    print("Next steps:")
    print("  1. Open the ESA Backend admin UI: /admin/secrets")
    print("  2. Add a new secret with key: GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN")
    print("  3. Paste the refresh token above as the value")
    print("  4. Restart the MCP server")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Commit**

```bash
git add scripts/mint_gsc_refresh_token.py
git commit -m "feat(mcp): add Search Console OAuth refresh token minting script"
```

---

### Task 6: Install Dependency + Smoke Test

- [ ] **Step 1: Install the dependency**

```bash
cd /home/louis/PycharmProjects/WebApp_BackendESA
pip install google-api-python-client>=2.100.0
```

- [ ] **Step 2: Verify import works**

```bash
python3 -c "from mcp_esa.services.google_searchconsole_service import GSCService, GSCConfig, GSCAPIError, GSC_AVAILABLE; print(f'GSC_AVAILABLE={GSC_AVAILABLE}')"
```

Expected: `GSC_AVAILABLE=True`

- [ ] **Step 3: Verify tools module imports**

```bash
python3 -c "from mcp_esa.tools.google_searchconsole_tools import register_google_searchconsole_tools; print('Tools module OK')"
```

Expected: `Tools module OK`

- [ ] **Step 4: Verify settings load the new config**

```bash
python3 -c "
from mcp_esa.config.settings import get_settings
s = get_settings()
print(f'enabled={s.google_searchconsole_enabled}')
print(f'client_id={s.google_searchconsole_client_id[:20]}...')
"
```

Expected: `enabled=True` and the client ID prefix.

- [ ] **Step 5: Verify server creates with GSC tools registered**

```bash
python3 -c "
from mcp_esa.server.mcp_server import create_mcp_server
app = create_mcp_server()
gsc_tools = [k for k in app.server._tool_handlers if k.startswith('GSC_')]
print(f'GSC tools: {gsc_tools}')
assert len(gsc_tools) == 8, f'Expected 8 GSC tools, got {len(gsc_tools)}'
print('All 8 GSC tools registered OK')
"
```

Expected: All 8 GSC tools listed and assertion passes.

---

### Task 7: Mint Refresh Token + Live Test

This task requires interactive browser auth — must be done manually.

- [ ] **Step 1: Run the minting script**

```bash
python scripts/mint_gsc_refresh_token.py
```

Consent in the browser when prompted. Copy the refresh token.

- [ ] **Step 2: Store the refresh token in vault**

Navigate to `/admin/secrets` in the ESA Backend UI. Add:
- Key: `GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN`
- Value: (the refresh token from step 1)

- [ ] **Step 3: Live test — test_connection**

Start the MCP server and call `GSC_test_connection`. Verify it returns `status: success` with a list of site URLs.

- [ ] **Step 4: Live test — analyze_keywords**

Call `GSC_analyze_keywords` with a known property and recent date range. Verify keyword data returns.
