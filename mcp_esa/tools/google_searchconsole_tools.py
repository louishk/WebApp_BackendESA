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
