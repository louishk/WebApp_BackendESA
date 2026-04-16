"""
Google Analytics 4 (GA4) Tools Module
MCP tools for Google Analytics 4 Data + Admin API operations.
"""

import logging
import json
from typing import Optional, Dict, List, TYPE_CHECKING

from mcp.server import Server

from mcp_esa.services.google_analytics4_service import (
    GA4Service,
    GA4Config,
    GA4APIError,
    GA4_AVAILABLE,
)
from mcp_esa.config.settings import get_settings

if TYPE_CHECKING:
    from mcp_esa.server.mcp_server import MCPServerApp

logger = logging.getLogger(__name__)


async def get_ga4_config() -> Optional[GA4Config]:
    """Get GA4 configuration from mcp.yaml + vault secrets."""
    settings = get_settings()
    if not settings.google_analytics4_enabled:
        return None
    return GA4Config(
        client_id=settings.google_analytics4_client_id,
        client_secret=settings.google_analytics4_client_secret,
        refresh_token=settings.google_analytics4_refresh_token,
    )


def _json(payload) -> str:
    return json.dumps(payload, default=str, ensure_ascii=False)


def register_google_analytics4_tools(server: Server, app: 'MCPServerApp') -> None:
    """Register all GA4 tools with the MCP server."""

    if not hasattr(server, '_tool_handlers'):
        server._tool_handlers = {}

    logger.info("Registering Google Analytics 4 tools")

    _cached_service: Optional[GA4Service] = None

    async def _service() -> GA4Service:
        nonlocal _cached_service
        if _cached_service is not None:
            return _cached_service
        if not GA4_AVAILABLE:
            raise GA4APIError(
                "GA4 libraries not installed. Run: pip install google-analytics-data google-analytics-admin"
            )
        config = await get_ga4_config()
        if not config:
            raise GA4APIError("GA4 not enabled in mcp.yaml")
        _cached_service = GA4Service(config)
        return _cached_service

    # =========================================================================
    # ACCOUNT / DISCOVERY
    # =========================================================================

    async def ga4_test_connection(auth_context: Optional[Dict] = None) -> str:
        try:
            svc = await _service()
            result = await svc.test_connection()
            return _json(result)
        except GA4APIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GA4 test_connection failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "Connection test failed. Check server logs."})

    async def ga4_list_properties(auth_context: Optional[Dict] = None) -> str:
        try:
            svc = await _service()
            return _json(await svc.list_properties())
        except GA4APIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GA4 list_properties failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "list_properties failed. Check server logs."})

    async def ga4_get_metadata(
        auth_context: Optional[Dict] = None,
        property_id: str = None,
    ) -> str:
        try:
            if not property_id:
                return _json({"status": "error", "message": "property_id is required"})
            svc = await _service()
            return _json(await svc.get_metadata(property_id))
        except GA4APIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GA4 get_metadata failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "get_metadata failed. Check server logs."})

    # =========================================================================
    # GENERIC REPORTS
    # =========================================================================

    async def ga4_run_report(
        auth_context: Optional[Dict] = None,
        property_id: str = None,
        dimensions: Optional[List[str]] = None,
        metrics: Optional[List[str]] = None,
        start_date: str = "7daysAgo",
        end_date: str = "today",
        limit: int = 100,
        order_by_metric: Optional[str] = None,
        order_desc: bool = True,
    ) -> str:
        try:
            if not property_id:
                return _json({"status": "error", "message": "property_id is required"})
            if not metrics:
                return _json({"status": "error", "message": "metrics is required (list of GA4 metric API names)"})
            svc = await _service()
            return _json(await svc.run_report(
                property_id=property_id,
                dimensions=dimensions or [],
                metrics=metrics,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                order_by_metric=order_by_metric,
                order_desc=order_desc,
            ))
        except GA4APIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GA4 run_report failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "run_report failed. Check server logs."})

    async def ga4_run_realtime(
        auth_context: Optional[Dict] = None,
        property_id: str = None,
        dimensions: Optional[List[str]] = None,
        metrics: Optional[List[str]] = None,
        limit: int = 100,
    ) -> str:
        try:
            if not property_id:
                return _json({"status": "error", "message": "property_id is required"})
            svc = await _service()
            return _json(await svc.run_realtime(
                property_id=property_id,
                dimensions=dimensions,
                metrics=metrics,
                limit=limit,
            ))
        except GA4APIError as e:
            return _json({"status": "error", "message": str(e)})
        except Exception as e:
            logger.error(f"GA4 run_realtime failed: {e}", exc_info=True)
            return _json({"status": "error", "message": "run_realtime failed. Check server logs."})

    # =========================================================================
    # PRE-BUILT REPORTS
    # =========================================================================

    def _make_prebuilt(method_name: str, label: str):
        async def _tool(
            auth_context: Optional[Dict] = None,
            property_id: str = None,
            start_date: str = "7daysAgo",
            end_date: str = "today",
            limit: int = 25,
        ) -> str:
            try:
                if not property_id:
                    return _json({"status": "error", "message": "property_id is required"})
                svc = await _service()
                method = getattr(svc, method_name)
                return _json(await method(
                    property_id=property_id,
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit,
                ))
            except GA4APIError as e:
                return _json({"status": "error", "message": str(e)})
            except Exception as e:
                logger.error(f"GA4 {label} failed: {e}", exc_info=True)
                return _json({"status": "error", "message": f"{label} failed. Check server logs."})
        _tool.__name__ = f"ga4_{method_name}"
        return _tool

    ga4_top_pages = _make_prebuilt("top_pages", "top_pages")
    ga4_traffic_sources = _make_prebuilt("traffic_sources", "traffic_sources")
    ga4_user_acquisition = _make_prebuilt("user_acquisition", "user_acquisition")
    ga4_conversions = _make_prebuilt("conversions", "conversions")
    ga4_device_breakdown = _make_prebuilt("device_breakdown", "device_breakdown")
    ga4_geo_breakdown = _make_prebuilt("geo_breakdown", "geo_breakdown")

    # =========================================================================
    # INPUT SCHEMAS
    # =========================================================================

    _no_params = {"type": "object", "properties": {}, "required": []}
    _property_only = {
        "type": "object",
        "properties": {
            "property_id": {"type": "string", "description": "GA4 property ID (e.g. '123456789' or 'properties/123456789')"}
        },
        "required": ["property_id"],
    }
    _prebuilt_schema = {
        "type": "object",
        "properties": {
            "property_id": {"type": "string", "description": "GA4 property ID"},
            "start_date": {"type": "string", "description": "Start date YYYY-MM-DD or relative (e.g. '7daysAgo')", "default": "7daysAgo"},
            "end_date": {"type": "string", "description": "End date YYYY-MM-DD or 'today'", "default": "today"},
            "limit": {"type": "integer", "description": "Max rows to return", "default": 25},
        },
        "required": ["property_id"],
    }

    ga4_test_connection._input_schema = _no_params
    ga4_list_properties._input_schema = _no_params
    ga4_get_metadata._input_schema = _property_only

    ga4_run_report._input_schema = {
        "type": "object",
        "properties": {
            "property_id": {"type": "string", "description": "GA4 property ID"},
            "dimensions": {"type": "array", "items": {"type": "string"}, "description": "GA4 dimension API names (e.g. ['country','deviceCategory'])"},
            "metrics": {"type": "array", "items": {"type": "string"}, "description": "GA4 metric API names (e.g. ['activeUsers','sessions'])"},
            "start_date": {"type": "string", "description": "Start date or relative", "default": "7daysAgo"},
            "end_date": {"type": "string", "description": "End date or 'today'", "default": "today"},
            "limit": {"type": "integer", "description": "Max rows", "default": 100},
            "order_by_metric": {"type": "string", "description": "Metric to sort by (optional)"},
            "order_desc": {"type": "boolean", "description": "Sort descending", "default": True},
        },
        "required": ["property_id", "metrics"],
    }
    ga4_run_realtime._input_schema = {
        "type": "object",
        "properties": {
            "property_id": {"type": "string", "description": "GA4 property ID"},
            "dimensions": {"type": "array", "items": {"type": "string"}, "description": "Realtime dimensions (default ['country'])"},
            "metrics": {"type": "array", "items": {"type": "string"}, "description": "Realtime metrics (default ['activeUsers'])"},
            "limit": {"type": "integer", "description": "Max rows", "default": 100},
        },
        "required": ["property_id"],
    }

    for fn in (ga4_top_pages, ga4_traffic_sources, ga4_user_acquisition,
               ga4_conversions, ga4_device_breakdown, ga4_geo_breakdown):
        fn._input_schema = _prebuilt_schema

    # =========================================================================
    # REGISTER
    # =========================================================================

    tools = {
        "GA4_test_connection": ga4_test_connection,
        "GA4_list_properties": ga4_list_properties,
        "GA4_get_metadata": ga4_get_metadata,
        "GA4_run_report": ga4_run_report,
        "GA4_run_realtime": ga4_run_realtime,
        "GA4_top_pages": ga4_top_pages,
        "GA4_traffic_sources": ga4_traffic_sources,
        "GA4_user_acquisition": ga4_user_acquisition,
        "GA4_conversions": ga4_conversions,
        "GA4_device_breakdown": ga4_device_breakdown,
        "GA4_geo_breakdown": ga4_geo_breakdown,
    }

    for name, handler in tools.items():
        server._tool_handlers[name] = handler

    logger.info(f"Registered {len(tools)} Google Analytics 4 tools")
