"""
MCP Server Setup for ESA Backend

Creates and configures the MCP server with tool registration.
Tools are registered via register_*_tools() functions in the tools/ directory.
"""

import logging
from datetime import datetime
from typing import Optional

from mcp.server import Server  # MCP SDK

from mcp_esa.config.settings import Settings, get_settings

logger = logging.getLogger(__name__)


class MCPServerApp:
    """MCP Server Application wrapper for ESA Backend."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self.server = Server(self.settings.mcp_server_name)
        self._startup_time = datetime.now()

    @property
    def startup_time(self) -> datetime:
        return self._startup_time

    def get_uptime(self) -> str:
        delta = datetime.now() - self._startup_time
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours}h {minutes}m {seconds}s"


def create_mcp_server(settings: Optional[Settings] = None) -> MCPServerApp:
    """Create and configure the MCP server with registered tools."""

    settings = settings or get_settings()
    app = MCPServerApp(settings)
    server = app.server

    # Register core health tools (always enabled)
    from mcp_esa.tools.health_tools import register_health_tools
    register_health_tools(server, app)
    logger.info("Health tools registered")

    # Register database tools
    if settings.database_enabled:
        try:
            from mcp_esa.tools.database_tools import register_database_tools
            register_database_tools(server, app)
            logger.info("Database tools registered")
        except Exception as e:
            logger.warning(f"Failed to register database tools: {e}")

    # Register Google Ads tools
    if settings.google_ads_enabled:
        try:
            from mcp_esa.tools.google_ads_tools import register_google_ads_tools
            register_google_ads_tools(server, app)
            logger.info("Google Ads tools registered")
        except Exception as e:
            logger.warning(f"Failed to register Google Ads tools: {e}")

    tool_count = len(server._tool_handlers) if hasattr(server, '_tool_handlers') else 0
    logger.info(f"MCP Server '{settings.mcp_server_name}' created with {tool_count} tools")
    return app
