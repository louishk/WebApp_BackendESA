"""
MCP Health Tools for ESA Backend

Basic health check and connectivity validation tools.
Always registered — serves as the foundation for tool registration pattern.
"""

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from mcp.server import Server  # MCP SDK

if TYPE_CHECKING:
    from mcp_esa.server.mcp_server import MCPServerApp

logger = logging.getLogger(__name__)


def register_health_tools(server: Server, app: 'MCPServerApp') -> None:
    """Register health check tools with the MCP server."""

    if not hasattr(server, '_tool_handlers'):
        server._tool_handlers = {}

    async def health_check() -> str:
        """Check MCP server health and return diagnostic information"""
        uptime = app.get_uptime()
        tool_count = len(server._tool_handlers) if hasattr(server, '_tool_handlers') else 0

        return f"""ESA Backend MCP Server Health Check
=====================================
Status: HEALTHY
Server: {app.settings.mcp_server_name}
Uptime: {uptime}
Timestamp: {datetime.now().isoformat()}
Registered Tools: {tool_count}
Transport: Streamable HTTP
Protocol: 2024-11-05
Auth: X-API-Key (esa_backend DB)
"""

    async def echo(message: str) -> str:
        """Echo a message back for testing connectivity"""
        if len(message) > 1024:
            return "Error: message too long (max 1024 characters)"
        return f"Echo [{datetime.now().isoformat()}]: {message}"

    async def ping() -> str:
        """Simple ping/pong test"""
        return "pong"

    # Input schemas
    health_check._input_schema = {"type": "object", "properties": {}, "required": []}
    echo._input_schema = {
        "type": "object",
        "properties": {"message": {"type": "string", "description": "The message to echo back", "maxLength": 1024}},
        "required": ["message"],
    }
    ping._input_schema = {"type": "object", "properties": {}, "required": []}

    # Register
    server._tool_handlers["health_check"] = health_check
    server._tool_handlers["echo"] = echo
    server._tool_handlers["ping"] = ping

    logger.info("Health tools registered: health_check, echo, ping")
