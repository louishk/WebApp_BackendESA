"""
MCP Streamable HTTP Transport for ESA Backend

Implements the MCP Streamable HTTP transport specification (protocol 2024-11-05).
Single /mcp endpoint for all JSON-RPC operations.
"""

import json
import logging
import asyncio
from typing import AsyncIterator, List
from datetime import datetime

from starlette.applications import Starlette
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import Response, JSONResponse, StreamingResponse
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from mcp_esa.server.mcp_server import MCPServerApp
from mcp_esa.config.settings import get_settings

logger = logging.getLogger(__name__)


class StreamableHTTPTransport:
    """
    Streamable HTTP Transport for MCP.

    Handles JSON-RPC requests on /mcp with optional SSE streaming.
    """

    def __init__(self, mcp_app: MCPServerApp):
        self.mcp_app = mcp_app
        self.server = mcp_app.server
        self.settings = mcp_app.settings

    async def handle_mcp_request(self, request: Request) -> Response:
        # Extract per-request allowed tools from auth middleware (NOT stored on self)
        allowed_tools = getattr(request.state, 'mcp_tools', []) if hasattr(request, 'state') else []

        if request.method == "POST":
            return await self._handle_post(request, allowed_tools)
        elif request.method == "GET":
            return await self._handle_get_stream(request)
        elif request.method == "OPTIONS":
            return Response(status_code=204)
        else:
            return JSONResponse({"error": "Method not allowed"}, status_code=405)

    async def _handle_post(self, request: Request, allowed_tools: List[str]) -> Response:
        try:
            body = await request.body()
            if not body:
                return JSONResponse(
                    {"jsonrpc": "2.0", "error": {"code": -32700, "message": "Empty request body"}, "id": None},
                    status_code=400,
                )

            data = json.loads(body)
            logger.debug(f"MCP request: {data.get('method', 'unknown')}")

            accept = request.headers.get("accept", "")
            if "text/event-stream" in accept:
                return await self._handle_streaming_response(data, allowed_tools)

            response = await self._process_jsonrpc(data, allowed_tools)
            return JSONResponse(response)

        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse error: {e}")
            return JSONResponse(
                {"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error: invalid JSON"}, "id": None},
                status_code=400,
            )
        except Exception as e:
            logger.error(f"MCP request error: {e}", exc_info=True)
            return JSONResponse(
                {"jsonrpc": "2.0", "error": {"code": -32603, "message": "Internal error"}, "id": None},
                status_code=500,
            )

    async def _handle_get_stream(self, request: Request) -> Response:
        async def event_stream() -> AsyncIterator[str]:
            yield f"data: {json.dumps({'type': 'connected', 'timestamp': datetime.now().isoformat()})}\n\n"
            while True:
                await asyncio.sleep(30)
                yield f"data: {json.dumps({'type': 'ping', 'timestamp': datetime.now().isoformat()})}\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )

    async def _handle_streaming_response(self, data: dict, allowed_tools: List[str]) -> Response:
        async def event_stream() -> AsyncIterator[str]:
            response = await self._process_jsonrpc(data, allowed_tools)
            yield f"data: {json.dumps(response)}\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )

    async def _process_jsonrpc(self, data: dict, allowed_tools: List[str]) -> dict:
        method = data.get("method", "")
        params = data.get("params", {})
        request_id = data.get("id")

        try:
            if method == "initialize":
                result = await self._handle_initialize(params)
            elif method == "tools/list":
                result = await self._handle_tools_list(params, allowed_tools)
            elif method == "tools/call":
                result = await self._handle_tools_call(params, allowed_tools)
            elif method == "resources/list":
                result = {"resources": []}
            elif method == "prompts/list":
                result = {"prompts": []}
            elif method == "ping":
                result = {"pong": True}
            else:
                return {
                    "jsonrpc": "2.0",
                    "error": {"code": -32601, "message": f"Method not found: {method}"},
                    "id": request_id,
                }

            return {"jsonrpc": "2.0", "result": result, "id": request_id}

        except Exception as e:
            logger.error(f"Error processing {method}: {e}", exc_info=True)
            return {
                "jsonrpc": "2.0",
                "error": {"code": -32603, "message": "Internal error"},
                "id": request_id,
            }

    async def _handle_initialize(self, params: dict) -> dict:
        client = params.get("clientInfo", {}).get("name", "unknown")
        logger.info(f"MCP initialize from: {client}")

        return {
            "protocolVersion": "2024-11-05",
            "serverInfo": {
                "name": self.settings.mcp_server_name,
                "version": "1.0.0",
            },
            "capabilities": {
                "tools": {"listChanged": True},
                "resources": {"subscribe": False, "listChanged": False},
                "prompts": {"listChanged": False},
                "logging": {},
            },
        }

    async def _handle_tools_list(self, params: dict, allowed_tools: List[str]) -> dict:
        tools = []
        if hasattr(self.server, '_tool_handlers'):
            for name, handler in self.server._tool_handlers.items():
                if allowed_tools and name not in allowed_tools:
                    continue
                tools.append({
                    "name": name,
                    "description": getattr(handler, '__doc__', '') or f"Tool: {name}",
                    "inputSchema": getattr(handler, '_input_schema', {"type": "object", "properties": {}}),
                })
        return {"tools": tools}

    async def _handle_tools_call(self, params: dict, allowed_tools: List[str]) -> dict:
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        if not tool_name:
            raise ValueError("Tool name is required")

        # Check per-key tool access
        if allowed_tools and tool_name not in allowed_tools:
            return {
                "content": [{"type": "text", "text": f"Access denied: tool '{tool_name}' is not allowed for this API key"}],
                "isError": True,
            }

        logger.info(f"Calling tool: {tool_name}")

        if hasattr(self.server, '_tool_handlers') and tool_name in self.server._tool_handlers:
            handler = self.server._tool_handlers[tool_name]
            try:
                result = await handler(**arguments)
                return {"content": [{"type": "text", "text": str(result)}], "isError": False}
            except Exception as e:
                logger.error(f"Tool {tool_name} failed: {e}")
                return {"content": [{"type": "text", "text": f"Tool '{tool_name}' encountered an error"}], "isError": True}
        else:
            raise ValueError(f"Unknown tool: {tool_name}")


async def health_check(request: Request) -> Response:
    return JSONResponse({
        "status": "healthy",
        "server": "esa-backend-mcp",
        "timestamp": datetime.now().isoformat(),
        "transport": "streamable-http",
    })


async def server_info(request: Request) -> Response:
    settings = get_settings()
    return JSONResponse({
        "name": settings.mcp_server_name,
        "version": "1.0.0",
        "transport": "streamable-http",
        "protocol_version": "2024-11-05",
        "auth": {"type": "X-API-Key"},
        "endpoints": {"mcp": "/mcp", "health": "/health"},
    })


def create_starlette_app(mcp_app: MCPServerApp) -> Starlette:
    """Create the Starlette application with Streamable HTTP transport and API key auth."""

    transport = StreamableHTTPTransport(mcp_app)

    routes = [
        Route("/", endpoint=server_info, methods=["GET"]),
        Route("/health", endpoint=health_check, methods=["GET"]),
        Route("/mcp", endpoint=transport.handle_mcp_request, methods=["GET", "POST", "OPTIONS"]),
    ]

    middleware = [
        Middleware(
            CORSMiddleware,
            allow_origins=["https://esa-backend.extraspaceasia.com"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        ),
    ]

    # API key authentication middleware
    from mcp_esa.server.auth import ApiKeyAuthMiddleware
    middleware.append(Middleware(ApiKeyAuthMiddleware))
    logger.info("API key authentication middleware enabled")

    app = Starlette(
        debug=mcp_app.settings.mcp_debug,
        routes=routes,
        middleware=middleware,
    )

    logger.info("Starlette app created with Streamable HTTP transport")
    return app
