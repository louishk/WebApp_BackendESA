#!/usr/bin/env python3
"""
ESA Backend MCP Server — Streamable HTTP Transport

Independent MCP server for the ESA Backend platform.
Authenticates via X-API-Key against the backend's api_keys table.

Usage:
    python main.py
    MCP_SERVER_PORT=8002 python main.py

Endpoints:
    GET  /        - Server information
    GET  /health  - Health check
    POST /mcp     - MCP JSON-RPC endpoint (requires X-API-Key)
"""

import sys
import logging
from pathlib import Path

# Add repo root (for mcp_esa/) and backend/python (for common/) to path
REPO_ROOT = Path(__file__).parent.parent
BACKEND_ROOT = REPO_ROOT / 'backend' / 'python'
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(BACKEND_ROOT))

import uvicorn

from mcp_esa.config.settings import get_settings
from mcp_esa.server.mcp_server import create_mcp_server
from mcp_esa.server.transport import create_starlette_app


def setup_logging(debug: bool = False) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def main() -> None:
    settings = get_settings()  # calls load_environment() internally via lru_cache
    setup_logging(settings.mcp_debug)
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("ESA Backend MCP Server — Streamable HTTP")
    logger.info("=" * 60)

    # Create MCP server with tools
    mcp_app = create_mcp_server(settings)

    # Create Starlette app with transport + auth
    app = create_starlette_app(mcp_app)

    logger.info(f"Server: {settings.mcp_server_name}")
    logger.info(f"Listen: {settings.mcp_server_host}:{settings.mcp_server_port}")
    logger.info(f"Auth:   X-API-Key (esa_backend DB)")
    logger.info(f"Debug:  {settings.mcp_debug}")
    logger.info("-" * 60)
    logger.info(f"  Info:   http://{settings.mcp_server_host}:{settings.mcp_server_port}/")
    logger.info(f"  Health: http://{settings.mcp_server_host}:{settings.mcp_server_port}/health")
    logger.info(f"  MCP:    http://{settings.mcp_server_host}:{settings.mcp_server_port}/mcp")
    logger.info("=" * 60)

    uvicorn.run(
        app,
        host=settings.mcp_server_host,
        port=settings.mcp_server_port,
        log_level="debug" if settings.mcp_debug else "info",
        access_log=settings.mcp_debug,
    )


if __name__ == "__main__":
    main()
