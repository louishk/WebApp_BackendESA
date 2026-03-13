"""
MCP Server Settings for ESA Backend

Loads configuration from the backend's config_loader and vault system.
Reuses the same database URLs and secrets as the main Flask app.
"""

import logging
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings
from pydantic import Field

logger = logging.getLogger(__name__)

# Repo root (where .env lives) and backend root (for common/ imports)
REPO_ROOT = Path(__file__).parent.parent.parent          # /WebApp_BackendESA/
BACKEND_ROOT = REPO_ROOT / 'backend' / 'python'          # /WebApp_BackendESA/backend/python/

# Ensure backend/python is on sys.path for common.* imports (once at module load)
import sys
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


def load_environment():
    """Load .env from repo root (same as Flask app)."""
    try:
        from dotenv import load_dotenv
        env_path = REPO_ROOT / '.env'
        if env_path.exists():
            load_dotenv(env_path)
            logger.debug(f"Loaded .env from {env_path}")
    except ImportError:
        pass


def _get_database_url(db_name: str) -> str:
    """Build database URL using the backend's config_loader."""
    from common.config_loader import get_database_url
    return get_database_url(db_name)


class Settings(BaseSettings):
    """MCP Server configuration."""

    # Server
    mcp_server_name: str = Field(default="ESA Backend MCP", alias="MCP_SERVER_NAME")
    mcp_server_host: str = Field(default="127.0.0.1", alias="MCP_SERVER_HOST")
    mcp_server_port: int = Field(default=8002, alias="MCP_SERVER_PORT")
    mcp_debug: bool = Field(default=False, alias="MCP_DEBUG")

    # Paths
    project_root: str = Field(default=str(REPO_ROOT))

    class Config:
        env_file = str(REPO_ROOT / '.env')
        env_file_encoding = 'utf-8'
        extra = 'ignore'

    def get_backend_db_url(self) -> str:
        """Get esa_backend database URL (for API key auth)."""
        return _get_database_url('backend')

    def get_pbi_db_url(self) -> str:
        """Get esa_pbi database URL (for analytics tools)."""
        return _get_database_url('pbi')


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    load_environment()
    return Settings()
