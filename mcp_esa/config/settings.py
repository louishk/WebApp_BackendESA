"""
MCP Server Settings for ESA Backend

Loads configuration from backend/python/config/mcp.yaml via config_loader.
Secrets resolved from vault (app_secrets table).
"""

import logging
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

# Repo root (where .env lives) and backend root (for common/ imports)
REPO_ROOT = Path(__file__).parent.parent.parent          # /WebApp_BackendESA/
BACKEND_ROOT = REPO_ROOT / 'backend' / 'python'          # /WebApp_BackendESA/backend/python/

# Ensure backend/python is on sys.path for common.* imports (once at module load)
import sys
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


def _load_environment():
    """Load .env from repo root (bootstrap secrets: VAULT_MASTER_KEY, DB_PASSWORD)."""
    try:
        from dotenv import load_dotenv
        env_path = REPO_ROOT / '.env'
        if env_path.exists():
            load_dotenv(env_path)
            logger.debug(f"Loaded .env from {env_path}")
    except ImportError:
        pass


class Settings:
    """MCP Server configuration loaded from mcp.yaml."""

    def __init__(self):
        from common.config_loader import get_config
        self._config = get_config()
        self._mcp = self._config.get_raw_config('mcp')
        self._server = self._mcp.get('server', {})
        self._features = self._mcp.get('features', {})
        self._gads = self._mcp.get('google_ads', {})

    # Server
    @property
    def mcp_server_name(self) -> str:
        return self._server.get('name', 'ESA Backend MCP')

    @property
    def mcp_server_host(self) -> str:
        return self._server.get('host', '127.0.0.1')

    @property
    def mcp_server_port(self) -> int:
        return int(self._server.get('port', 8002))

    @property
    def mcp_debug(self) -> bool:
        return self._server.get('debug', False)

    @property
    def project_root(self) -> str:
        return str(REPO_ROOT)

    # Feature flags
    @property
    def database_enabled(self) -> bool:
        return self._features.get('database', True)

    @property
    def google_ads_enabled(self) -> bool:
        return self._features.get('google_ads', True)

    @property
    def revenue_enabled(self) -> bool:
        return self._features.get('revenue', True)

    # Google Ads (non-secret fields)
    @property
    def google_ads_client_id(self) -> str:
        return self._gads.get('client_id', '')

    @property
    def google_ads_login_customer_id(self) -> str:
        return self._gads.get('login_customer_id', '')

    # Google Ads secrets (resolved from vault)
    @property
    def google_ads_client_secret(self) -> str:
        vault_key = self._gads.get('client_secret_vault', 'GOOGLE_ADS_CLIENT_SECRET')
        return self._config.get_secret(vault_key) or ''

    @property
    def google_ads_developer_token(self) -> str:
        vault_key = self._gads.get('developer_token_vault', 'GOOGLE_ADS_DEVELOPER_TOKEN')
        return self._config.get_secret(vault_key) or ''

    @property
    def google_ads_refresh_token(self) -> str:
        vault_key = self._gads.get('refresh_token_vault', 'GOOGLE_ADS_REFRESH_TOKEN')
        return self._config.get_secret(vault_key) or ''

    # Database URLs (for auth middleware)
    def get_backend_db_url(self) -> str:
        from common.config_loader import get_database_url
        return get_database_url('backend')

    def get_pbi_db_url(self) -> str:
        from common.config_loader import get_database_url
        return get_database_url('pbi')


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    _load_environment()
    return Settings()
