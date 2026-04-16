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
        self._ga4 = self._mcp.get('google_analytics4', {})
        self._gsc = self._mcp.get('google_searchconsole', {})
        self._sugarcrm = self._mcp.get('sugarcrm', {})
        self._ms_oauth = self._mcp.get('microsoft_oauth', {})
        self._naver = self._mcp.get('naver_searchad', {})

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

    @property
    def naver_searchad_enabled(self) -> bool:
        return self._features.get('naver_searchad', True)

    # Naver Search Ad — vault-first, env fallback (for local endpoint testing)
    def _vault_or_env(self, vault_key: str, env_key: str) -> str:
        import os
        val = self._config.get_secret(vault_key) if vault_key else None
        return val or os.getenv(env_key, '') or ''

    @property
    def naver_searchad_base_url(self) -> str:
        return self._naver.get('base_url', 'https://api.searchad.naver.com')

    @property
    def naver_searchad_api_key(self) -> str:
        return self._vault_or_env(
            self._naver.get('api_key_vault', 'NAVER_SEARCHAD_API_KEY'),
            'NAVER_SEARCHAD_API_KEY',
        )

    @property
    def naver_searchad_secret_key(self) -> str:
        return self._vault_or_env(
            self._naver.get('secret_key_vault', 'NAVER_SEARCHAD_SECRET_KEY'),
            'NAVER_SEARCHAD_SECRET_KEY',
        )

    @property
    def naver_searchad_customer_id(self) -> str:
        return self._vault_or_env(
            self._naver.get('customer_id_vault', 'NAVER_SEARCHAD_CUSTOMER_ID'),
            'NAVER_SEARCHAD_CUSTOMER_ID',
        )

    # Microsoft OAuth (for claude.ai Enterprise SSO)
    @property
    def ms_oauth_enabled(self) -> bool:
        return self._ms_oauth.get('enabled', False)

    @property
    def ms_oauth_client_id(self) -> str:
        return self._ms_oauth.get('client_id', '')

    @property
    def ms_oauth_client_secret(self) -> str:
        vault_key = self._ms_oauth.get('client_secret_vault', 'MS_OAUTH_CLIENT_SECRET')
        return self._config.get_secret(vault_key) or ''

    @property
    def ms_oauth_tenant_id(self) -> str:
        return self._ms_oauth.get('tenant_id', 'common')

    @property
    def ms_oauth_redirect_uri(self) -> str:
        return self._ms_oauth.get('redirect_uri', '')

    @property
    def ms_oauth_allowed_domains(self) -> list:
        return self._ms_oauth.get('allowed_domains', ['extraspaceasia.com'])

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

    # Google Analytics 4
    @property
    def google_analytics4_enabled(self) -> bool:
        return self._features.get('google_analytics4', False)

    @property
    def google_analytics4_client_id(self) -> str:
        # Falls back to the Google Ads client_id (same OAuth app)
        return self._ga4.get('client_id') or self._gads.get('client_id', '')

    @property
    def google_analytics4_client_secret(self) -> str:
        vault_key = self._ga4.get('client_secret_vault', 'GOOGLE_ADS_CLIENT_SECRET')
        return self._config.get_secret(vault_key) or ''

    @property
    def google_analytics4_refresh_token(self) -> str:
        vault_key = self._ga4.get('refresh_token_vault', 'GOOGLE_ANALYTICS_REFRESH_TOKEN')
        return self._config.get_secret(vault_key) or ''

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

    # SugarCRM feature flag
    @property
    def sugarcrm_enabled(self) -> bool:
        return self._features.get('sugarcrm', False)

    # SugarCRM (non-secret fields)
    @property
    def sugarcrm_url(self) -> str:
        return self._sugarcrm.get('url', '')

    @property
    def sugarcrm_username(self) -> str:
        return self._sugarcrm.get('username', '')

    @property
    def sugarcrm_client_id(self) -> str:
        return self._sugarcrm.get('client_id', 'sugar')

    @property
    def sugarcrm_platform(self) -> str:
        return self._sugarcrm.get('platform', 'mobile')

    @property
    def sugarcrm_api_version(self) -> str:
        return self._sugarcrm.get('api_version', 'v11')

    @property
    def sugarcrm_timeout(self) -> int:
        return int(self._sugarcrm.get('timeout', 30))

    # SugarCRM secrets (resolved from vault)
    @property
    def sugarcrm_password(self) -> str:
        vault_key = self._sugarcrm.get('password_vault', 'SUGARCRM_PASSWORD')
        return self._config.get_secret(vault_key) or ''

    @property
    def sugarcrm_client_secret(self) -> str:
        vault_key = self._sugarcrm.get('client_secret_vault', 'SUGARCRM_CLIENT_SECRET')
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
