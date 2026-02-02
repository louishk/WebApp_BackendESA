"""
Unified Configuration Loader for ESA Backend

Loads configuration from YAML files and resolves secrets from vault.
Provides a single source of truth for all application configuration.
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


def _load_root_env():
    """Load root .env file for bootstrap secrets (VAULT_MASTER_KEY)."""
    try:
        from dotenv import load_dotenv
        # Find root .env file
        current = Path(__file__).parent.parent.parent  # backend/
        root_env = current.parent / '.env'
        if root_env.exists():
            load_dotenv(root_env)
            logger.debug(f"Loaded root .env from {root_env}")
    except ImportError:
        pass  # dotenv not installed, rely on environment variables


# Load root .env on module import
_load_root_env()


class ConfigSection:
    """
    Dynamic configuration section that allows dot-notation access.
    Example: config.database.backend.host
    """

    def __init__(self, data: Dict[str, Any] = None, vault=None):
        self._data = data or {}
        self._vault = vault
        self._resolved_cache = {}

    def __getattr__(self, name: str) -> Any:
        if name.startswith('_'):
            return super().__getattribute__(name)

        if name not in self._data:
            return None

        value = self._data[name]

        # If it's a dict, wrap it in ConfigSection for nested access
        if isinstance(value, dict):
            return ConfigSection(value, self._vault)

        # If key ends with _vault, resolve from vault
        if isinstance(value, str) and name.endswith('_vault'):
            return self._resolve_vault(value)

        return value

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def _resolve_vault(self, vault_key: str) -> Optional[str]:
        """Resolve a vault reference to its actual value."""
        if vault_key in self._resolved_cache:
            return self._resolved_cache[vault_key]

        if self._vault is None:
            return None

        try:
            value = self._vault.get(vault_key)
            self._resolved_cache[vault_key] = value
            return value
        except Exception as e:
            logger.warning(f"Failed to resolve vault key {vault_key}: {e}")
            return None

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value with optional default."""
        value = getattr(self, key)
        return value if value is not None else default

    def get_secret(self, vault_key: str, default: Any = None) -> Any:
        """Get a secret directly from vault."""
        value = self._resolve_vault(vault_key)
        return value if value is not None else default

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (does not resolve vault references)."""
        return self._data.copy()

    def __repr__(self):
        return f"ConfigSection({list(self._data.keys())})"


class AppConfig:
    """
    Main application configuration.
    Loads from YAML files and integrates with vault for secrets.
    """

    def __init__(self, config_dir: str = None, vault_dir: str = None):
        """
        Initialize configuration.

        Args:
            config_dir: Path to config directory containing YAML files
            vault_dir: Path to vault directory for secrets
        """
        self._config_dir = Path(config_dir) if config_dir else self._find_config_dir()
        self._vault = None
        self._sections: Dict[str, ConfigSection] = {}

        # Initialize vault
        self._init_vault(vault_dir)

        # Load all config files
        self._load_configs()

    def _find_config_dir(self) -> Path:
        """Find config directory by searching from current location."""
        # Try relative to this file first
        base = Path(__file__).parent.parent.parent  # backend/
        config_path = base / "config"
        if config_path.exists():
            return config_path

        # Try from cwd
        config_path = Path.cwd() / "config"
        if config_path.exists():
            return config_path

        # Search upward
        current = Path.cwd()
        for _ in range(5):
            config_path = current / "backend" / "config"
            if config_path.exists():
                return config_path
            config_path = current / "config"
            if config_path.exists():
                return config_path
            if current.parent == current:
                break
            current = current.parent

        raise FileNotFoundError("Could not find config directory")

    def _init_vault(self, vault_dir: str = None):
        """Initialize the vault connection."""
        try:
            from common.secrets_vault import get_vault
            self._vault = get_vault(vault_dir)
            logger.info("Vault initialized successfully")
        except Exception as e:
            logger.warning(f"Vault not available: {e}. Secrets will not be resolved.")
            self._vault = None

    def _load_configs(self):
        """Load all YAML config files."""
        if not self._config_dir.exists():
            logger.warning(f"Config directory not found: {self._config_dir}")
            return

        for yaml_file in self._config_dir.glob("*.yaml"):
            section_name = yaml_file.stem  # filename without extension
            try:
                with open(yaml_file, 'r') as f:
                    data = yaml.safe_load(f) or {}
                self._sections[section_name] = ConfigSection(data, self._vault)
                logger.debug(f"Loaded config: {section_name}")
            except Exception as e:
                logger.error(f"Failed to load {yaml_file}: {e}")

    def __getattr__(self, name: str) -> ConfigSection:
        if name.startswith('_'):
            return super().__getattribute__(name)

        if name in self._sections:
            return self._sections[name]

        # Return empty section for missing configs
        return ConfigSection({}, self._vault)

    def reload(self):
        """Reload all configuration files."""
        self._sections.clear()
        self._load_configs()
        logger.info("Configuration reloaded")

    def get_secret(self, key: str, default: Any = None) -> Any:
        """Get a secret directly from vault."""
        if self._vault is None:
            return default
        try:
            value = self._vault.get(key)
            return value if value is not None else default
        except Exception:
            return default

    def set_secret(self, key: str, value: str) -> bool:
        """Set a secret in vault."""
        if self._vault is None:
            return False
        try:
            self._vault.set(key, value)
            return True
        except Exception as e:
            logger.error(f"Failed to set secret {key}: {e}")
            return False

    def delete_secret(self, key: str) -> bool:
        """Delete a secret from vault."""
        if self._vault is None:
            return False
        try:
            return self._vault.delete(key)
        except Exception as e:
            logger.error(f"Failed to delete secret {key}: {e}")
            return False

    def list_secrets(self) -> list:
        """List all secret keys in vault."""
        if self._vault is None:
            return []
        try:
            return self._vault.list_keys()
        except Exception:
            return []

    @property
    def vault_available(self) -> bool:
        """Check if vault is available."""
        return self._vault is not None

    def get_config_files(self) -> list:
        """List all loaded config files."""
        return list(self._sections.keys())

    def get_section(self, name: str) -> ConfigSection:
        """Get a config section by name."""
        return self._sections.get(name, ConfigSection({}, self._vault))

    def update_config(self, section: str, data: Dict[str, Any]) -> bool:
        """
        Update a config section and save to YAML file.

        Args:
            section: Config section name (e.g., 'app', 'database')
            data: New configuration data

        Returns:
            True if successful
        """
        yaml_file = self._config_dir / f"{section}.yaml"
        try:
            with open(yaml_file, 'w') as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)

            # Reload the section
            self._sections[section] = ConfigSection(data, self._vault)
            logger.info(f"Updated config: {section}")
            return True
        except Exception as e:
            logger.error(f"Failed to update config {section}: {e}")
            return False

    def get_raw_config(self, section: str) -> Dict[str, Any]:
        """Get raw config data for a section (for editing)."""
        yaml_file = self._config_dir / f"{section}.yaml"
        if yaml_file.exists():
            with open(yaml_file, 'r') as f:
                return yaml.safe_load(f) or {}
        return {}


# =============================================================================
# Singleton instance and convenience functions
# =============================================================================

_config_instance: Optional[AppConfig] = None


def get_config(config_dir: str = None, vault_dir: str = None) -> AppConfig:
    """
    Get or create the global config instance.

    Args:
        config_dir: Path to config directory (only used on first call)
        vault_dir: Path to vault directory (only used on first call)

    Returns:
        AppConfig instance
    """
    global _config_instance

    if _config_instance is None:
        _config_instance = AppConfig(config_dir, vault_dir)

    return _config_instance


def reload_config():
    """Reload the global configuration."""
    global _config_instance
    if _config_instance:
        _config_instance.reload()


# =============================================================================
# Helper functions for common config access patterns
# =============================================================================

def get_database_url(db_name: str = 'backend') -> str:
    """
    Build database URL from config.

    Args:
        db_name: Database section name ('backend' or 'pbi')

    Returns:
        PostgreSQL connection URL
    """
    config = get_config()
    db = getattr(config.database, db_name)

    if db is None:
        raise ValueError(f"Database config not found: {db_name}")

    # password_vault automatically resolves from vault due to _vault suffix
    password = db.password_vault
    if not password:
        # Get the raw vault key name for error message
        raw_data = config.get_raw_config('database')
        vault_key = raw_data.get(db_name, {}).get('password_vault', 'unknown')
        raise ValueError(f"Database password not found in vault for key: {vault_key}")

    return (
        f"postgresql://{db.username}:{password}"
        f"@{db.host}:{db.port}/{db.name}"
        f"?sslmode={db.sslmode}"
    )


def get_flask_config() -> Dict[str, Any]:
    """Get Flask configuration dictionary."""
    config = get_config()
    app_cfg = config.app

    # secret_key_vault automatically resolves from vault due to _vault suffix
    secret_key = app_cfg.flask.secret_key_vault if app_cfg.flask else None
    if not secret_key:
        # Generate a random key if not in vault
        import secrets
        secret_key = secrets.token_hex(32)
        logger.warning("Flask secret key not in vault, using random key")

    return {
        'SECRET_KEY': secret_key,
        'DEBUG': app_cfg.app.debug if app_cfg.app else False,
        'SESSION_COOKIE_SECURE': app_cfg.session.cookie_secure if app_cfg.session else False,
        'SESSION_COOKIE_HTTPONLY': app_cfg.session.cookie_httponly if app_cfg.session else True,
        'SESSION_COOKIE_SAMESITE': app_cfg.session.cookie_samesite if app_cfg.session else 'Lax',
    }
