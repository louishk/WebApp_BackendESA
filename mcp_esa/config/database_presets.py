"""
Database Preset Configuration for ESA MCP Server
Loads database presets from backend/python/config/mcp.yaml
Passwords resolved from vault via config_loader's _vault suffix convention.
"""

import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration"""
    type: str
    # Traditional DB fields
    host: str = None
    port: int = None
    database: str = None
    user: str = None
    password: str = None
    ssl: bool = False
    nickname: Optional[str] = None
    # BigQuery-specific fields
    project_id: Optional[str] = None
    credentials_path: Optional[str] = None
    credentials_json: Optional[str] = None
    dataset: Optional[str] = None
    location: Optional[str] = None

    def to_connection_string(self, include_password: bool = False) -> str:
        """Generate a connection string (optionally without password for logging)"""
        if self.type == 'bigquery':
            dataset_str = f"/{self.dataset}" if self.dataset else ""
            creds_str = " (vault)" if self.credentials_json else " (service account)" if self.credentials_path else " (ADC)"
            return f"bigquery://{self.project_id}{dataset_str}{creds_str}"

        pw = self.password if include_password else "***"
        return f"{self.type}://{self.user}:{pw}@{self.host}:{self.port}/{self.database}"


class DatabasePresets:
    """Database presets loaded from mcp.yaml config."""

    def __init__(self):
        self._presets: Dict[str, DatabaseConfig] = {}
        self._loaded = False

    def _ensure_loaded(self):
        """Lazy-load presets from config on first access."""
        if self._loaded:
            return

        try:
            from common.config_loader import get_config
            from common.secrets_vault import vault_config
            config = get_config()
            raw = config.get_raw_config('mcp')
            databases = raw.get('databases', {})

            for name, db in databases.items():
                db_type = db.get('type', 'postgresql')

                if db_type == 'bigquery':
                    # Resolve credentials from vault
                    creds_json = None
                    vault_key = db.get('credentials_json_vault')
                    if vault_key:
                        creds_json = vault_config(vault_key)

                    self._presets[name] = DatabaseConfig(
                        type='bigquery',
                        project_id=db.get('project_id'),
                        credentials_json=creds_json,
                        credentials_path=db.get('credentials_path'),
                        dataset=db.get('dataset'),
                        location=db.get('location', 'US'),
                        nickname=name,
                    )
                else:
                    # Traditional DB — resolve password from vault (falls back to env)
                    password = None
                    vault_key = db.get('password_vault')
                    if vault_key:
                        password = vault_config(vault_key)

                    self._presets[name] = DatabaseConfig(
                        type=db_type,
                        host=db.get('host'),
                        port=int(db.get('port', 5432)),
                        database=db.get('database'),
                        user=db.get('user'),
                        password=password or '',
                        ssl=db.get('ssl', False),
                        nickname=name,
                    )

            logger.info(f"Loaded {len(self._presets)} database presets from mcp.yaml: {list(self._presets.keys())}")
        except Exception as e:
            logger.error(f"Failed to load database presets from mcp.yaml: {e}")

        self._loaded = True

    def get_available_presets(self) -> Dict[str, DatabaseConfig]:
        """Get all configured database presets."""
        self._ensure_loaded()
        return self._presets.copy()

    def get_preset(self, name: str) -> Optional[DatabaseConfig]:
        """Get a specific preset by name."""
        self._ensure_loaded()
        return self._presets.get(name)

    def get_presets_by_type(self, db_type: str) -> Dict[str, DatabaseConfig]:
        """Get all presets of a specific database type."""
        self._ensure_loaded()
        return {n: c for n, c in self._presets.items() if c.type == db_type}

    def validate_preset(self, preset_name: str, config: DatabaseConfig) -> List[str]:
        """Validate a database preset configuration."""
        errors = []
        if config.type == 'bigquery':
            if not config.project_id:
                errors.append(f"{preset_name}: Project ID is required for BigQuery")
            return errors

        if not config.host:
            errors.append(f"{preset_name}: Host is required")
        if not config.database:
            errors.append(f"{preset_name}: Database name is required")
        if not config.user:
            errors.append(f"{preset_name}: Username is required")
        if config.port is None or config.port <= 0 or config.port > 65535:
            errors.append(f"{preset_name}: Port must be between 1 and 65535")
        return errors


# Singleton
_database_presets = None


def get_database_presets() -> DatabasePresets:
    """Get the database presets instance."""
    global _database_presets
    if _database_presets is None:
        _database_presets = DatabasePresets()
    return _database_presets
