"""
Configuration management for data pipeline module.
Uses unified config system (YAML + vault) for databases, Redis, and HTTP client.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Any
import os


def get_pipeline_config(pipeline_name: str, key: str, default: Any = None) -> Any:
    """
    Get pipeline-specific config from scheduler.yaml.

    Usage:
        location_codes = get_pipeline_config('rentroll', 'location_codes', [])
        chunk_size = get_pipeline_config('fxrate', 'sql_chunk_size', 1000)

    Args:
        pipeline_name: Name of the pipeline (rentroll, mimo, discount, fxrate, sugarcrm, etc.)
        key: Configuration key to retrieve
        default: Default value if not found

    Returns:
        Configuration value or default
    """
    try:
        from common.config_loader import get_config
        config = get_config()
        pipelines = getattr(config.scheduler, 'pipelines', None)
        if pipelines:
            pipeline = getattr(pipelines, pipeline_name, None)
            if pipeline:
                return getattr(pipeline, key, default)
    except Exception:
        pass
    return default


class DatabaseType(Enum):
    """Supported database types"""
    AZURE_SQL = "azure_sql"
    MARIADB = "mariadb"
    POSTGRESQL = "postgresql"


@dataclass
class DatabaseConfig:
    """
    Database connection configuration.
    Supports Azure SQL Server, MariaDB, and PostgreSQL.
    """
    db_type: DatabaseType
    host: str
    port: int
    database: str
    username: str
    password: str
    driver: Optional[str] = None  # Required for Azure SQL (ODBC driver)

    # Connection pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 60  # Increased for large data transfers
    pool_recycle: int = 1800
    pool_pre_ping: bool = True

    def __repr__(self) -> str:
        """Safe representation without password"""
        return (f"DatabaseConfig(db_type={self.db_type.value}, host={self.host}, "
                f"database={self.database}, username={self.username})")


@dataclass
class SOAPConfig:
    """
    SOAP API configuration.
    Manages SOAP client settings including authentication.
    """
    base_url: str
    corp_code: str
    corp_user: str  # Corporate username
    api_key: str  # API key (will be formatted as :::APIKEY by SOAPClient)
    corp_password: str
    timeout: int = 60  # Increased for large payloads
    retries: int = 3

    def __repr__(self) -> str:
        """Safe representation without sensitive data"""
        return (f"SOAPConfig(base_url={self.base_url}, corp_code={self.corp_code}, "
                f"corp_user={self.corp_user})")


@dataclass
class DataLayerConfig:
    """
    Main configuration class for data pipeline.
    Manages database, Redis cache, HTTP client, and SOAP settings.
    """
    databases: Dict[str, DatabaseConfig] = field(default_factory=dict)

    # Redis cache settings
    redis_url: Optional[str] = None
    cache_enabled: bool = True
    cache_default_ttl: int = 3600

    # Batch processing settings (for 1+ MB payloads)
    batch_chunk_size: int = 500
    compression_threshold: int = 1024

    # HTTP client settings
    http_pool_connections: int = 10
    http_pool_maxsize: int = 20
    http_total_retries: int = 3
    http_timeout: int = 30  # Increased for large payloads

    # SOAP client settings
    soap: Optional[SOAPConfig] = None

    @classmethod
    def from_env(cls) -> 'DataLayerConfig':
        """
        Load configuration from unified config system (YAML + vault).

        Returns:
            DataLayerConfig: Configuration loaded from config system
        """
        from common.config_loader import get_config

        app_config = get_config()
        databases = {}

        # Load PostgreSQL/PBI configuration from database.yaml
        try:
            db_cfg = app_config.database.pbi
            if db_cfg and db_cfg.host:
                password = db_cfg.password_vault  # Auto-resolved from vault
                databases['postgresql'] = DatabaseConfig(
                    db_type=DatabaseType.POSTGRESQL,
                    host=db_cfg.host,
                    port=db_cfg.port or 5432,
                    database=db_cfg.name,
                    username=db_cfg.username,
                    password=password,
                    pool_size=db_cfg.pool.size if db_cfg.pool else 5,
                    max_overflow=db_cfg.pool.max_overflow if db_cfg.pool else 10,
                    pool_timeout=60,
                    pool_recycle=1800,
                )
        except Exception as e:
            pass  # PostgreSQL not configured

        # Load SOAP configuration from apis.yaml
        soap_config = None
        try:
            soap_cfg = app_config.apis.soap
            if soap_cfg and soap_cfg.base_url:
                soap_config = SOAPConfig(
                    base_url=soap_cfg.base_url,
                    corp_code=soap_cfg.corp_code,
                    corp_user=soap_cfg.corp_user,
                    api_key=soap_cfg.api_key_vault,  # Auto-resolved from vault
                    corp_password=soap_cfg.corp_password_vault,  # Auto-resolved from vault
                    timeout=soap_cfg.timeout or 60,
                    retries=soap_cfg.retries or 3,
                )
        except Exception as e:
            pass  # SOAP not configured

        # Load Redis configuration from database.yaml
        redis_url = None
        cache_enabled = False
        try:
            redis_cfg = app_config.database.redis
            if redis_cfg and redis_cfg.enabled:
                redis_url = redis_cfg.url
                cache_enabled = True
        except Exception:
            pass

        return cls(
            databases=databases,
            redis_url=redis_url,
            cache_enabled=cache_enabled,
            cache_default_ttl=3600,
            batch_chunk_size=500,
            compression_threshold=1024,
            http_pool_connections=10,
            http_pool_maxsize=20,
            http_total_retries=3,
            http_timeout=30,
            soap=soap_config,
        )

    @classmethod
    def from_dict(cls, config_dict: dict) -> 'DataLayerConfig':
        """
        Load configuration from dictionary.

        Args:
            config_dict: Dictionary with configuration values

        Returns:
            DataLayerConfig: Configuration loaded from dictionary
        """
        databases = {}

        # Parse database configurations
        for db_name, db_conf in config_dict.get('databases', {}).items():
            db_type_str = db_conf.get('db_type', '').lower()

            # Map string to DatabaseType enum
            db_type_map = {
                'azure_sql': DatabaseType.AZURE_SQL,
                'mariadb': DatabaseType.MARIADB,
                'postgresql': DatabaseType.POSTGRESQL,
            }

            if db_type_str not in db_type_map:
                continue

            databases[db_name] = DatabaseConfig(
                db_type=db_type_map[db_type_str],
                host=db_conf['host'],
                port=db_conf.get('port', 5432 if db_type_str == 'postgresql' else
                                         (3306 if db_type_str == 'mariadb' else 1433)),
                database=db_conf['database'],
                username=db_conf['username'],
                password=db_conf['password'],
                driver=db_conf.get('driver'),
                pool_size=db_conf.get('pool_size', 5),
                max_overflow=db_conf.get('max_overflow', 10),
                pool_timeout=db_conf.get('pool_timeout', 60),
                pool_recycle=db_conf.get('pool_recycle', 1800),
            )

        # Parse SOAP configuration (if available)
        soap_config = None
        soap_conf = config_dict.get('soap')
        if soap_conf:
            soap_config = SOAPConfig(
                base_url=soap_conf['base_url'],
                corp_code=soap_conf['corp_code'],
                corp_user=soap_conf['corp_user'],
                api_key=soap_conf['api_key'],
                corp_password=soap_conf['corp_password'],
                timeout=soap_conf.get('timeout', 60),
                retries=soap_conf.get('retries', 3),
            )

        return cls(
            databases=databases,
            redis_url=config_dict.get('redis_url'),
            cache_enabled=config_dict.get('cache_enabled', True),
            cache_default_ttl=config_dict.get('cache_default_ttl', 3600),
            batch_chunk_size=config_dict.get('batch_chunk_size', 500),
            compression_threshold=config_dict.get('compression_threshold', 1024),
            http_pool_connections=config_dict.get('http_pool_connections', 10),
            http_pool_maxsize=config_dict.get('http_pool_maxsize', 20),
            http_total_retries=config_dict.get('http_total_retries', 3),
            http_timeout=config_dict.get('http_timeout', 30),
            soap=soap_config,
        )
