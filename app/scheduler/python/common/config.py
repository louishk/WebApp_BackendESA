"""
Configuration management for data pipeline module.
Handles .env-based configuration for databases, Redis, and HTTP client.
Sensitive values are loaded from encrypted vault with .env fallback.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional
from decouple import config as env_config, Csv

# Import vault-aware config for sensitive values
try:
    from common.secrets_vault import vault_config as secure_config
except ImportError:
    # Fallback if vault not available
    secure_config = env_config


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
        Load configuration from environment variables (.env file).

        Returns:
            DataLayerConfig: Configuration loaded from environment
        """
        databases = {}

        # Load Azure SQL configuration (if available)
        try:
            azure_host = env_config('AZURE_SQL_HOST', default=None)
            if azure_host:
                databases['azure_sql'] = DatabaseConfig(
                    db_type=DatabaseType.AZURE_SQL,
                    host=azure_host,
                    port=env_config('AZURE_SQL_PORT', default=1433, cast=int),
                    database=env_config('AZURE_SQL_DATABASE'),
                    username=env_config('AZURE_SQL_USERNAME'),
                    password=secure_config('AZURE_SQL_PASSWORD'),  # From vault
                    driver=env_config('AZURE_SQL_DRIVER', default='ODBC Driver 17 for SQL Server'),
                    pool_size=env_config('DB_POOL_SIZE', default=5, cast=int),
                    max_overflow=env_config('DB_MAX_OVERFLOW', default=10, cast=int),
                    pool_timeout=env_config('DB_POOL_TIMEOUT', default=60, cast=int),
                    pool_recycle=env_config('DB_POOL_RECYCLE', default=1800, cast=int),
                )
        except Exception:
            pass  # Azure SQL not configured

        # Load MariaDB configuration (if available)
        try:
            mariadb_host = env_config('MARIADB_HOST', default=None)
            if mariadb_host:
                databases['mariadb'] = DatabaseConfig(
                    db_type=DatabaseType.MARIADB,
                    host=mariadb_host,
                    port=env_config('MARIADB_PORT', default=3306, cast=int),
                    database=env_config('MARIADB_DATABASE'),
                    username=env_config('MARIADB_USERNAME'),
                    password=secure_config('MARIADB_PASSWORD'),  # From vault
                    pool_size=env_config('DB_POOL_SIZE', default=5, cast=int),
                    max_overflow=env_config('DB_MAX_OVERFLOW', default=10, cast=int),
                    pool_timeout=env_config('DB_POOL_TIMEOUT', default=60, cast=int),
                    pool_recycle=env_config('DB_POOL_RECYCLE', default=1800, cast=int),
                )
        except Exception:
            pass  # MariaDB not configured

        # Load PostgreSQL configuration (if available)
        try:
            pg_host = env_config('POSTGRESQL_HOST', default=None)
            if pg_host:
                databases['postgresql'] = DatabaseConfig(
                    db_type=DatabaseType.POSTGRESQL,
                    host=pg_host,
                    port=env_config('POSTGRESQL_PORT', default=5432, cast=int),
                    database=env_config('POSTGRESQL_DATABASE'),
                    username=env_config('POSTGRESQL_USERNAME'),
                    password=secure_config('POSTGRESQL_PASSWORD'),  # From vault
                    pool_size=env_config('DB_POOL_SIZE', default=5, cast=int),
                    max_overflow=env_config('DB_MAX_OVERFLOW', default=10, cast=int),
                    pool_timeout=env_config('DB_POOL_TIMEOUT', default=60, cast=int),
                    pool_recycle=env_config('DB_POOL_RECYCLE', default=1800, cast=int),
                )
        except Exception:
            pass  # PostgreSQL not configured

        # Load SOAP configuration (if available)
        soap_config = None
        try:
            soap_url = env_config('SOAP_BASE_URL', default=None)
            if soap_url:
                soap_config = SOAPConfig(
                    base_url=soap_url,
                    corp_code=env_config('SOAP_CORP_CODE'),
                    corp_user=env_config('SOAP_CORP_USER'),
                    api_key=secure_config('SOAP_API_KEY'),  # From vault
                    corp_password=secure_config('SOAP_CORP_PASSWORD'),  # From vault
                    timeout=env_config('SOAP_TIMEOUT', default=60, cast=int),
                    retries=env_config('SOAP_RETRIES', default=3, cast=int),
                )
        except Exception:
            pass  # SOAP not configured

        return cls(
            databases=databases,
            redis_url=env_config('REDIS_URL', default=None),
            cache_enabled=env_config('CACHE_ENABLED', default=True, cast=bool),
            cache_default_ttl=env_config('CACHE_DEFAULT_TTL', default=3600, cast=int),
            batch_chunk_size=env_config('BATCH_CHUNK_SIZE', default=500, cast=int),
            compression_threshold=env_config('COMPRESSION_THRESHOLD', default=1024, cast=int),
            http_pool_connections=env_config('HTTP_POOL_CONNECTIONS', default=10, cast=int),
            http_pool_maxsize=env_config('HTTP_POOL_MAXSIZE', default=20, cast=int),
            http_total_retries=env_config('HTTP_TOTAL_RETRIES', default=3, cast=int),
            http_timeout=env_config('HTTP_TIMEOUT', default=30, cast=int),
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
