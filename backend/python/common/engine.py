"""
Database engine factory supporting Azure SQL, MariaDB, and PostgreSQL.
Handles connection pooling and retry logic for large data transfers.
"""

import time
import urllib.parse
import logging
from sqlalchemy import create_engine, exc
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import Engine

from .config import DatabaseConfig, DatabaseType


logger = logging.getLogger(__name__)


def create_engine_from_config(
    db_config: DatabaseConfig,
    retries: int = 3,
    retry_delay: int = 5
) -> Engine:
    """
    Create SQLAlchemy engine from database configuration with retry logic.

    Supports:
    - Azure SQL Server (mssql+pyodbc)
    - MariaDB (mysql+pymysql)
    - PostgreSQL (postgresql+psycopg2)

    Args:
        db_config: Database configuration
        retries: Number of connection retry attempts (default: 3)
        retry_delay: Delay between retries in seconds (default: 5)

    Returns:
        Engine: SQLAlchemy engine with connection pooling

    Raises:
        ValueError: If database type is unsupported
        OperationalError: If connection fails after retries
    """
    # Build connection string based on database type
    connection_url = _build_connection_string(db_config)

    attempt = 0
    while attempt < retries:
        try:
            # Create engine with connection pooling
            engine = create_engine(
                connection_url,
                pool_size=db_config.pool_size,
                max_overflow=db_config.max_overflow,
                pool_timeout=db_config.pool_timeout,
                pool_recycle=db_config.pool_recycle,
                pool_pre_ping=db_config.pool_pre_ping
            )

            logger.info(
                f"SQLAlchemy engine created successfully: {db_config.db_type.value} "
                f"(host={db_config.host}, database={db_config.database})"
            )

            # Test connection
            with engine.connect() as conn:
                logger.debug(f"Connection test successful for {db_config.db_type.value}")

            return engine

        except OperationalError as oe:
            attempt += 1
            logger.error(
                f"Connection attempt {attempt}/{retries} failed for {db_config.db_type.value}: {oe}"
            )

            if attempt >= retries:
                logger.critical(
                    f"Max retries ({retries}) reached. Could not create SQLAlchemy engine."
                )
                raise

            logger.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

        except exc.SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error occurred for {db_config.db_type.value}: {e}")
            raise

    # Should never reach here, but for type safety
    raise OperationalError("Failed to create database engine", None, None)


def _build_connection_string(db_config: DatabaseConfig) -> str:
    """
    Build database-specific connection string.

    Args:
        db_config: Database configuration

    Returns:
        str: Connection string for SQLAlchemy

    Raises:
        ValueError: If database type is unsupported
    """
    # URL-encode credentials for special characters
    username = urllib.parse.quote_plus(db_config.username)
    password = urllib.parse.quote_plus(db_config.password)

    if db_config.db_type == DatabaseType.AZURE_SQL:
        # Azure SQL Server: mssql+pyodbc
        if not db_config.driver:
            raise ValueError("Azure SQL requires ODBC driver specification")

        driver = urllib.parse.quote_plus(db_config.driver)
        connection_url = (
            f"mssql+pyodbc://{username}:{password}"
            f"@{db_config.host}:{db_config.port}/{db_config.database}"
            f"?driver={driver}"
            f"&Encrypt=yes&TrustServerCertificate=yes"
            f"&Connection Timeout={db_config.pool_timeout}"
        )
        logger.debug(f"Azure SQL connection string built (driver={db_config.driver})")

    elif db_config.db_type == DatabaseType.MARIADB:
        # MariaDB: mysql+pymysql
        connection_url = (
            f"mysql+pymysql://{username}:{password}"
            f"@{db_config.host}:{db_config.port}/{db_config.database}"
        )
        logger.debug("MariaDB connection string built")

    elif db_config.db_type == DatabaseType.POSTGRESQL:
        # PostgreSQL: postgresql+psycopg2
        connection_url = (
            f"postgresql+psycopg2://{username}:{password}"
            f"@{db_config.host}:{db_config.port}/{db_config.database}"
        )
        logger.debug("PostgreSQL connection string built")

    else:
        raise ValueError(
            f"Unsupported database type: {db_config.db_type}. "
            f"Supported types: {', '.join([t.value for t in DatabaseType])}"
        )

    return connection_url


def get_pool_stats(engine: Engine) -> dict:
    """
    Get connection pool statistics for monitoring.

    Args:
        engine: SQLAlchemy engine

    Returns:
        dict: Pool statistics including size, checked in/out, overflow, etc.
    """
    try:
        pool = engine.pool

        stats = {
            'pool_size': pool.size(),
            'checked_in': pool.checkedin(),
            'checked_out': pool.checkedout(),
            'overflow': pool.overflow(),
            'invalid': pool.invalid() if hasattr(pool, 'invalid') else 0,
        }

        # Calculate utilization percentage
        if pool.size() > 0:
            stats['utilization'] = f"{(pool.checkedout() / pool.size()) * 100:.1f}%"
        else:
            stats['utilization'] = "0%"

        return stats

    except Exception as e:
        logger.error(f"Error getting pool stats: {e}")
        return {"error": str(e)}
