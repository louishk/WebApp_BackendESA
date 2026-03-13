"""
MCP v3 Simple Database Connection Module
Clean database connection class for multi-database support
"""

import re
import time
import logging
from typing import List, Dict, Any

# Import DatabaseConfig from config module
from mcp_esa.config.database_presets import DatabaseConfig

logger = logging.getLogger(__name__)

# Regex for validating SQL identifiers (table names, schema names, dataset names)
_IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_\-]{0,127}$')


def _validate_identifier(value: str, label: str = "identifier") -> str:
    """Validate a SQL identifier to prevent injection. Raises ValueError if invalid."""
    if not value or not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid {label}: must be alphanumeric/underscore, got '{value}'")
    return value

# Import database drivers
DATABASE_DRIVERS = {}

try:
    import asyncpg
    DATABASE_DRIVERS['postgresql'] = asyncpg
    logger.info("PostgreSQL driver (asyncpg) available")
except ImportError:
    logger.warning("PostgreSQL driver not available")

try:
    import aiomysql
    DATABASE_DRIVERS['mysql'] = aiomysql
    DATABASE_DRIVERS['mariadb'] = aiomysql  # MariaDB uses same driver
    logger.info("MySQL/MariaDB driver (aiomysql) available")
except ImportError:
    logger.warning("MySQL/MariaDB driver not available")

try:
    import aioodbc
    DATABASE_DRIVERS['mssql'] = aioodbc
    logger.info("SQL Server driver (aioodbc) available")
except ImportError:
    logger.warning("SQL Server driver not available")

try:
    from google.cloud import bigquery as bq_client
    from google.oauth2 import service_account as bq_service_account
    DATABASE_DRIVERS['bigquery'] = bq_client
    logger.info("BigQuery driver (google-cloud-bigquery) available")
except ImportError:
    logger.warning("BigQuery driver not available")


class SimpleDatabase:
    """Simple database connection with clean logging support"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self.connected = False
        # BigQuery uses a client instead of a connection
        self._bq_client = None

    async def connect(self):
        """Connect to database"""
        if self.config.type not in DATABASE_DRIVERS:
            raise ImportError(f"Driver for {self.config.type} not available")

        # BigQuery uses project_id instead of host:port
        if self.config.type == 'bigquery':
            logger.info(
                f"Attempting to connect to BigQuery project: "
                f"{self.config.project_id}"
                f"{f' (dataset: {self.config.dataset})' if self.config.dataset else ''}"
            )
        else:
            logger.info(
                f"Attempting to connect to {self.config.type} database: "
                f"{self.config.host}:{self.config.port}/{self.config.database}"
            )

        try:
            if self.config.type == 'postgresql':
                self.connection = await asyncpg.connect(
                    host=self.config.host,
                    port=self.config.port,
                    user=self.config.user,
                    password=self.config.password,
                    database=self.config.database,
                    ssl='require' if self.config.ssl else 'prefer'
                )
                # Test connection
                await self.connection.fetchval("SELECT 1")

            elif self.config.type in ['mysql', 'mariadb']:
                # For aiomysql, ssl=None means no SSL, ssl=True uses default SSL context
                ssl_config = None
                if self.config.ssl:
                    import ssl as ssl_module
                    ssl_config = ssl_module.create_default_context()

                self.connection = await aiomysql.connect(
                    host=self.config.host,
                    port=self.config.port,
                    user=self.config.user,
                    password=self.config.password,
                    db=self.config.database,
                    ssl=ssl_config
                )
                # Test connection
                async with self.connection.cursor() as cursor:
                    await cursor.execute("SELECT 1")

            elif self.config.type == 'mssql':
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.config.host},{self.config.port};"
                    f"DATABASE={self.config.database};"
                    f"UID={self.config.user};"
                    f"PWD={self.config.password};"
                    f"Encrypt={'yes' if self.config.ssl else 'no'};"
                    f"TrustServerCertificate={'no' if self.config.ssl else 'yes'};"
                )
                self.connection = await aioodbc.connect(dsn=conn_str)
                # Test connection
                async with self.connection.cursor() as cursor:
                    await cursor.execute("SELECT 1")

            elif self.config.type == 'bigquery':
                # BigQuery authentication
                credentials = None
                if self.config.credentials_json:
                    import json as _json
                    creds_info = _json.loads(self.config.credentials_json)
                    credentials = bq_service_account.Credentials.from_service_account_info(
                        creds_info,
                        scopes=["https://www.googleapis.com/auth/bigquery"]
                    )
                    logger.debug("Using service account credentials from vault")
                elif self.config.credentials_path:
                    credentials = bq_service_account.Credentials.from_service_account_file(
                        self.config.credentials_path,
                        scopes=["https://www.googleapis.com/auth/bigquery"]
                    )
                    logger.debug(f"Using service account credentials from: {self.config.credentials_path}")
                else:
                    logger.debug("Using Application Default Credentials (ADC) for BigQuery")

                # Create BigQuery client (synchronous, but we'll wrap calls)
                self._bq_client = bq_client.Client(
                    project=self.config.project_id,
                    credentials=credentials,
                    location=self.config.location
                )
                # Test connection by running a simple query
                test_query = "SELECT 1"
                query_job = self._bq_client.query(test_query)
                list(query_job.result())  # Force execution

            self.connected = True
            if self.config.type == 'bigquery':
                logger.info(f"Successfully connected to BigQuery project: {self.config.project_id}")
            else:
                logger.info(f"Successfully connected to {self.config.type} database: {self.config.database}")

        except Exception as e:
            self.connected = False
            logger.error(f"Failed to connect to {self.config.type} database: {e}")
            raise Exception(f"Failed to connect to {self.config.type} database")

    async def execute_query(self, query: str, params: List[Any] = None) -> List[Dict[str, Any]]:
        """Execute query and return results"""
        # BigQuery uses _bq_client instead of connection
        if self.config.type == 'bigquery':
            if not self._bq_client or not self.connected:
                raise Exception("Not connected to BigQuery")
        elif not self.connection or not self.connected:
            raise Exception("Not connected to database")

        params = params or []
        logger.debug(f"Executing query on {self.config.type}: {query[:100]}..." if len(query) > 100 else f"Executing: {query}")

        try:
            start_time = time.time()

            if self.config.type == 'postgresql':
                rows = await self.connection.fetch(query, *params)
                result = [dict(row) for row in rows]

            elif self.config.type in ['mysql', 'mariadb']:
                async with self.connection.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(query, params)
                    if cursor.description:
                        result = await cursor.fetchall()
                        result = [dict(row) for row in result]
                    else:
                        result = [{"affected_rows": cursor.rowcount}]

            elif self.config.type == 'mssql':
                async with self.connection.cursor() as cursor:
                    await cursor.execute(query, params)
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        rows = await cursor.fetchall()
                        result = [dict(zip(columns, row)) for row in rows]
                    else:
                        result = [{"affected_rows": cursor.rowcount}]

            elif self.config.type == 'bigquery':
                # BigQuery query execution (synchronous client)
                # Set default dataset if configured
                job_config = None
                if self.config.dataset:
                    job_config = bq_client.QueryJobConfig(
                        default_dataset=f"{self.config.project_id}.{self.config.dataset}"
                    )

                query_job = self._bq_client.query(query, job_config=job_config)
                rows = query_job.result()

                # Convert to list of dicts
                result = [dict(row) for row in rows]

            execution_time = round((time.time() - start_time) * 1000, 2)
            logger.debug(f"Query executed in {execution_time}ms, returned {len(result)} rows")

            return result

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise Exception("Query execution failed")

    async def get_tables(self, schema: str = None) -> List[Dict[str, Any]]:
        """Get list of tables in the database

        For BigQuery, 'schema' refers to the dataset name.
        """
        try:
            if self.config.type == 'postgresql':
                query = """
                    SELECT table_name, table_type, table_schema
                    FROM information_schema.tables
                    WHERE table_schema = COALESCE($1, 'public')
                    ORDER BY table_name
                """
                params = [schema] if schema else ['public']

            elif self.config.type in ['mysql', 'mariadb']:
                db_name = schema or self.config.database
                query = """
                    SELECT table_name, table_type, table_schema
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    ORDER BY table_name
                """
                params = [db_name]

            elif self.config.type == 'mssql':
                query = """
                    SELECT TABLE_NAME as table_name, TABLE_TYPE as table_type, TABLE_SCHEMA as table_schema
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = COALESCE(?, 'dbo')
                    ORDER BY TABLE_NAME
                """
                params = [schema] if schema else ['dbo']

            elif self.config.type == 'bigquery':
                # For BigQuery, schema = dataset
                dataset = schema or self.config.dataset
                if not dataset:
                    raise Exception("Dataset must be specified for BigQuery (use 'schema' parameter)")

                # Validate identifiers to prevent injection
                _validate_identifier(self.config.project_id, "project_id")
                _validate_identifier(dataset, "dataset")

                query = f"""
                    SELECT table_name, table_type, '{dataset}' as table_schema, creation_time
                    FROM `{self.config.project_id}.{dataset}.INFORMATION_SCHEMA.TABLES`
                    ORDER BY table_name
                """
                return await self.execute_query(query)

            else:
                raise Exception(f"Get tables not supported for {self.config.type}")

            return await self.execute_query(query, params)

        except Exception as e:
            logger.error(f"Failed to get tables: {e}")
            raise

    async def describe_table(self, table_name: str, schema: str = None) -> List[Dict[str, Any]]:
        """Get table structure information

        For BigQuery, 'schema' refers to the dataset name.
        """
        try:
            if self.config.type == 'postgresql':
                query = """
                    SELECT column_name,
                           data_type,
                           is_nullable,
                           column_default,
                           character_maximum_length,
                           numeric_precision,
                           numeric_scale
                    FROM information_schema.columns
                    WHERE table_name = $1
                      AND table_schema = COALESCE($2, 'public')
                    ORDER BY ordinal_position
                """
                params = [table_name, schema or 'public']

            elif self.config.type in ['mysql', 'mariadb']:
                db_name = schema or self.config.database
                query = """
                    SELECT column_name,
                           data_type,
                           is_nullable,
                           column_default,
                           character_maximum_length,
                           numeric_precision,
                           numeric_scale
                    FROM information_schema.columns
                    WHERE table_name = %s
                      AND table_schema = %s
                    ORDER BY ordinal_position
                """
                params = [table_name, db_name]

            elif self.config.type == 'mssql':
                query = """
                    SELECT COLUMN_NAME as column_name,
                           DATA_TYPE as data_type,
                           IS_NULLABLE as is_nullable,
                           COLUMN_DEFAULT as column_default,
                           CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                           NUMERIC_PRECISION as numeric_precision,
                           NUMERIC_SCALE as numeric_scale
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = ?
                      AND TABLE_SCHEMA = COALESCE(?, 'dbo')
                    ORDER BY ORDINAL_POSITION
                """
                params = [table_name, schema or 'dbo']

            elif self.config.type == 'bigquery':
                # For BigQuery, schema = dataset
                dataset = schema or self.config.dataset
                if not dataset:
                    raise Exception("Dataset must be specified for BigQuery (use 'schema' parameter)")

                # Validate identifiers to prevent injection
                _validate_identifier(self.config.project_id, "project_id")
                _validate_identifier(dataset, "dataset")
                _validate_identifier(table_name, "table_name")

                query = f"""
                    SELECT column_name,
                           data_type,
                           is_nullable,
                           column_default,
                           NULL as character_maximum_length,
                           NULL as numeric_precision,
                           NULL as numeric_scale
                    FROM `{self.config.project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """
                return await self.execute_query(query)

            else:
                raise Exception(f"Describe table not supported for {self.config.type}")

            return await self.execute_query(query, params)

        except Exception as e:
            logger.error(f"Failed to describe table {table_name}: {e}")
            raise

    async def test_connection(self) -> bool:
        """Test if the database connection is still valid"""
        try:
            if self.config.type == 'bigquery':
                result = await self.execute_query("SELECT 1 as test")
            else:
                result = await self.execute_query("SELECT 1")
            return len(result) > 0
        except Exception:
            self.connected = False
            return False

    async def close(self):
        """Close database connection"""
        try:
            if self.config.type == 'bigquery':
                if self._bq_client:
                    self._bq_client.close()
                    self._bq_client = None
                    self.connected = False
                    logger.info("Closed BigQuery connection")
            elif self.connection:
                if self.config.type == 'postgresql':
                    await self.connection.close()
                elif self.config.type in ['mysql', 'mariadb', 'mssql']:
                    self.connection.close()

                self.connection = None
                self.connected = False
                logger.info(f"Closed {self.config.type} connection")

        except Exception as e:
            logger.error(f"Error closing {self.config.type} connection: {e}")
            self.connection = None
            self._bq_client = None
            self.connected = False

    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information (safe for external display — no host/user/port)"""
        if self.config.type == 'bigquery':
            return {
                'database_type': self.config.type,
                'project_id': self.config.project_id,
                'dataset': self.config.dataset,
                'location': self.config.location,
                'connected': self.connected,
                'nickname': self.config.nickname
            }
        return {
            'database_type': self.config.type,
            'database': self.config.database,
            'ssl_enabled': self.config.ssl,
            'connected': self.connected,
            'nickname': self.config.nickname
        }

    def __repr__(self):
        status = "connected" if self.connected else "disconnected"
        if self.config.type == 'bigquery':
            return f"<SimpleDatabase bigquery://{self.config.project_id} ({status})>"
        return f"<SimpleDatabase {self.config.type}://{self.config.database} ({status})>"
