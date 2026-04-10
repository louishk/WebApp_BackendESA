"""
MCP v3 Database Service Module
Business logic for database operations
"""

import re
import time
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from mcp_esa.services.simple_database import SimpleDatabase, DatabaseConfig

logger = logging.getLogger(__name__)


@dataclass
class DatabaseOperationResult:
    """Result of a database operation"""
    success: bool
    data: Any = None
    error: Optional[str] = None
    execution_time_ms: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None

    def to_formatted_string(self) -> str:
        """Convert result to formatted string for MCP tools"""
        if self.success:
            return f"[SUCCESS] Operation completed in {self.execution_time_ms}ms\n\nData: {self.data}"
        else:
            return f"[ERROR] {self.error}"


@dataclass
class QuerySafetyResult:
    """Result of query safety analysis"""
    is_safe: bool
    reason: str


class DatabaseService:
    """
    Database service providing business logic for database operations
    Separated from MCP tools for better testability and reusability
    """

    def __init__(self):
        self.logger = logger

    async def connect_to_database(self, config: DatabaseConfig) -> SimpleDatabase:
        """
        Connect to a database using the provided configuration
        Returns a database connection instance
        """
        try:
            db = SimpleDatabase(config)
            await db.connect()
            self.logger.info(f"Successfully connected to {config.type} database: {config.database}")
            return db
        except Exception as e:
            self.logger.error(f"Failed to connect to {config.type} database: {e}")
            raise

    async def validate_connection(self, db_connection: SimpleDatabase) -> DatabaseOperationResult:
        """Validate that a database connection is still active"""
        start_time = time.time()

        try:
            if hasattr(db_connection, 'config'):
                result = await db_connection.execute_query("SELECT 1")
            else:
                return DatabaseOperationResult(
                    success=False,
                    error="Database connection has no configuration"
                )

            execution_time = round((time.time() - start_time) * 1000, 2)

            return DatabaseOperationResult(
                success=True,
                data={"status": "connected", "validation_result": result},
                execution_time_ms=execution_time,
                metadata={"validation_query": "SELECT 1"}
            )

        except Exception as e:
            execution_time = round((time.time() - start_time) * 1000, 2)
            self.logger.error(f"Connection validation failed: {e}")
            return DatabaseOperationResult(
                success=False,
                error="Connection validation failed",
                execution_time_ms=execution_time
            )

    async def execute_safe_query(
        self,
        db_connection: SimpleDatabase,
        query: str,
        params: List[Any] = None
    ) -> DatabaseOperationResult:
        """Execute a query with safety checks and proper error handling"""
        start_time = time.time()

        try:
            # Basic safety checks
            safety_result = self._check_query_safety(query)
            if not safety_result.is_safe:
                return DatabaseOperationResult(
                    success=False,
                    error=f"Query rejected for safety: {safety_result.reason}"
                )

            # Execute the query
            result = await db_connection.execute_query(query, params or [])
            execution_time = round((time.time() - start_time) * 1000, 2)

            # Analyze result
            metadata = self._analyze_query_result(query, result, execution_time)

            return DatabaseOperationResult(
                success=True,
                data=result,
                execution_time_ms=execution_time,
                metadata=metadata
            )

        except Exception as e:
            execution_time = round((time.time() - start_time) * 1000, 2)
            self.logger.error(f"Query execution failed: {e}")
            return DatabaseOperationResult(
                success=False,
                error="Query execution failed",
                execution_time_ms=execution_time
            )

    def _check_query_safety(self, query: str) -> QuerySafetyResult:
        """Check if a query is safe to execute — SELECT/SHOW/DESCRIBE/EXPLAIN/WITH only."""
        # Strip leading SQL comments (-- and /* */) to prevent comment-based bypass
        cleaned = re.sub(r'--[^\n]*', '', query)
        cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
        cleaned = cleaned.strip().upper()

        if not cleaned:
            return QuerySafetyResult(is_safe=False, reason="Empty query")

        # Whitelist: only allow read-only query types
        allowed_prefixes = ('SELECT', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN', 'WITH')
        if not cleaned.startswith(allowed_prefixes):
            return QuerySafetyResult(
                is_safe=False,
                reason="Only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are allowed"
            )

        # Check for very long queries (potential DoS)
        if len(query) > 10000:
            return QuerySafetyResult(
                is_safe=False,
                reason="Query is too long"
            )

        # Check for suspicious patterns
        if query.upper().count('UNION') > 5:
            return QuerySafetyResult(
                is_safe=False,
                reason="Query contains excessive UNION statements"
            )

        return QuerySafetyResult(is_safe=True, reason="Query passed safety checks")

    def _analyze_query_result(
        self,
        query: str,
        result: List[Dict],
        execution_time: float
    ) -> Dict[str, Any]:
        """Analyze query result and provide metadata"""
        query_type = self._determine_query_type(query)

        metadata = {
            'query_type': query_type,
            'rows_returned': len(result) if isinstance(result, list) else 0,
            'execution_time_category': self._categorize_execution_time(execution_time),
            'result_size_bytes': len(str(result))
        }

        if query_type == 'SELECT':
            metadata['is_large_result'] = len(result) > 1000 if isinstance(result, list) else False
        elif query_type in ['INSERT', 'UPDATE', 'DELETE']:
            if isinstance(result, list) and len(result) > 0:
                first_row = result[0]
                if isinstance(first_row, dict) and 'affected_rows' in first_row:
                    metadata['affected_rows'] = first_row['affected_rows']

        return metadata

    def _determine_query_type(self, query: str) -> str:
        """Determine the type of SQL query"""
        query_upper = query.upper().strip()

        if query_upper.startswith('SELECT'):
            return 'SELECT'
        elif query_upper.startswith('INSERT'):
            return 'INSERT'
        elif query_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif query_upper.startswith('DELETE'):
            return 'DELETE'
        elif query_upper.startswith('CREATE'):
            return 'CREATE'
        elif query_upper.startswith('DROP'):
            return 'DROP'
        elif query_upper.startswith('ALTER'):
            return 'ALTER'
        elif query_upper.startswith('SHOW'):
            return 'SHOW'
        elif query_upper.startswith('DESCRIBE') or query_upper.startswith('DESC'):
            return 'DESCRIBE'
        else:
            return 'OTHER'

    def _categorize_execution_time(self, execution_time_ms: float) -> str:
        """Categorize execution time for monitoring"""
        if execution_time_ms < 100:
            return 'FAST'
        elif execution_time_ms < 1000:
            return 'NORMAL'
        elif execution_time_ms < 5000:
            return 'SLOW'
        else:
            return 'VERY_SLOW'

    @staticmethod
    def extract_table_references(query: str) -> set:
        """
        Extract table names referenced in a SQL query using regex.
        Catches FROM, JOIN, and INTO clauses. Not a full SQL parser,
        but sufficient for read-only SELECT queries.
        """
        # Normalize whitespace
        normalized = re.sub(r'\s+', ' ', query.strip())

        tables = set()

        # Strip double-quoted identifiers to plain names (prevents bypass via "Table")
        normalized = re.sub(r'"([^"]+)"', r'\1', normalized)

        # Match: FROM table, JOIN table, INTO table
        # Handles optional schema prefix (schema.table)
        # Stops at whitespace, comma, parenthesis, or semicolon
        pattern = r'(?:FROM|JOIN|INTO)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)(?:\s|,|\(|;|$)'
        matches = re.findall(pattern, normalized, re.IGNORECASE)

        # System schemas that should be blocked when table restrictions are active
        blocked_schemas = {'information_schema', 'pg_catalog'}

        for match in matches:
            # If schema.table, check for system schema access
            if '.' in match:
                schema_part, table_part = match.rsplit('.', 1)
                if schema_part.lower() in blocked_schemas:
                    # Mark as a system schema access — caller should block
                    tables.add(f'_system_.{table_part.lower()}')
                else:
                    tables.add(table_part.lower())
            else:
                # Skip SQL keywords that can follow FROM/JOIN
                if match.upper() not in ('SELECT', 'LATERAL', 'UNNEST', 'GENERATE_SERIES', 'VALUES'):
                    tables.add(match.lower())

        return tables


# Global database service instance
_database_service = None


def get_database_service() -> DatabaseService:
    """Get the global database service instance"""
    global _database_service
    if _database_service is None:
        _database_service = DatabaseService()
    return _database_service
