"""
MCP v3 Database Tools
Tools for database connections, queries, and schema inspection
"""

import json
import logging
from typing import TYPE_CHECKING, Dict

from mcp.server import Server

from mcp_esa.services.simple_database import SimpleDatabase
from mcp_esa.services.database_service import get_database_service
from mcp_esa.config.database_presets import get_database_presets
from mcp_esa.server.transport import allowed_db_presets_var, allowed_db_tables_var

if TYPE_CHECKING:
    from mcp_esa.server.mcp_server import MCPServerApp

logger = logging.getLogger(__name__)

# Global connection storage
_active_connections: Dict[str, SimpleDatabase] = {}


def _get_allowed_tables(connection_name: str) -> list:
    """Get the allowed tables for a connection. Returns None if no restrictions."""
    table_rules = allowed_db_tables_var.get({})
    if not table_rules:
        return None
    tables = table_rules.get(connection_name)
    if not tables:
        return None  # Missing key or empty list = no restrictions
    return [t.lower() for t in tables]


def register_database_tools(server: Server, app: 'MCPServerApp') -> None:
    """Register database tools with the MCP server"""

    if not hasattr(server, '_tool_handlers'):
        server._tool_handlers = {}

    db_service = get_database_service()
    presets = get_database_presets()

    # ========================================
    # DB_list_database_presets
    # ========================================
    async def DB_list_database_presets() -> str:
        """List all available database presets from environment configuration"""
        available = presets.get_available_presets()

        # Filter by per-key preset restrictions
        allowed_presets = allowed_db_presets_var.get([])
        if allowed_presets:
            available = {k: v for k, v in available.items() if k in allowed_presets}

        if not available:
            return "No database presets configured. Check your .env file for POSTGRES*, MARIADB*, MYSQL*, or MSSQL* configurations."

        lines = ["Available Database Presets:", "=" * 40]

        for preset_name, config in available.items():
            lines.append(f"\n{preset_name}:")
            lines.append(f"  Type: {config.type}")
            if config.type == 'bigquery':
                lines.append(f"  Project: {config.project_id}")
                if config.dataset:
                    lines.append(f"  Dataset: {config.dataset}")
            else:
                lines.append(f"  Database: {config.database}")

        lines.append(f"\nTotal: {len(available)} preset(s)")
        return "\n".join(lines)

    # ========================================
    # DB_connect_preset
    # ========================================
    async def DB_connect_preset(preset_name: str) -> str:
        """
        Connect to a database using a predefined preset.
        Use DB_list_database_presets to see available presets.
        """
        global _active_connections

        # Check per-key preset restriction
        allowed_presets = allowed_db_presets_var.get([])
        if allowed_presets and preset_name not in allowed_presets:
            return f"Access denied: preset '{preset_name}' is not allowed for this API key"

        # Check if already connected
        if preset_name in _active_connections:
            conn = _active_connections[preset_name]
            if conn.connected:
                return f"Already connected to {preset_name} ({conn.config.type})"

        # Get preset configuration
        config = presets.get_preset(preset_name)
        if not config:
            available = list(presets.get_available_presets().keys())
            return f"Preset '{preset_name}' not found. Available presets: {', '.join(available)}"

        try:
            db = await db_service.connect_to_database(config)
            _active_connections[preset_name] = db

            return f"""Successfully connected to database preset: {preset_name}

Connection Details:
  Type: {config.type}
  Database: {config.database if config.type != 'bigquery' else config.project_id}

Use this connection with:
  - DB_execute_query(connection_name="{preset_name}", query="SELECT ...")
  - DB_list_tables(connection_name="{preset_name}")
  - DB_describe_table(connection_name="{preset_name}", table_name="...")
"""
        except Exception as e:
            logger.error(f"Failed to connect to preset {preset_name}: {e}")
            return f"Failed to connect to {preset_name}. Check server logs for details."

    # ========================================
    # DB_connect_multiple_presets
    # ========================================
    async def DB_connect_multiple_presets(preset_names: str) -> str:
        """
        Connect to multiple database presets at once.
        Pass comma-separated preset names.
        """
        names = [n.strip() for n in preset_names.split(',') if n.strip()]

        if not names:
            return "No preset names provided. Example: DB_connect_multiple_presets(preset_names='cresus,api_db')"

        results = []
        for name in names:
            result = await DB_connect_preset(name)
            results.append(f"--- {name} ---\n{result}")

        return "\n\n".join(results)

    # ========================================
    # DB_execute_query
    # ========================================
    async def DB_execute_query(connection_name: str, query: str) -> str:
        """
        Execute a SQL query on an existing database connection.
        Use DB_connect_preset first to establish a connection.
        """
        global _active_connections

        if connection_name not in _active_connections:
            return f"No active connection named '{connection_name}'. Use DB_connect_preset(preset_name='{connection_name}') first."

        conn = _active_connections[connection_name]
        if not conn.connected:
            return f"Connection '{connection_name}' is not active. Please reconnect using DB_connect_preset."

        try:
            # Check per-key table restrictions
            allowed_tables = _get_allowed_tables(connection_name)
            if allowed_tables is not None:
                referenced = db_service.extract_table_references(query)
                # Block system schema access (information_schema, pg_catalog)
                system_refs = {t for t in referenced if t.startswith('_system_.')}
                if system_refs:
                    return "Access denied: system catalog queries are not allowed when table restrictions are active"
                blocked = referenced - set(allowed_tables)
                if blocked:
                    blocked_list = ', '.join(sorted(blocked))
                    return f"Access denied: query references restricted table(s): {blocked_list}"

            result = await db_service.execute_safe_query(conn, query)

            if not result.success:
                return f"Query failed: {result.error}"

            # Format the results
            data = result.data
            metadata = result.metadata or {}

            lines = [
                f"Query executed successfully on {connection_name}",
                f"Execution time: {result.execution_time_ms}ms ({metadata.get('execution_time_category', 'N/A')})",
                f"Query type: {metadata.get('query_type', 'N/A')}",
                f"Rows returned: {metadata.get('rows_returned', 0)}",
                "",
                "Results:",
                "-" * 40
            ]

            if isinstance(data, list) and len(data) > 0:
                # For SELECT queries, format as table
                if len(data) <= 50:
                    lines.append(json.dumps(data, indent=2, default=str))
                else:
                    lines.append(f"(Showing first 50 of {len(data)} rows)")
                    lines.append(json.dumps(data[:50], indent=2, default=str))
            else:
                lines.append(str(data))

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return "Query execution failed. Check server logs for details."

    # ========================================
    # DB_list_tables
    # ========================================
    async def DB_list_tables(connection_name: str, schema: str = None) -> str:
        """
        List all tables in a connected database.
        Optionally specify schema name (defaults to 'public' for PostgreSQL, 'dbo' for SQL Server).
        """
        global _active_connections

        if connection_name not in _active_connections:
            return f"No active connection named '{connection_name}'. Use DB_connect_preset first."

        conn = _active_connections[connection_name]
        if not conn.connected:
            return f"Connection '{connection_name}' is not active."

        try:
            tables = await conn.get_tables(schema)

            # Filter by per-key table restrictions
            allowed_tables = _get_allowed_tables(connection_name)
            if allowed_tables is not None:
                filtered = []
                for table in tables:
                    if isinstance(table, dict):
                        name = table.get('table_name') or table.get('TABLE_NAME') or list(table.values())[0]
                    else:
                        name = str(table)
                    if name.lower() in allowed_tables:
                        filtered.append(table)
                tables = filtered

            if not tables:
                return f"No tables found in {connection_name}" + (f" (schema: {schema})" if schema else "")

            lines = [
                f"Tables in {connection_name}" + (f" (schema: {schema})" if schema else ""),
                "=" * 40
            ]

            for i, table in enumerate(tables, 1):
                if isinstance(table, dict):
                    name = table.get('table_name') or table.get('TABLE_NAME') or list(table.values())[0]
                    table_type = table.get('table_type', table.get('TABLE_TYPE', ''))
                    lines.append(f"{i}. {name}" + (f" ({table_type})" if table_type else ""))
                else:
                    lines.append(f"{i}. {table}")

            lines.append(f"\nTotal: {len(tables)} table(s)")
            return "\n".join(lines)

        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return "Failed to list tables. Check server logs for details."

    # ========================================
    # DB_describe_table
    # ========================================
    async def DB_describe_table(connection_name: str, table_name: str, schema: str = None) -> str:
        """
        Get the structure/columns of a specific table.
        """
        global _active_connections

        if connection_name not in _active_connections:
            return f"No active connection named '{connection_name}'. Use DB_connect_preset first."

        conn = _active_connections[connection_name]
        if not conn.connected:
            return f"Connection '{connection_name}' is not active."

        # Check per-key table restrictions
        allowed_tables = _get_allowed_tables(connection_name)
        if allowed_tables is not None and table_name.lower() not in allowed_tables:
            return f"Access denied: table '{table_name}' is not accessible for this API key"

        try:
            columns = await conn.describe_table(table_name, schema)

            if not columns:
                return f"Table '{table_name}' not found or has no columns."

            lines = [
                f"Table: {table_name}" + (f" (schema: {schema})" if schema else ""),
                "=" * 60,
                ""
            ]

            for col in columns:
                if isinstance(col, dict):
                    name = col.get('column_name') or col.get('Field', 'unknown')
                    dtype = col.get('data_type') or col.get('Type', 'unknown')
                    nullable = col.get('is_nullable', col.get('Null', 'YES'))
                    default = col.get('column_default') or col.get('Default', '')
                    lines.append(f"  {name}: {dtype} (nullable: {nullable})" + (f" [default: {default}]" if default else ""))
                else:
                    lines.append(f"  {col}")

            lines.append(f"\nTotal: {len(columns)} column(s)")
            return "\n".join(lines)

        except Exception as e:
            logger.error(f"Failed to describe table: {e}")
            return "Failed to describe table. Check server logs for details."

    # ========================================
    # DB_list_connections
    # ========================================
    async def DB_list_connections() -> str:
        """List all active database connections"""
        global _active_connections

        if not _active_connections:
            return "No active database connections. Use DB_connect_preset to connect."

        lines = ["Active Database Connections:", "=" * 40]

        for name, conn in _active_connections.items():
            info = conn.get_connection_info()
            status = "Connected" if info['connected'] else "Disconnected"
            lines.append(f"\n{name}:")
            lines.append(f"  Status: {status}")
            lines.append(f"  Type: {info['database_type']}")
            if info.get('database'):
                lines.append(f"  Database: {info['database']}")
            if info.get('project_id'):
                lines.append(f"  Project: {info['project_id']}")

        lines.append(f"\nTotal: {len(_active_connections)} connection(s)")
        return "\n".join(lines)

    # ========================================
    # DB_disconnect_database
    # ========================================
    async def DB_disconnect_database(connection_name: str) -> str:
        """Close and remove a database connection"""
        global _active_connections

        if connection_name not in _active_connections:
            return f"No active connection named '{connection_name}'."

        conn = _active_connections[connection_name]
        try:
            await conn.close()
            del _active_connections[connection_name]
            return f"Successfully disconnected from {connection_name}"
        except Exception as e:
            # Still remove from active connections
            del _active_connections[connection_name]
            return f"Connection removed (with cleanup error — see server logs)"

    # ========================================
    # Register tools with input schemas
    # ========================================

    DB_list_database_presets._input_schema = {
        "type": "object",
        "properties": {},
        "required": []
    }

    DB_connect_preset._input_schema = {
        "type": "object",
        "properties": {
            "preset_name": {
                "type": "string",
                "description": "Name of the database preset to connect to"
            }
        },
        "required": ["preset_name"]
    }

    DB_connect_multiple_presets._input_schema = {
        "type": "object",
        "properties": {
            "preset_names": {
                "type": "string",
                "description": "Comma-separated list of preset names to connect to"
            }
        },
        "required": ["preset_names"]
    }

    DB_execute_query._input_schema = {
        "type": "object",
        "properties": {
            "connection_name": {
                "type": "string",
                "description": "Name of the active database connection"
            },
            "query": {
                "type": "string",
                "description": "SQL query to execute"
            }
        },
        "required": ["connection_name", "query"]
    }

    DB_list_tables._input_schema = {
        "type": "object",
        "properties": {
            "connection_name": {
                "type": "string",
                "description": "Name of the active database connection"
            },
            "schema": {
                "type": "string",
                "description": "Optional schema name (defaults to 'public' for PostgreSQL, 'dbo' for SQL Server)"
            }
        },
        "required": ["connection_name"]
    }

    DB_describe_table._input_schema = {
        "type": "object",
        "properties": {
            "connection_name": {
                "type": "string",
                "description": "Name of the active database connection"
            },
            "table_name": {
                "type": "string",
                "description": "Name of the table to describe"
            },
            "schema": {
                "type": "string",
                "description": "Optional schema name"
            }
        },
        "required": ["connection_name", "table_name"]
    }

    DB_list_connections._input_schema = {
        "type": "object",
        "properties": {},
        "required": []
    }

    DB_disconnect_database._input_schema = {
        "type": "object",
        "properties": {
            "connection_name": {
                "type": "string",
                "description": "Name of the connection to disconnect"
            }
        },
        "required": ["connection_name"]
    }

    # Add to server's tool handlers
    server._tool_handlers["DB_list_database_presets"] = DB_list_database_presets
    server._tool_handlers["DB_connect_preset"] = DB_connect_preset
    server._tool_handlers["DB_connect_multiple_presets"] = DB_connect_multiple_presets
    server._tool_handlers["DB_execute_query"] = DB_execute_query
    server._tool_handlers["DB_list_tables"] = DB_list_tables
    server._tool_handlers["DB_describe_table"] = DB_describe_table
    server._tool_handlers["DB_list_connections"] = DB_list_connections
    server._tool_handlers["DB_disconnect_database"] = DB_disconnect_database

    logger.info("Database tools registered: DB_list_database_presets, DB_connect_preset, DB_connect_multiple_presets, DB_execute_query, DB_list_tables, DB_describe_table, DB_list_connections, DB_disconnect_database")
