"""
SugarCRM to SQL Pipeline

Fetches data from SugarCRM REST API and pushes to PostgreSQL database.

Features:
- Dynamic table generation from SugarCRM metadata
- Three modes: backfill (all data), auto (incremental), single-module
- Pagination handling for 400K+ records (Leads)
- Simple primary key (sugar_id) for cumulative data
- Chunked upsert for large datasets
- Memory-efficient batch processing

Usage:
    # Backfill all configured modules
    python sugarcrm_to_sql.py --mode backfill

    # Backfill specific module
    python sugarcrm_to_sql.py --mode backfill --module Leads

    # Incremental update (records modified since last N days)
    python sugarcrm_to_sql.py --mode auto

    # With custom date filter
    python sugarcrm_to_sql.py --mode backfill --since 2024-01-01

    # Limit records (for testing)
    python sugarcrm_to_sql.py --mode backfill --module Contacts --limit 100

Configuration (in scheduler.yaml):
    pipelines.sugarcrm.modules: List of modules to sync
    pipelines.sugarcrm.batch_size: API batch size (default: 200)
    pipelines.sugarcrm.sql_chunk_size: SQL upsert chunk size (default: 500)
    pipelines.sugarcrm.incremental_days: Days to look back for auto mode (default: 7)

Configuration (in apis.yaml):
    sugarcrm.base_url: SugarCRM instance URL
    sugarcrm.username: Username
    sugarcrm.password_vault: Vault key for password
    sugarcrm.client_id: OAuth client ID
    sugarcrm.client_secret_vault: Vault key for client secret
"""

import argparse
import logging
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional, Type, Generator

from tqdm import tqdm
from sqlalchemy import Column, String, Integer, DateTime, Date, Boolean, Numeric, Text, inspect as sa_inspect, text
from sqlalchemy.ext.declarative import declarative_base

# Import common utilities
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import (
    DataLayerConfig,
    create_engine_from_config,
    SessionManager,
    UpsertOperations,
    convert_to_bool,
    convert_to_int,
    convert_to_decimal,
    convert_to_datetime,
)
from common.config import get_pipeline_config
from common.sugarcrm_client import SugarCRMClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Declarative base for dynamic models
DynamicBase = declarative_base()


# =============================================================================
# Configuration
# =============================================================================

DEFAULT_MODULES = ['Leads', 'Contacts', 'Accounts', 'Opportunities']
DEFAULT_BATCH_SIZE = 200
DEFAULT_SQL_CHUNK_SIZE = 500
DEFAULT_INCREMENTAL_DAYS = 7


# =============================================================================
# SugarCRM Type to SQLAlchemy Type Mapping
# =============================================================================

def map_sugar_type_to_sqlalchemy(sugar_type: str, field_name: str) -> Column:
    """
    Map SugarCRM field type to SQLAlchemy Column type.

    Args:
        sugar_type: SugarCRM field type (varchar, int, bool, datetime, etc.)
        field_name: Field name for context

    Returns:
        SQLAlchemy Column instance
    """
    sugar_type = sugar_type.lower() if sugar_type else 'varchar'

    # String types - use Text to avoid truncation for all string data
    if sugar_type in ['varchar', 'name', 'phone', 'url', 'email', 'id', 'link', 'char']:
        return Column(Text, nullable=True)
    elif sugar_type in ['text', 'longtext', 'html', 'json']:
        return Column(Text, nullable=True)

    # Numeric types
    elif sugar_type in ['int', 'integer', 'tinyint']:
        return Column(Integer, nullable=True)
    elif sugar_type in ['decimal', 'float', 'double', 'currency']:
        return Column(Numeric(18, 4), nullable=True)

    # Boolean types
    elif sugar_type in ['bool', 'boolean']:
        return Column(Boolean, nullable=True)

    # Date/Time types
    elif sugar_type in ['date']:
        return Column(Date, nullable=True)
    elif sugar_type in ['datetime', 'datetimecombo']:
        return Column(DateTime, nullable=True)

    # Enum/Dropdown types (store as text to handle long values)
    elif sugar_type in ['enum', 'multienum', 'radioenum']:
        return Column(Text, nullable=True)

    # Relationship types - use Text for safety (some relate fields have long values)
    elif sugar_type in ['relate', 'parent', 'parent_type']:
        return Column(Text, nullable=True)

    # Default to Text for safety
    else:
        logger.debug(f"Unknown SugarCRM type '{sugar_type}' for field '{field_name}', using Text")
        return Column(Text, nullable=True)


# Field types to exclude (contain nested/relationship data)
EXCLUDED_FIELD_TYPES = {
    'link',           # Relationship links (nested objects)
    'collection',     # Collection fields
    'team_list',      # Team list (nested)
    'locked_fields',  # Internal locked fields
    'json',           # JSON blobs (handle separately if needed)
}

# Link fields to extract into dimension tables
DIMENSION_MAPPINGS = {
    # link_field_name: (dimension_table, name_field)
    'created_by_link': ('sugarcrm_dim_users', 'full_name'),
    'modified_user_link': ('sugarcrm_dim_users', 'full_name'),
    'assigned_user_link': ('sugarcrm_dim_users', 'full_name'),
    'accounts': ('sugarcrm_dim_accounts', 'name'),
    'contacts': ('sugarcrm_dim_contacts', 'name'),
    'campaign_leads': ('sugarcrm_dim_campaigns', 'name'),
}

# Cache for dimension models
_dimension_models: Dict[str, Type] = {}


def get_dimension_model(table_name: str, name_field: str) -> Type:
    """
    Get or create a dimension table model.

    Args:
        table_name: Name of the dimension table (e.g., 'sugarcrm_dim_users')
        name_field: Name of the name column (e.g., 'full_name' or 'name')

    Returns:
        SQLAlchemy model class
    """
    if table_name in _dimension_models:
        return _dimension_models[table_name]

    columns = {
        '__tablename__': table_name,
        'id': Column(String(36), primary_key=True, nullable=False,
                     comment="SugarCRM record ID"),
        name_field: Column(Text, nullable=True, comment="Display name"),
        'synced_at': Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow,
                           nullable=False, comment="Last sync timestamp"),
    }

    model_class = type(
        table_name.replace('_', ' ').title().replace(' ', ''),
        (DynamicBase,),
        columns
    )

    _dimension_models[table_name] = model_class
    logger.info(f"Created dimension model '{table_name}'")
    return model_class


def extract_dimensions(record: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Extract dimension records from nested link fields.

    Args:
        record: Raw record from SugarCRM API

    Returns:
        Dict of table_name -> list of dimension records
    """
    dimensions: Dict[str, List[Dict[str, Any]]] = {}

    for link_field, (table_name, name_field) in DIMENSION_MAPPINGS.items():
        if link_field not in record:
            continue

        nested = record[link_field]
        if not isinstance(nested, dict):
            continue

        # Extract id and name
        dim_id = nested.get('id')
        dim_name = nested.get(name_field) or nested.get('name') or nested.get('full_name')

        # Skip empty records
        if not dim_id or dim_id == '':
            continue

        if table_name not in dimensions:
            dimensions[table_name] = []

        # Add dimension record
        dim_record = {
            'id': dim_id,
            name_field: dim_name,
        }
        dimensions[table_name].append(dim_record)

    return dimensions


def push_dimensions_to_database(
    dimensions: Dict[str, List[Dict[str, Any]]],
    config: DataLayerConfig
) -> Dict[str, int]:
    """
    Push dimension records to database.

    Args:
        dimensions: Dict of table_name -> list of records
        config: Database configuration

    Returns:
        Dict of table_name -> records upserted
    """
    if not dimensions:
        return {}

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    engine = create_engine_from_config(db_config)
    session_manager = SessionManager(engine)
    results = {}

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        for table_name, records in dimensions.items():
            if not records:
                continue

            # Deduplicate by id
            seen_ids = set()
            unique_records = []
            for r in records:
                if r['id'] not in seen_ids:
                    seen_ids.add(r['id'])
                    unique_records.append(r)

            # Get name field for this table
            name_field = 'name'
            for _, (tbl, nf) in DIMENSION_MAPPINGS.items():
                if tbl == table_name:
                    name_field = nf
                    break

            # Get or create model
            model_class = get_dimension_model(table_name, name_field)

            # Create table if needed
            DynamicBase.metadata.create_all(engine, tables=[model_class.__table__])

            # Upsert
            upsert_ops.upsert_batch(
                model=model_class,
                records=unique_records,
                constraint_columns=['id'],
                chunk_size=500
            )

            results[table_name] = len(unique_records)

    return results


def create_dynamic_model(module_name: str, field_defs: Dict[str, Dict]) -> Type:
    """
    Dynamically create SQLAlchemy model from SugarCRM metadata.

    Args:
        module_name: SugarCRM module name (e.g., 'Leads')
        field_defs: Field definitions from metadata API

    Returns:
        SQLAlchemy model class
    """
    table_name = f'sugarcrm_{module_name.lower()}'

    # Base columns (simple primary key - cumulative data, no snapshots)
    columns = {
        '__tablename__': table_name,
        'sugar_id': Column(String(36), primary_key=True, nullable=False,
                           comment="SugarCRM record UUID"),
        # Sync timestamps (when we synced, not SugarCRM dates)
        'synced_at': Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False,
                           comment="When this record was last synced from SugarCRM"),
    }

    # Add fields from metadata
    skipped_fields = []
    for field_name, field_info in field_defs.items():
        # Skip the 'id' field since we use 'sugar_id'
        if field_name == 'id':
            continue

        # Skip some internal fields
        if field_name.startswith('_') or field_name in ['my_favorite']:
            continue

        # Get field type
        if isinstance(field_info, dict):
            sugar_type = field_info.get('type', 'varchar')
        else:
            sugar_type = 'varchar'

        # Skip excluded types (link, collection, etc.)
        if sugar_type in EXCLUDED_FIELD_TYPES:
            skipped_fields.append(field_name)
            continue

        columns[field_name] = map_sugar_type_to_sqlalchemy(sugar_type, field_name)

    if skipped_fields:
        logger.debug(f"Skipped {len(skipped_fields)} link/collection fields: {skipped_fields[:5]}...")

    # Create the model class dynamically
    model_class = type(
        f'SugarCRM{module_name}',
        (DynamicBase,),
        columns
    )

    logger.info(f"Created dynamic model '{table_name}' with {len(columns) - 4} fields")
    return model_class


# =============================================================================
# Data Transformation
# =============================================================================

def transform_record(
    record: Dict[str, Any],
    field_defs: Dict[str, Dict],
    valid_columns: Optional[set] = None
) -> Dict[str, Any]:
    """
    Transform SugarCRM record to database format.

    Args:
        record: Raw record from SugarCRM API
        field_defs: Field definitions for type conversion
        valid_columns: Set of valid column names in the model (optional)

    Returns:
        Transformed record dictionary
    """
    transformed = {
        'sugar_id': record.get('id'),
    }

    for field_name, value in record.items():
        if field_name == 'id':
            continue
        if field_name.startswith('_') or field_name in ['my_favorite']:
            continue

        # Skip fields not in the model schema
        if valid_columns and field_name not in valid_columns:
            continue

        field_info = field_defs.get(field_name, {})
        if isinstance(field_info, dict):
            sugar_type = field_info.get('type', 'varchar').lower()
        else:
            sugar_type = 'varchar'

        # Skip excluded types (already handled by valid_columns, but double-check)
        if sugar_type in EXCLUDED_FIELD_TYPES:
            continue

        # Convert based on type
        try:
            if value is None or value == '':
                transformed[field_name] = None
            elif sugar_type in ['int', 'integer', 'tinyint']:
                transformed[field_name] = convert_to_int(value)
            elif sugar_type in ['decimal', 'float', 'double', 'currency']:
                transformed[field_name] = convert_to_decimal(value)
            elif sugar_type in ['bool', 'boolean']:
                transformed[field_name] = convert_to_bool(value)
            elif sugar_type in ['datetime', 'datetimecombo']:
                transformed[field_name] = convert_to_datetime(value)
            elif sugar_type == 'date':
                if isinstance(value, str):
                    try:
                        transformed[field_name] = datetime.strptime(value[:10], '%Y-%m-%d').date()
                    except ValueError:
                        transformed[field_name] = None
                else:
                    transformed[field_name] = value
            elif sugar_type == 'email':
                # Email fields can be lists of dicts - extract primary email address
                # Handle both actual lists and string representations
                email_value = value

                # Parse string representation if needed
                if isinstance(value, str) and value.startswith('['):
                    try:
                        import ast
                        email_value = ast.literal_eval(value)
                    except (ValueError, SyntaxError):
                        email_value = value

                if isinstance(email_value, list) and len(email_value) > 0:
                    # Find primary email or use first one
                    email_addr = None
                    for email_obj in email_value:
                        if isinstance(email_obj, dict):
                            if email_obj.get('primary_address'):
                                email_addr = email_obj.get('email_address')
                                break
                            elif not email_addr:
                                email_addr = email_obj.get('email_address')
                    transformed[field_name] = email_addr
                elif isinstance(email_value, list) and len(email_value) == 0:
                    transformed[field_name] = None
                elif isinstance(email_value, dict):
                    transformed[field_name] = email_value.get('email_address')
                elif isinstance(email_value, str) and '@' in email_value:
                    # Already a plain email
                    transformed[field_name] = email_value
                else:
                    transformed[field_name] = None
            elif sugar_type == 'multienum':
                # Multienum fields return lists - convert to comma-separated string
                if isinstance(value, list):
                    # Filter out empty strings and join
                    non_empty = [str(v) for v in value if v and str(v).strip()]
                    transformed[field_name] = ','.join(non_empty) if non_empty else None
                else:
                    transformed[field_name] = str(value) if value else None
            elif sugar_type == 'relate' and isinstance(value, list):
                # Relate fields can be lists of dicts (e.g., team_name)
                # Extract the primary or first name value
                if len(value) > 0:
                    name_val = None
                    for item in value:
                        if isinstance(item, dict):
                            if item.get('primary'):
                                name_val = item.get('name')
                                break
                            elif not name_val:
                                name_val = item.get('name')
                    transformed[field_name] = name_val
                else:
                    transformed[field_name] = None
            elif isinstance(value, (list, dict)):
                # Any other nested structure - skip or convert to string
                # This catches edge cases we haven't explicitly handled
                if isinstance(value, dict) and 'name' in value:
                    transformed[field_name] = value.get('name')
                elif isinstance(value, dict) and 'id' in value:
                    transformed[field_name] = value.get('id')
                elif isinstance(value, list) and len(value) == 0:
                    transformed[field_name] = None
                else:
                    # Skip complex nested data
                    logger.debug(f"Skipping complex nested field '{field_name}'")
                    continue
            else:
                # String types - store as-is (using Text columns)
                transformed[field_name] = str(value) if value is not None else None
        except Exception as e:
            logger.debug(f"Error converting field '{field_name}': {e}")
            transformed[field_name] = None

    return transformed


# =============================================================================
# Database Operations
# =============================================================================

def sync_table_schema(engine, model_class: Type) -> List[str]:
    """
    Compare the dynamic model columns to the live database table and
    ALTER TABLE ADD COLUMN for any that are missing.

    Safe for production — only adds columns, never drops or modifies.

    Args:
        engine: SQLAlchemy engine
        model_class: Dynamic model class built from current SugarCRM metadata

    Returns:
        List of column names that were added
    """
    table_name = model_class.__tablename__
    inspector = sa_inspect(engine)

    if not inspector.has_table(table_name):
        return []  # Table doesn't exist yet; create_all will handle it

    existing_columns = {col['name'] for col in inspector.get_columns(table_name)}
    model_columns = {col.name: col for col in model_class.__table__.columns}

    missing = [
        (name, col) for name, col in model_columns.items()
        if name not in existing_columns
    ]

    if not missing:
        return []

    added = []
    with engine.begin() as conn:
        for col_name, col in missing:
            # Map SQLAlchemy type to a safe SQL type string
            col_type = col.type.compile(dialect=engine.dialect)
            stmt = f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_type} NULL'
            logger.info(f"Adding missing column: {table_name}.{col_name} ({col_type})")
            conn.execute(text(stmt))
            added.append(col_name)

    return added


def push_records_to_database(
    records: List[Dict[str, Any]],
    model_class: Type,
    config: DataLayerConfig,
    chunk_size: int = DEFAULT_SQL_CHUNK_SIZE
) -> int:
    """
    Push records to PostgreSQL database using upsert.

    Args:
        records: List of transformed record dictionaries
        model_class: SQLAlchemy model class
        config: Database configuration
        chunk_size: Records per upsert chunk

    Returns:
        Number of records processed
    """
    if not records:
        return 0

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    # Create table if not exists, then add any missing columns
    DynamicBase.metadata.create_all(engine, tables=[model_class.__table__])
    added_cols = sync_table_schema(engine, model_class)
    if added_cols:
        tqdm.write(f"  Table '{model_class.__tablename__}': added {len(added_cols)} new column(s): {', '.join(added_cols)}")
    else:
        tqdm.write(f"  Table '{model_class.__tablename__}' ready")

    session_manager = SessionManager(engine)
    num_chunks = (len(records) + chunk_size - 1) // chunk_size
    total_processed = 0

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        with tqdm(total=len(records), desc="  Upserting records", unit="rec") as pbar:
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]

                upsert_ops.upsert_batch(
                    model=model_class,
                    records=chunk,
                    constraint_columns=['sugar_id'],
                    chunk_size=chunk_size
                )

                total_processed += len(chunk)
                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i//chunk_size + 1}/{num_chunks}"})

    return total_processed


# =============================================================================
# Pipeline Functions
# =============================================================================

def process_module(
    client: SugarCRMClient,
    module: str,
    config: DataLayerConfig,
    filter_expr: Optional[List[Dict]] = None,
    limit: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    sql_chunk_size: int = DEFAULT_SQL_CHUNK_SIZE
) -> int:
    """
    Process a single SugarCRM module.

    Args:
        client: SugarCRM API client
        module: Module name
        config: Database configuration
        filter_expr: Optional filter expression
        limit: Maximum records to process (for testing)
        batch_size: API batch size
        sql_chunk_size: SQL upsert chunk size

    Returns:
        Number of records processed
    """
    print(f"\n{'='*60}")
    print(f"Processing module: {module}")
    print(f"{'='*60}")

    # Get module metadata
    print(f"  Fetching metadata for {module}...")
    field_defs, error = client.get_module_fields(module)
    if error:
        print(f"  ERROR: Could not get metadata for {module}: {error}")
        return 0

    print(f"  Found {len(field_defs)} fields")

    # Get record count
    count, error = client.count_records(module, filter_expr)
    if error:
        print(f"  WARNING: Could not get count: {error}")
        count = "unknown"
    else:
        print(f"  Total records: {count:,}")

    if limit and isinstance(count, int) and count > limit:
        print(f"  Limiting to {limit:,} records (--limit)")

    # Create dynamic model
    model_class = create_dynamic_model(module, field_defs)

    # Get valid column names from the model
    valid_columns = set(c.name for c in model_class.__table__.columns)
    logger.debug(f"Valid columns for {module}: {len(valid_columns)}")

    # Fetch and process records in batches
    total_processed = 0
    all_records = []
    all_dimensions: Dict[str, List[Dict[str, Any]]] = {}  # Accumulate dimension records

    print(f"\n  Fetching records from SugarCRM...")

    with tqdm(total=limit or count if isinstance(count, int) else None,
              desc=f"  Fetching {module}", unit="rec") as pbar:

        for batch in client.fetch_all_records(
            module=module,
            filter_expr=filter_expr,
            fields=None,  # Get all fields
            batch_size=batch_size,
            order_by='date_modified:ASC'
        ):
            # Transform batch
            for record in batch:
                # Extract dimensions from raw record (before transformation)
                dims = extract_dimensions(record)
                for table_name, dim_records in dims.items():
                    if table_name not in all_dimensions:
                        all_dimensions[table_name] = []
                    all_dimensions[table_name].extend(dim_records)

                # Transform for main table
                transformed = transform_record(record, field_defs, valid_columns)
                all_records.append(transformed)
                pbar.update(1)

                if limit and len(all_records) >= limit:
                    break

            # Push to database periodically (every 5000 records)
            if len(all_records) >= 5000:
                print(f"\n  Pushing {len(all_records)} records to database...")
                processed = push_records_to_database(all_records, model_class, config, sql_chunk_size)
                total_processed += processed
                all_records = []

                # Push dimensions periodically too
                if all_dimensions:
                    dim_results = push_dimensions_to_database(all_dimensions, config)
                    for tbl, cnt in dim_results.items():
                        tqdm.write(f"    Dimension {tbl}: {cnt} records")
                    all_dimensions = {}

            if limit and total_processed + len(all_records) >= limit:
                break

    # Push remaining records
    if all_records:
        print(f"\n  Pushing final {len(all_records)} records to database...")
        processed = push_records_to_database(all_records, model_class, config, sql_chunk_size)
        total_processed += processed

    # Push remaining dimensions
    if all_dimensions:
        print(f"\n  Pushing dimension tables...")
        dim_results = push_dimensions_to_database(all_dimensions, config)
        for tbl, cnt in dim_results.items():
            print(f"    {tbl}: {cnt} unique records")

    print(f"\n  Completed {module}: {total_processed:,} records processed")
    return total_processed


def run_backfill(
    modules: List[str],
    config: DataLayerConfig,
    since_date: Optional[date] = None,
    limit: Optional[int] = None,
    single_module: Optional[str] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    sql_chunk_size: int = DEFAULT_SQL_CHUNK_SIZE
) -> Dict[str, int]:
    """
    Run backfill mode - fetch all/filtered records.

    Args:
        modules: List of modules to process
        config: Database configuration
        since_date: Optional date filter (date_modified >= since_date)
        limit: Maximum records per module (for testing)
        single_module: Process only this module
        batch_size: API batch size
        sql_chunk_size: SQL upsert chunk size

    Returns:
        Dict of module -> record count
    """
    results = {}

    # Create SugarCRM client
    client = SugarCRMClient.from_env()
    if not client.authenticate():
        raise RuntimeError("Failed to authenticate with SugarCRM")

    try:
        # Filter modules if single_module specified
        if single_module:
            if single_module not in modules:
                modules = [single_module]
            else:
                modules = [single_module]

        # Build filter expression
        filter_expr = None
        if since_date:
            filter_expr = [{'date_modified': {'$gte': since_date.isoformat()}}]

        # Process each module
        for module in modules:
            try:
                count = process_module(
                    client=client,
                    module=module,
                    config=config,
                    filter_expr=filter_expr,
                    limit=limit,
                    batch_size=batch_size,
                    sql_chunk_size=sql_chunk_size
                )
                results[module] = count
            except Exception as e:
                logger.error(f"Error processing {module}: {e}")
                results[module] = 0

    finally:
        client.logout()

    return results


def run_auto(
    modules: List[str],
    config: DataLayerConfig,
    incremental_days: int = DEFAULT_INCREMENTAL_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    sql_chunk_size: int = DEFAULT_SQL_CHUNK_SIZE
) -> Dict[str, int]:
    """
    Run auto mode - incremental update for recent records.

    Args:
        modules: List of modules to process
        config: Database configuration
        incremental_days: Days to look back
        batch_size: API batch size
        sql_chunk_size: SQL upsert chunk size

    Returns:
        Dict of module -> record count
    """
    since_date = date.today() - timedelta(days=incremental_days)
    print(f"Auto mode: Fetching records modified since {since_date}")

    return run_backfill(
        modules=modules,
        config=config,
        since_date=since_date,
        batch_size=batch_size,
        sql_chunk_size=sql_chunk_size
    )


# =============================================================================
# CLI and Main
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='SugarCRM to SQL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill all configured modules
  python sugarcrm_to_sql.py --mode backfill

  # Backfill specific module
  python sugarcrm_to_sql.py --mode backfill --module Leads

  # Incremental update (last 7 days)
  python sugarcrm_to_sql.py --mode auto

  # With date filter
  python sugarcrm_to_sql.py --mode backfill --since 2024-01-01

  # Test with limited records
  python sugarcrm_to_sql.py --mode backfill --module Contacts --limit 100
        """
    )

    parser.add_argument(
        '--mode',
        choices=['backfill', 'auto'],
        required=True,
        help='Extraction mode: backfill (all/filtered), auto (incremental)'
    )

    parser.add_argument(
        '--module',
        type=str,
        help='Process only this module (e.g., Leads, Contacts)'
    )

    parser.add_argument(
        '--since',
        type=str,
        help='Filter records modified since this date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum records per module (for testing)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=None,
        help=f'API batch size (default: {DEFAULT_BATCH_SIZE})'
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=None,
        help=f'SQL upsert chunk size (default: {DEFAULT_SQL_CHUNK_SIZE})'
    )

    return parser.parse_args()


def main():
    """Main function."""
    args = parse_args()

    # Load configuration
    config = DataLayerConfig.from_env()

    # Load SugarCRM-specific config from unified config
    modules = get_pipeline_config('sugarcrm', 'modules', DEFAULT_MODULES)
    batch_size = args.batch_size or get_pipeline_config('sugarcrm', 'batch_size', DEFAULT_BATCH_SIZE)
    sql_chunk_size = args.chunk_size or get_pipeline_config('sugarcrm', 'sql_chunk_size', DEFAULT_SQL_CHUNK_SIZE)
    incremental_days = get_pipeline_config('sugarcrm', 'incremental_days', DEFAULT_INCREMENTAL_DAYS)

    # Parse since date
    since_date = None
    if args.since:
        since_date = datetime.strptime(args.since, '%Y-%m-%d').date()

    # Print header
    print("=" * 70)
    print("SugarCRM to SQL Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode.upper()}")
    print(f"Modules: {', '.join(modules)}")
    if args.module:
        print(f"Single Module: {args.module}")
    if since_date:
        print(f"Since: {since_date}")
    if args.limit:
        print(f"Limit: {args.limit} records per module")
    print(f"Target: PostgreSQL - {config.databases.get('postgresql', {})}")
    print("=" * 70)

    # Run pipeline
    if args.mode == 'backfill':
        results = run_backfill(
            modules=modules,
            config=config,
            since_date=since_date,
            limit=args.limit,
            single_module=args.module,
            batch_size=batch_size,
            sql_chunk_size=sql_chunk_size
        )
    elif args.mode == 'auto':
        results = run_auto(
            modules=modules,
            config=config,
            incremental_days=incremental_days,
            batch_size=batch_size,
            sql_chunk_size=sql_chunk_size
        )
    else:
        raise ValueError(f"Unknown mode: {args.mode}")

    # Print summary
    print("\n" + "=" * 70)
    print("Pipeline completed!")
    print("=" * 70)

    total_records = 0
    for module, count in results.items():
        print(f"  {module}: {count:,} records")
        total_records += count

    print(f"\n  TOTAL: {total_records:,} records")
    print("=" * 70)


if __name__ == "__main__":
    main()
