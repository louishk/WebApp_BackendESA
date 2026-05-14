"""
SugarCrmLeadsPipeline — sync CRM data (Leads, Contacts, Accounts, etc.) from
SugarCRM into esa_pbi sugarcrm_* tables.

Features:
- Dynamic table generation from SugarCRM metadata
- Two modes: backfill (all/filtered data), auto (incremental by date_modified)
- Pagination handling for 400K+ records
- Simple primary key (sugar_id) — cumulative, no snapshots
- Chunked upsert for large datasets
- Schema drift detection: ALTER TABLE ADD COLUMN for new SugarCRM fields

Scope keys honoured (all optional):
  - mode:   'auto' | 'backfill'     default 'auto'
  - module: restrict to one module  (Leads, Contacts, …)
  - since:  'YYYY-MM-DD'            overrides watermark
  - limit:  int cap per module      (testing)

Configuration (scheduler.yaml):
  pipelines.sugarcrm.modules
  pipelines.sugarcrm.batch_size         (default 200)
  pipelines.sugarcrm.sql_chunk_size     (default 500)
  pipelines.sugarcrm.incremental_days   (default 7)
"""

import logging
import re
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Type

from sqlalchemy import (
    Column, String, Integer, DateTime, Date, Boolean, Numeric, Text,
    inspect as sa_inspect, text,
)
from sqlalchemy.ext.declarative import declarative_base

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_MODULES = ['Leads', 'Contacts', 'Accounts', 'Opportunities']
DEFAULT_BATCH_SIZE = 200
DEFAULT_SQL_CHUNK_SIZE = 500
DEFAULT_INCREMENTAL_DAYS = 7

EXCLUDED_FIELD_TYPES = {
    'link', 'collection', 'team_list', 'locked_fields', 'json',
}

DIMENSION_MAPPINGS = {
    'created_by_link':    ('sugarcrm_dim_users',     'full_name'),
    'modified_user_link': ('sugarcrm_dim_users',     'full_name'),
    'assigned_user_link': ('sugarcrm_dim_users',     'full_name'),
    'accounts':           ('sugarcrm_dim_accounts',  'name'),
    'contacts':           ('sugarcrm_dim_contacts',  'name'),
    'campaign_leads':     ('sugarcrm_dim_campaigns', 'name'),
}

_SAFE_IDENT = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,62}$')

# ---------------------------------------------------------------------------
# Dynamic declarative base (separate from common Base so create_all is scoped)
# ---------------------------------------------------------------------------

DynamicBase = declarative_base()

# Cache: module/table name → model class
_dynamic_models: Dict[str, Type] = {}
_dimension_models: Dict[str, Type] = {}


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

def _map_sugar_type(sugar_type: str) -> Column:
    t = (sugar_type or 'varchar').lower()
    if t in ('varchar', 'name', 'phone', 'url', 'email', 'id', 'link', 'char',
             'text', 'longtext', 'html', 'enum', 'multienum', 'radioenum',
             'relate', 'parent', 'parent_type'):
        return Column(Text, nullable=True)
    if t in ('int', 'integer', 'tinyint'):
        return Column(Integer, nullable=True)
    if t in ('decimal', 'float', 'double', 'currency'):
        return Column(Numeric(18, 4), nullable=True)
    if t in ('bool', 'boolean'):
        return Column(Boolean, nullable=True)
    if t == 'date':
        return Column(Date, nullable=True)
    if t in ('datetime', 'datetimecombo'):
        return Column(DateTime, nullable=True)
    logger.debug("Unknown SugarCRM type '%s', using Text", t)
    return Column(Text, nullable=True)


# ---------------------------------------------------------------------------
# Dynamic model creation
# ---------------------------------------------------------------------------

def _create_dynamic_model(module_name: str, field_defs: Dict[str, Dict]) -> Type:
    table_name = f'sugarcrm_{module_name.lower()}'
    if table_name in _dynamic_models:
        return _dynamic_models[table_name]

    columns: Dict[str, Any] = {
        '__tablename__': table_name,
        'sugar_id': Column(String(36), primary_key=True, nullable=False,
                           comment='SugarCRM record UUID'),
        'synced_at': Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow,
                            nullable=False, comment='Last sync timestamp'),
    }

    skipped = []
    for field_name, field_info in field_defs.items():
        if field_name == 'id':
            continue
        if field_name.startswith('_') or field_name == 'my_favorite':
            continue
        sugar_type = field_info.get('type', 'varchar') if isinstance(field_info, dict) else 'varchar'
        if sugar_type in EXCLUDED_FIELD_TYPES:
            skipped.append(field_name)
            continue
        columns[field_name] = _map_sugar_type(sugar_type)

    if skipped:
        logger.debug("Skipped %d link/collection fields for %s: %s...",
                     len(skipped), module_name, skipped[:5])

    model_class = type(f'SugarCRM{module_name}', (DynamicBase,), columns)
    _dynamic_models[table_name] = model_class
    logger.info("Created dynamic model '%s' with %d data fields",
                table_name, len(columns) - 4)
    return model_class


def _get_dimension_model(table_name: str, name_field: str) -> Type:
    if table_name in _dimension_models:
        return _dimension_models[table_name]

    columns = {
        '__tablename__': table_name,
        'id': Column(String(36), primary_key=True, nullable=False,
                     comment='SugarCRM record ID'),
        name_field: Column(Text, nullable=True, comment='Display name'),
        'synced_at': Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow,
                            nullable=False, comment='Last sync timestamp'),
    }
    model_class = type(
        table_name.replace('_', ' ').title().replace(' ', ''),
        (DynamicBase,),
        columns,
    )
    _dimension_models[table_name] = model_class
    logger.info("Created dimension model '%s'", table_name)
    return model_class


# ---------------------------------------------------------------------------
# Schema sync (ADD COLUMN only — never drops)
# ---------------------------------------------------------------------------

def _sync_table_schema(engine, model_class: Type) -> List[str]:
    table_name = model_class.__tablename__
    inspector = sa_inspect(engine)
    if not inspector.has_table(table_name):
        return []

    existing = {col['name'] for col in inspector.get_columns(table_name)}
    missing = [
        (name, col)
        for name, col in model_class.__table__.columns.items()
        if name not in existing
    ]
    if not missing:
        return []

    added = []
    with engine.begin() as conn:
        for col_name, col in missing:
            if not _SAFE_IDENT.match(col_name) or not _SAFE_IDENT.match(table_name):
                logger.warning("Skipping unsafe identifier: %s.%r", table_name, col_name)
                continue
            col_type = col.type.compile(dialect=engine.dialect)
            stmt = f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_type} NULL'
            logger.info("Adding missing column: %s.%s (%s)", table_name, col_name, col_type)
            conn.execute(text(stmt))
            added.append(col_name)

    return added


# ---------------------------------------------------------------------------
# Record transformation
# ---------------------------------------------------------------------------

def _transform_record(
    record: Dict[str, Any],
    field_defs: Dict[str, Dict],
    valid_columns: Optional[set] = None,
) -> Dict[str, Any]:
    from common import convert_to_bool, convert_to_int, convert_to_decimal, convert_to_datetime

    transformed = {'sugar_id': record.get('id')}

    for field_name, value in record.items():
        if field_name == 'id':
            continue
        if field_name.startswith('_') or field_name == 'my_favorite':
            continue
        if valid_columns and field_name not in valid_columns:
            continue

        field_info = field_defs.get(field_name, {})
        sugar_type = (
            field_info.get('type', 'varchar').lower()
            if isinstance(field_info, dict)
            else 'varchar'
        )
        if sugar_type in EXCLUDED_FIELD_TYPES:
            continue

        try:
            if value is None or value == '':
                transformed[field_name] = None
            elif sugar_type in ('int', 'integer', 'tinyint'):
                transformed[field_name] = convert_to_int(value)
            elif sugar_type in ('decimal', 'float', 'double', 'currency'):
                transformed[field_name] = convert_to_decimal(value)
            elif sugar_type in ('bool', 'boolean'):
                transformed[field_name] = convert_to_bool(value)
            elif sugar_type in ('datetime', 'datetimecombo'):
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
                email_value = value
                if isinstance(value, str) and value.startswith('[') and len(value) <= 4096:
                    try:
                        import ast
                        email_value = ast.literal_eval(value)
                    except (ValueError, SyntaxError):
                        email_value = value
                if isinstance(email_value, list):
                    addr = None
                    for obj in email_value:
                        if isinstance(obj, dict):
                            if obj.get('primary_address'):
                                addr = obj.get('email_address')
                                break
                            addr = addr or obj.get('email_address')
                    transformed[field_name] = addr
                elif isinstance(email_value, dict):
                    transformed[field_name] = email_value.get('email_address')
                elif isinstance(email_value, str) and '@' in email_value:
                    transformed[field_name] = email_value
                else:
                    transformed[field_name] = None
            elif sugar_type == 'multienum':
                if isinstance(value, list):
                    non_empty = [str(v) for v in value if v and str(v).strip()]
                    transformed[field_name] = ','.join(non_empty) if non_empty else None
                else:
                    transformed[field_name] = str(value) if value else None
            elif sugar_type == 'relate' and isinstance(value, list):
                name_val = None
                for item in value:
                    if isinstance(item, dict):
                        if item.get('primary'):
                            name_val = item.get('name')
                            break
                        name_val = name_val or item.get('name')
                transformed[field_name] = name_val
            elif isinstance(value, (list, dict)):
                if isinstance(value, dict) and 'name' in value:
                    transformed[field_name] = value.get('name')
                elif isinstance(value, dict) and 'id' in value:
                    transformed[field_name] = value.get('id')
                elif isinstance(value, list) and len(value) == 0:
                    transformed[field_name] = None
                else:
                    logger.debug("Skipping complex nested field '%s'", field_name)
                    continue
            else:
                transformed[field_name] = str(value) if value is not None else None
        except Exception as e:
            logger.debug("Error converting field '%s': %s", field_name, e)
            transformed[field_name] = None

    return transformed


def _extract_dimensions(record: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    dimensions: Dict[str, List[Dict[str, Any]]] = {}
    for link_field, (table_name, name_field) in DIMENSION_MAPPINGS.items():
        nested = record.get(link_field)
        if not isinstance(nested, dict):
            continue
        dim_id = nested.get('id')
        if not dim_id:
            continue
        dim_name = nested.get(name_field) or nested.get('name') or nested.get('full_name')
        dimensions.setdefault(table_name, []).append({'id': dim_id, name_field: dim_name})
    return dimensions


# ---------------------------------------------------------------------------
# Database write helpers
# ---------------------------------------------------------------------------

def _push_dimensions(
    dimensions: Dict[str, List[Dict[str, Any]]],
    config,
    engine,
) -> None:
    from common import SessionManager, UpsertOperations

    db_config = config.databases.get('postgresql')
    if not db_config or not dimensions:
        return

    session_manager = SessionManager(engine)
    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)
        for table_name, records in dimensions.items():
            if not records:
                continue
            seen: set = set()
            unique = [r for r in records if not (r['id'] in seen or seen.add(r['id']))]

            name_field = 'name'
            for _, (tbl, nf) in DIMENSION_MAPPINGS.items():
                if tbl == table_name:
                    name_field = nf
                    break

            model_class = _get_dimension_model(table_name, name_field)
            DynamicBase.metadata.create_all(engine, tables=[model_class.__table__])
            upsert_ops.upsert_batch(
                model=model_class,
                records=unique,
                constraint_columns=['id'],
                chunk_size=DEFAULT_SQL_CHUNK_SIZE,
            )
            logger.info("sugarcrm dimensions %s: %d records", table_name, len(unique))


def _push_records(
    records: List[Dict[str, Any]],
    model_class: Type,
    config,
    chunk_size: int,
    engine,
) -> int:
    from common import SessionManager, UpsertOperations

    if not records:
        return 0

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    DynamicBase.metadata.create_all(engine, tables=[model_class.__table__])
    added_cols = _sync_table_schema(engine, model_class)
    if added_cols:
        logger.info("Table '%s': added %d new column(s): %s",
                    model_class.__tablename__, len(added_cols), ', '.join(added_cols))

    session_manager = SessionManager(engine)
    total = 0
    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            upsert_ops.upsert_batch(
                model=model_class,
                records=chunk,
                constraint_columns=['sugar_id'],
                chunk_size=chunk_size,
            )
            total += len(chunk)

    return total


# ---------------------------------------------------------------------------
# Module-level processing
# ---------------------------------------------------------------------------

def _process_module(
    client,
    module: str,
    config,
    engine,
    filter_expr: Optional[List[Dict]] = None,
    limit: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    sql_chunk_size: int = DEFAULT_SQL_CHUNK_SIZE,
) -> int:
    from common.data_utils import adaptive_batch_params

    logger.info("sugarcrm processing module: %s", module)

    field_defs, error = client.get_module_fields(module)
    if error:
        logger.error("Could not get metadata for %s: %s", module, error)
        return 0
    logger.info("sugarcrm %s: %d fields in metadata", module, len(field_defs))

    count, error = client.count_records(module, filter_expr)
    if error:
        logger.warning("Could not get record count for %s: %s", module, error)
        count = None
    else:
        logger.info("sugarcrm %s: %d records to sync", module, count)

    params = adaptive_batch_params(count, batch_size, sql_chunk_size)
    if params.is_large:
        logger.info("sugarcrm %s: large dataset — scaling batch params", module)
        batch_size = params.api_batch_size
        sql_chunk_size = params.sql_chunk_size
        client.timeout = params.client_timeout

    model_class = _create_dynamic_model(module, field_defs)
    valid_columns = {c.name for c in model_class.__table__.columns}

    total_processed = 0
    buffer: List[Dict[str, Any]] = []
    dim_buffer: Dict[str, List[Dict[str, Any]]] = {}

    for batch in client.fetch_all_records(
        module=module,
        filter_expr=filter_expr,
        fields=None,
        batch_size=batch_size,
        order_by='date_modified:ASC',
    ):
        for record in batch:
            for tbl, recs in _extract_dimensions(record).items():
                dim_buffer.setdefault(tbl, []).extend(recs)
            buffer.append(_transform_record(record, field_defs, valid_columns))
            if limit and len(buffer) + total_processed >= limit:
                break

        if len(buffer) >= params.push_threshold:
            total_processed += _push_records(buffer, model_class, config, sql_chunk_size, engine)
            logger.info("sugarcrm %s: pushed %d records so far", module, total_processed)
            buffer = []
            if dim_buffer:
                _push_dimensions(dim_buffer, config, engine)
                dim_buffer = {}

        if limit and total_processed + len(buffer) >= limit:
            break

    if buffer:
        total_processed += _push_records(buffer, model_class, config, sql_chunk_size, engine)
    if dim_buffer:
        _push_dimensions(dim_buffer, config, engine)

    logger.info("sugarcrm %s: complete — %d records", module, total_processed)
    return total_processed


# ---------------------------------------------------------------------------
# Public run() orchestrator
# ---------------------------------------------------------------------------

def run(
    mode: str = 'auto',
    module: Optional[str] = None,
    since: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Sync SugarCRM modules into esa_pbi sugarcrm_* tables.

    Returns {'records': int, 'modules': {module: count}, 'mode': str}
    """
    from common import DataLayerConfig
    from common.config import get_pipeline_config
    from common.db import get_engine
    from common.sugarcrm_client import SugarCRMClient

    config = DataLayerConfig.from_env()
    modules: List[str] = get_pipeline_config('sugarcrm', 'modules', DEFAULT_MODULES)
    batch_size: int = get_pipeline_config('sugarcrm', 'batch_size', DEFAULT_BATCH_SIZE)
    sql_chunk_size: int = get_pipeline_config('sugarcrm', 'sql_chunk_size', DEFAULT_SQL_CHUNK_SIZE)
    incremental_days: int = get_pipeline_config('sugarcrm', 'incremental_days', DEFAULT_INCREMENTAL_DAYS)

    # Resolve filter
    filter_expr: Optional[List[Dict]] = None
    if since:
        try:
            since_date = datetime.strptime(since, '%Y-%m-%d').date()
        except ValueError:
            logger.warning("sugarcrm: invalid 'since' date '%s', ignoring", since)
            since_date = None
        if since_date:
            filter_expr = [{'date_modified': {'$gte': since_date.isoformat()}}]
    elif mode == 'auto':
        since_date = date.today() - timedelta(days=incremental_days)
        filter_expr = [{'date_modified': {'$gte': since_date.isoformat()}}]
        logger.info("sugarcrm auto mode: fetching records modified since %s", since_date)

    # Module list
    target_modules = [module] if module else modules

    engine = get_engine('pbi')
    client = SugarCRMClient.from_env()
    if not client.authenticate():
        raise RuntimeError("Failed to authenticate with SugarCRM")

    results: Dict[str, int] = {}
    try:
        for mod in target_modules:
            try:
                results[mod] = _process_module(
                    client=client,
                    module=mod,
                    config=config,
                    engine=engine,
                    filter_expr=filter_expr,
                    limit=limit,
                    batch_size=batch_size,
                    sql_chunk_size=sql_chunk_size,
                )
            except Exception:
                logger.exception("sugarcrm: error processing module %s", mod)
                results[mod] = 0
    finally:
        client.logout()

    total = sum(results.values())
    logger.info("sugarcrm pipeline complete: %d total records across %d modules", total, len(results))
    return {'records': total, 'modules': results, 'mode': mode}


# ---------------------------------------------------------------------------
# Pipeline class
# ---------------------------------------------------------------------------

class SugarCrmLeadsPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')
        module = scope.get('module')
        since = scope.get('since')
        limit = scope.get('limit')

        result = run(mode=mode, module=module, since=since, limit=limit)

        return RunResult(
            status='refreshed',
            records=result['records'],
            scope=scope,
            metadata={'mode': mode, 'modules': result['modules']},
        )
