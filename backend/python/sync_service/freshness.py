"""
Freshness checks — query target tables directly for self-healing age detection.

No tracking table involved. The "last sync time" is whatever the data says it is.
Supports per-scope queries via the freshness_scope_column on SyncPipeline.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text

from sync_service.config import get_engine
from sync_service.models import SyncPipeline

logger = logging.getLogger(__name__)

# Strict identifier regex (PostgreSQL: letters, digits, underscore, starts with letter/_)
_IDENT_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def _validate_identifier(name: str, label: str) -> str:
    """Guard against SQL injection via unquoted identifiers.

    The freshness_table and freshness_column values come from the
    sync_pipelines table, which is writable via API — so we MUST validate
    before interpolating into raw SQL.
    """
    if not name:
        raise ValueError(f"{label} is empty")
    if not _IDENT_RE.match(name):
        raise ValueError(f"{label} contains invalid characters: {name!r}")
    return name


def _pick_scope_value(scope: Optional[Dict[str, Any]], scope_column: Optional[str]) -> Optional[Any]:
    """Extract the scope filter value for the pipeline's scope_column.

    Scope dict can use either:
      - {"site_code": "L017"}          (single value)
      - {"site_codes": ["L017", ...]}  (list — freshness uses the newest-stale one)

    Returns the value to filter by, or None if scope doesn't target this column.
    """
    if not scope or not scope_column:
        return None

    # Try direct match first
    if scope_column in scope:
        return scope[scope_column]

    # Try plural form (site_code → site_codes)
    plural = scope_column + 's'
    if plural in scope:
        v = scope[plural]
        if isinstance(v, (list, tuple)) and len(v) == 1:
            return v[0]
        # Multi-value scope can't use a single MAX() query efficiently —
        # caller should loop or pick the least-fresh one
        return v

    return None


def check_freshness(
    pipeline: SyncPipeline,
    scope: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    """Return age in seconds of the freshest record in the pipeline's target table.

    Args:
        pipeline: Pipeline registry row
        scope: Optional scope dict (e.g., {"site_code": "L017"})

    Returns:
        Age in seconds (float) if a row exists, else None (meaning "never synced")

    Raises:
        ValueError if pipeline freshness config is invalid
    """
    if not pipeline.freshness_table or not pipeline.freshness_column:
        logger.debug(f"Pipeline {pipeline.pipeline_name} has no freshness config")
        return None

    table = _validate_identifier(pipeline.freshness_table, 'freshness_table')
    column = _validate_identifier(pipeline.freshness_column, 'freshness_column')

    sql_parts = [f'SELECT MAX("{column}") FROM "{table}"']
    params: Dict[str, Any] = {}

    scope_value = _pick_scope_value(scope, pipeline.freshness_scope_column)
    if scope_value is not None and pipeline.freshness_scope_column:
        scope_col = _validate_identifier(
            pipeline.freshness_scope_column, 'freshness_scope_column'
        )
        if isinstance(scope_value, (list, tuple)):
            sql_parts.append(f'WHERE "{scope_col}" = ANY(:scope_value)')
            params['scope_value'] = list(scope_value)
        else:
            sql_parts.append(f'WHERE "{scope_col}" = :scope_value')
            params['scope_value'] = scope_value

    sql = ' '.join(sql_parts)

    engine = get_engine(pipeline.freshness_database)
    with engine.connect() as conn:
        row = conn.execute(text(sql), params).fetchone()

    if not row or row[0] is None:
        return None

    latest = row[0]
    if isinstance(latest, datetime):
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - latest).total_seconds()
        return age

    # Fallback — column wasn't a timestamp, can't compute age
    logger.warning(
        f"Freshness column {table}.{column} is not a datetime: {type(latest).__name__}"
    )
    return None


def is_stale(
    pipeline: SyncPipeline,
    scope: Optional[Dict[str, Any]] = None,
    max_age_seconds: Optional[int] = None,
) -> bool:
    """Return True if data is stale and needs refresh.

    Uses max_age_seconds override if provided, else the pipeline's default TTL.
    If no data exists (never synced), returns True (stale).
    """
    ttl = max_age_seconds if max_age_seconds is not None else pipeline.freshness_ttl_seconds
    age = check_freshness(pipeline, scope)
    if age is None:
        return True  # no data → definitely stale
    return age > ttl
