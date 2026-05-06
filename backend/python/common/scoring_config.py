"""
Call Scoring Config

Read/write/validate the editable scoring rubric stored in the
`call_scoring_config` table in esa_pbi.

The rubric is one JSON document (see migration 049) with this shape:

    {
      "model": "grok-3-mini",
      "temperature": 0.2,
      "max_tokens": 1500,
      "system_prompt": "...",
      "context_hints": {...},
      "dimensions": [
        {
          "key": "quality_politeness",
          "label": "Politeness",
          "type": "int",
          "min": 1,
          "max": 10,
          "applies_to": "all" | "sales" | "support",
          "sugar_field": "zoom_quality_politeness_c",
          "rubric": "1=...",
          "enabled": true
        },
        ...
      ]
    }

Functions:
- `get_active_config()`       — fetch + cache the active config (5-min TTL)
- `save_config(config_dict, updated_by)` — validate, version-bump, archive prior row
- `validate_config(config_dict)` — return list[str] of error messages (empty = valid)
- `invalidate_cache()`        — force the next get_active_config() to re-fetch
"""

import logging
import threading
import time
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

CACHE_TTL_SEC = 300  # 5 minutes
VALID_DIMENSION_TYPES = ('int', 'enum', 'text', 'bool')
VALID_APPLIES_TO = ('all', 'sales', 'support')

# Module-level cache, guarded by a lock for thread-safety
_cache_lock = threading.Lock()
_cached_config: Optional[Dict[str, Any]] = None
_cached_at: float = 0.0
_cached_version: int = 0
_engine: Optional[Engine] = None


def _get_engine() -> Engine:
    """Lazy-init the esa_pbi engine for this module."""
    global _engine
    if _engine is None:
        from common.config_loader import get_database_url
        _engine = create_engine(get_database_url('pbi'), pool_pre_ping=True, pool_recycle=300)
    return _engine


def invalidate_cache() -> None:
    """Force the next call to get_active_config() to re-read from DB."""
    global _cached_config, _cached_at, _cached_version
    with _cache_lock:
        _cached_config = None
        _cached_at = 0.0
        _cached_version = 0


def get_active_config(force_refresh: bool = False) -> Dict[str, Any]:
    """Return the active scoring config dict, with 5-minute in-process cache.

    The returned dict has an extra `_version` key (int) so callers can record
    which rubric version was used to score a given call.
    """
    global _cached_config, _cached_at, _cached_version

    now = time.monotonic()
    with _cache_lock:
        if not force_refresh and _cached_config is not None and (now - _cached_at) < CACHE_TTL_SEC:
            return _cached_config

    # Fetch fresh
    engine = _get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT config_json, version
            FROM call_scoring_config
            WHERE name = 'default' AND is_active = TRUE
            LIMIT 1
        """)).fetchone()

    if not row:
        raise RuntimeError(
            "No active call_scoring_config found. Run migration 049 or seed the table."
        )

    cfg = dict(row[0]) if row[0] else {}
    cfg['_version'] = int(row[1])

    with _cache_lock:
        _cached_config = cfg
        _cached_at = now
        _cached_version = int(row[1])

    return cfg


def validate_config(config: Dict[str, Any]) -> List[str]:
    """Validate a config dict. Returns list of error strings (empty if valid)."""
    errors: List[str] = []

    if not isinstance(config, dict):
        return ['Config must be a JSON object']

    # Required top-level keys
    for key in ('model', 'system_prompt', 'dimensions'):
        if key not in config:
            errors.append(f"Missing required key: {key!r}")

    if 'temperature' in config:
        try:
            t = float(config['temperature'])
            if not 0.0 <= t <= 2.0:
                errors.append("temperature must be between 0.0 and 2.0")
        except (TypeError, ValueError):
            errors.append("temperature must be a number")

    if 'max_tokens' in config:
        try:
            mt = int(config['max_tokens'])
            if mt < 500 or mt > 16000:
                errors.append("max_tokens must be between 500 and 16000")
        except (TypeError, ValueError):
            errors.append("max_tokens must be an integer")

    dims = config.get('dimensions')
    if not isinstance(dims, list) or not dims:
        errors.append("dimensions must be a non-empty list")
        return errors  # can't validate further

    seen_keys = set()
    seen_sugar_fields = set()
    for i, d in enumerate(dims):
        prefix = f"dimensions[{i}]"
        if not isinstance(d, dict):
            errors.append(f"{prefix} must be an object")
            continue

        # Required per-dimension fields
        for k in ('key', 'type', 'sugar_field', 'applies_to', 'rubric'):
            if k not in d or d[k] in (None, ''):
                errors.append(f"{prefix}.{k} is required")

        key = d.get('key')
        if key:
            if key in seen_keys:
                errors.append(f"{prefix}.key {key!r} is duplicated")
            seen_keys.add(key)
            if not isinstance(key, str) or not key.replace('_', '').isalnum():
                errors.append(f"{prefix}.key must be alphanumeric/underscore: {key!r}")

        sf = d.get('sugar_field')
        if sf:
            if sf in seen_sugar_fields:
                errors.append(f"{prefix}.sugar_field {sf!r} is duplicated")
            seen_sugar_fields.add(sf)

        dtype = d.get('type')
        if dtype and dtype not in VALID_DIMENSION_TYPES:
            errors.append(f"{prefix}.type must be one of {VALID_DIMENSION_TYPES}, got {dtype!r}")

        applies = d.get('applies_to')
        if applies and applies not in VALID_APPLIES_TO:
            errors.append(f"{prefix}.applies_to must be one of {VALID_APPLIES_TO}, got {applies!r}")

        if dtype == 'int':
            try:
                lo = int(d.get('min', 1))
                hi = int(d.get('max', 10))
                if lo >= hi:
                    errors.append(f"{prefix}.min must be < max")
            except (TypeError, ValueError):
                errors.append(f"{prefix}.min/max must be integers for type=int")

        if dtype == 'enum':
            vals = d.get('values')
            vbp = d.get('values_by_parent')
            # Accept either a flat list of values OR a hierarchical dict
            if vbp is not None:
                if not isinstance(vbp, dict) or not vbp:
                    errors.append(f"{prefix}.values_by_parent must be a non-empty object")
                else:
                    for pkey, plist in vbp.items():
                        if not isinstance(plist, list) or not plist:
                            errors.append(
                                f"{prefix}.values_by_parent[{pkey!r}] must be a non-empty list"
                            )
                if not d.get('parent_key'):
                    errors.append(
                        f"{prefix}.parent_key is required when values_by_parent is set"
                    )
            else:
                if not isinstance(vals, list) or not vals:
                    errors.append(f"{prefix}.values must be a non-empty list for type=enum")

        if dtype == 'text':
            if 'max_length' in d:
                try:
                    ml = int(d['max_length'])
                    if ml < 1 or ml > 5000:
                        errors.append(f"{prefix}.max_length must be 1..5000")
                except (TypeError, ValueError):
                    errors.append(f"{prefix}.max_length must be an integer")

    return errors


def save_config(config: Dict[str, Any], updated_by: str = 'unknown') -> int:
    """Validate and persist a new version of the scoring config.

    Archives the prior row to call_scoring_config_history, bumps the version,
    and invalidates the in-process cache so the next pipeline run picks up the
    change.

    Returns:
        The new version number.

    Raises:
        ValueError if the config fails validation.
    """
    # Strip the synthetic _version field if the UI round-tripped it back
    cfg = {k: v for k, v in config.items() if k != '_version'}

    errors = validate_config(cfg)
    if errors:
        raise ValueError("Config validation failed:\n  - " + "\n  - ".join(errors))

    import json as _json
    config_str = _json.dumps(cfg)
    engine = _get_engine()

    with engine.begin() as conn:
        # Archive current row
        prior = conn.execute(text("""
            SELECT id, name, config_json, version
            FROM call_scoring_config
            WHERE name = 'default' AND is_active = TRUE
            LIMIT 1
        """)).fetchone()

        if prior:
            conn.execute(text("""
                INSERT INTO call_scoring_config_history
                    (config_id, name, config_json, version, updated_by)
                VALUES (:cid, :name, :cfg, :version, :ub)
            """), {
                'cid': prior[0],
                'name': prior[1],
                'cfg': _json.dumps(prior[2]),
                'version': prior[3],
                'ub': updated_by,
            })

            new_version = (prior[3] or 1) + 1
            conn.execute(text("""
                UPDATE call_scoring_config
                SET config_json = CAST(:cfg AS jsonb),
                    version = :version,
                    updated_by = :ub,
                    updated_at = NOW()
                WHERE id = :cid
            """), {
                'cfg': config_str,
                'version': new_version,
                'ub': updated_by,
                'cid': prior[0],
            })
        else:
            new_version = 1
            conn.execute(text("""
                INSERT INTO call_scoring_config (name, config_json, version, updated_by, is_active)
                VALUES ('default', CAST(:cfg AS jsonb), :version, :ub, TRUE)
            """), {
                'cfg': config_str,
                'version': new_version,
                'ub': updated_by,
            })

    invalidate_cache()
    logger.info("Saved call_scoring_config v%d by %s", new_version, updated_by)
    return new_version


def get_history(limit: int = 20) -> List[Dict[str, Any]]:
    """Return recent config versions from the history table (newest first)."""
    engine = _get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, version, updated_by, archived_at
            FROM call_scoring_config_history
            ORDER BY archived_at DESC
            LIMIT :limit
        """), {'limit': limit}).fetchall()
    return [
        {'id': r[0], 'version': r[1], 'updated_by': r[2], 'archived_at': r[3].isoformat() if r[3] else None}
        for r in rows
    ]
