"""
Admin-editable global tunables for the recommendation engine.

Each setting has a type, default value, and short description. The recommender
reads via `get_setting(key)` with an in-process cache (60 s TTL) so admin
saves propagate within a minute without restarting Gunicorn.

Source of truth is the `mw_recommender_settings` table in esa_middleware
(simple key/value). Missing rows fall back to the spec default.

Add a new tunable by appending to `_SETTINGS_SPEC` and adding a default —
the admin UI auto-renders an input row for it.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Cache TTL — short enough that admin edits propagate quickly, long enough to
# avoid hammering the DB on every recommendation request.
_CACHE_TTL_SECONDS = 60

_cache_lock = threading.Lock()
_cache: Dict[str, Any] = {}
_cache_loaded_at: float = 0.0


@dataclass(frozen=True)
class SettingSpec:
    key: str
    type_: str          # 'int' | 'float' | 'bool' | 'str'
    default: Any
    label: str
    description: str
    group: str          # 'slot' | 'pricing' | 'pool' | 'ops'
    min_value: Optional[float] = None
    max_value: Optional[float] = None


# Order here drives the order on the admin UI.
_SETTINGS_SPEC: list[SettingSpec] = [
    # ───────── Slot tuning ─────────
    SettingSpec(
        key='slot2_max_distance_km', type_='int', default=50,
        label='Slot 2 max distance (km)',
        description="Furthest neighbouring site Slot 2 will reach when the requested location has no inventory. Higher = more fallback options, lower = stay close to the customer's pick.",
        group='slot', min_value=5, max_value=200,
    ),
    SettingSpec(
        key='slot3_size_band_pct', type_='int', default=20,
        label='Slot 3 size band (±%)',
        description='Slot 3 (Best Price) widens to size buckets within ±N% of the requested size. Higher = more cheaper alternatives but less size-faithful; lower = stricter fit.',
        group='slot', min_value=5, max_value=50,
    ),
    SettingSpec(
        key='slot3_min_savings_pct', type_='int', default=0,
        label='Slot 3 minimum savings (%)',
        description='Slot 3 must be at least N% cheaper than Slot 1 to qualify. 0 = strictly cheaper. Set to e.g. 10 to only show Slot 3 when the saving is meaningful.',
        group='slot', min_value=0, max_value=50,
    ),

    # ───────── Pricing ─────────
    SettingSpec(
        key='drop_low_confidence_quotes', type_='bool', default=False,
        label='Drop low-confidence quotes',
        description="Skip candidates whose calculator returns confidence='low' (free-month / multi-month-prepay). Recommended ON once we trust the basic-discount pricing — keeps risky quotes out of the bot.",
        group='pricing',
    ),

    # ───────── Pool composition ─────────
    SettingSpec(
        key='include_legacy_default', type_='bool', default=True,
        label='Include legacy-named units by default',
        description='Emit units whose sTypeName failed SOP COM01 parsing but were rescued by inventory_type_mappings. Caller can still override per-request via constraints.include_legacy.',
        group='pool',
    ),
    SettingSpec(
        key='min_pool_size_alert', type_='int', default=3,
        label='Pool size alert threshold',
        description='Log a warning when the candidate pool for a request has fewer than N rows. Useful for spotting under-served sites.',
        group='ops', min_value=0, max_value=20,
    ),

    # ───────── Operational ─────────
    SettingSpec(
        key='require_promo_dates_for_tactical', type_='bool', default=True,
        label='Require promo dates for Tactical/Seasonal plans',
        description='Audit promotes to ERROR (red) when Tactical/Seasonal plans have no promo period. Evergreen / stdrate-override are exempt.',
        group='ops',
    ),

    # ───────── Perpetual Discount Orchestration ─────────
    # Defaults consumed by the /api/reservations/move-in handler when a
    # plan doesn't carry its own override. Two master switches gate the
    # SOAP follow-up calls so admins can flip them independently after
    # watching the audit log.
    SettingSpec(
        key='ecri_default_offset_months', type_='int', default=12,
        label='ECRI default offset (months from move-in)',
        description='When no plan-level override is set, schedule the ECRI rate change at move_in_date + this many months. Matches the typical 12-month renewal cadence.',
        group='perpetual', min_value=1, max_value=60,
    ),
    SettingSpec(
        key='ecri_default_pct', type_='float', default=5.0,
        label='ECRI default uplift (%)',
        description='Default % bump applied to the lease\'s effective rate when scheduling the future rate change. Per-plan post_prepay_ecri_pct overrides this.',
        group='perpetual', min_value=0, max_value=50,
    ),
    SettingSpec(
        key='ecri_min_offset_months', type_='int', default=6,
        label='ECRI minimum offset floor (months)',
        description='Hard floor — never schedule a rate change earlier than this many months from move-in, even if a plan\'s prepayment_months is shorter.',
        group='perpetual', min_value=1, max_value=12,
    ),
    SettingSpec(
        key='ecri_auto_schedule_enabled', type_='bool', default=False,
        label='Enable auto-schedule of ECRI at move-in (master switch)',
        description='When ON, every successful /move-in enqueues a ScheduleTenantRateChange_v2 SOAP call to set up the future ECRI. When OFF, the orchestrator only logs intent — manual ops workflow continues. Flip this first, payment automation second.',
        group='perpetual',
    ),
    SettingSpec(
        key='perpetual_auto_payment_enabled', type_='bool', default=False,
        label='Enable auto-prepayment push for perpetual plans (master switch)',
        description='When ON, perpetual+prepay plans push the prepay surplus via PaymentSimpleCash after MoveIn so dPaidThru advances by prepayment_months. When OFF, the bot can still quote the prepay total but the surplus stays as a credit balance until ops applies it manually.',
        group='perpetual',
    ),
    SettingSpec(
        key='move_in_cost_use_soap_fallback', type_='bool', default=True,
        label='/move-in/cost — call SOAP for SOAP-truth confirmation',
        description='When ON, the /move-in/cost endpoint calls SOAP MoveInCostRetrieveWithDiscount_Reservation_v4 as the right-before-charging confirmation, then adds the prepayment add-on from our calculator. When OFF, answers entirely from the calculator (saves one SOAP round-trip per booking; only safe when calculator is fully validated).',
        group='perpetual',
    ),
]

# Lookup helper
_SPEC_BY_KEY: Dict[str, SettingSpec] = {s.key: s for s in _SETTINGS_SPEC}


# ---------------------------------------------------------------------------
# Type coercion
# ---------------------------------------------------------------------------

def _coerce(spec: SettingSpec, raw: Optional[str]) -> Any:
    if raw is None:
        return spec.default
    try:
        if spec.type_ == 'bool':
            return str(raw).strip().lower() in ('true', '1', 'yes', 'on')
        if spec.type_ == 'int':
            v = int(str(raw).strip())
            if spec.min_value is not None and v < spec.min_value:
                v = int(spec.min_value)
            if spec.max_value is not None and v > spec.max_value:
                v = int(spec.max_value)
            return v
        if spec.type_ == 'float':
            v = float(str(raw).strip())
            if spec.min_value is not None and v < spec.min_value:
                v = float(spec.min_value)
            if spec.max_value is not None and v > spec.max_value:
                v = float(spec.max_value)
            return v
        return str(raw).strip()
    except (TypeError, ValueError):
        return spec.default


# ---------------------------------------------------------------------------
# Read API
# ---------------------------------------------------------------------------

def _load_all(db_session) -> Dict[str, Any]:
    """One DB roundtrip to load every setting; returns a typed dict."""
    out: Dict[str, Any] = {s.key: s.default for s in _SETTINGS_SPEC}
    try:
        rows = db_session.execute(text(
            "SELECT key, value FROM mw_recommender_settings"
        )).fetchall()
    except Exception as exc:
        logger.warning("recommender_settings load failed: %s — using defaults", exc)
        return out
    for key, value in rows:
        spec = _SPEC_BY_KEY.get(key)
        if not spec:
            # Unknown key — keep around but don't expose
            continue
        out[key] = _coerce(spec, value)
    return out


def get_all_settings(db_session) -> Dict[str, Any]:
    """Return every setting (typed) with cache."""
    global _cache, _cache_loaded_at
    now = time.monotonic()
    with _cache_lock:
        if _cache and now - _cache_loaded_at < _CACHE_TTL_SECONDS:
            return dict(_cache)
        _cache = _load_all(db_session)
        _cache_loaded_at = now
        return dict(_cache)


def get_setting(key: str, db_session) -> Any:
    """Return a single typed setting (or its spec default)."""
    if key not in _SPEC_BY_KEY:
        raise KeyError(f'unknown recommender setting: {key}')
    return get_all_settings(db_session).get(key, _SPEC_BY_KEY[key].default)


def list_specs() -> list[SettingSpec]:
    """For the admin UI — render a row per setting in defined order."""
    return list(_SETTINGS_SPEC)


def clear_cache() -> None:
    """Force the next read to re-hit DB (used after a save + tests)."""
    global _cache, _cache_loaded_at
    with _cache_lock:
        _cache = {}
        _cache_loaded_at = 0.0


# ---------------------------------------------------------------------------
# Write API (admin only)
# ---------------------------------------------------------------------------

def update_settings(updates: Dict[str, str], updated_by: str, db_session) -> int:
    """
    Upsert each (key, value) pair. Values are stored as TEXT — coercion
    happens on read.
    Returns the number of rows changed (insert + update).
    """
    if not updates:
        return 0
    changed = 0
    for key, value in updates.items():
        if key not in _SPEC_BY_KEY:
            logger.warning("update_settings: ignoring unknown key %r", key)
            continue
        spec = _SPEC_BY_KEY[key]
        # Validate by coercing — silently snaps to bounds for numeric types.
        coerced = _coerce(spec, value)
        if spec.type_ == 'bool':
            stored = '1' if coerced else '0'
        else:
            stored = str(coerced)
        result = db_session.execute(text("""
            INSERT INTO mw_recommender_settings (key, value, updated_at, updated_by)
            VALUES (:key, :value, now(), :updated_by)
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value,
                updated_at = now(),
                updated_by = EXCLUDED.updated_by
            WHERE mw_recommender_settings.value IS DISTINCT FROM EXCLUDED.value
        """), {'key': key, 'value': stored, 'updated_by': updated_by})
        if result.rowcount:
            changed += 1
    db_session.commit()
    clear_cache()
    return changed
