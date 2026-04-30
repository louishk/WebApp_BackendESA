"""
Recommender service — pure Python, no Flask, no current_app.

Consumed by web/routes/recommendations.py. All DB side-effects are confined to
functions that explicitly accept a db_session argument; the slot-builder and
normalise helpers are truly pure and unit-testable without any DB.

Public surface (all importable from here):
  RecommendationRequest, CandidateRow, ValidationError
  normalise_request, relax_strategy, resume_session
  fetch_candidate_pool, build_slot1, build_slot2, build_slot3
  quote_slot, log_served
"""
from __future__ import annotations

import json
import logging
import secrets
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class RecommendationRequest:
    """Normalized, post-validation recommendation request."""
    mode: str                   # default 'recommendation'
    level: str                  # default 'standard'
    filters: Dict[str, List]    # all dim values normalised to lists
    duration_months: int
    constraints: Dict[str, Any] # max_distance_km, include_legacy, max_results, exclude_unit_ids
    context: Dict[str, Any]     # channel, request_id, session_id, customer_id,
                                # previous_request_id, picked_slot, action


@dataclass
class CandidateRow:
    """One row from the per-unit best-plan rollup (cheapest plan × concession)."""
    site_id: int
    site_code: str
    unit_id: int
    plan_id: int
    concession_id: int
    unit_type: Optional[str]
    climate_type: Optional[str]
    size_range: Optional[str]
    std_rate: Decimal
    effective_rate: Optional[Decimal]
    smart_lock: Optional[Dict[str, Any]]
    parse_ok: bool
    legacy_mapped: bool
    plan_name: str
    min_duration_months: Optional[int]
    max_duration_months: Optional[int]
    distribution_channel: Optional[str]
    hidden_rate: Optional[bool]
    # Concession params — needed by quote_slot
    amt_type: Optional[int]
    pct_discount: Optional[Decimal]
    fixed_discount: Optional[Decimal]
    max_amount_off: Optional[Decimal]
    in_month: Optional[int]
    prepay: Optional[bool]
    prepaid_months: Optional[int]
    coupon_code: Optional[str] = None
    std_sec_dep: Optional[Decimal] = None
    # Phase 3.6 — NL fields surfaced on each slot.
    concession_name: Optional[str] = None
    size_sqft: Optional[Decimal] = None
    lock_in_months: Optional[int] = None
    payment_terms: Optional[str] = None
    promo_valid_until: Optional[Any] = None       # date or None
    lock_in_period: Optional[str] = None          # raw string fallback


class ValidationError(ValueError):
    """Input failed validation in normalise_request."""


# ---------------------------------------------------------------------------
# Input normalisation
# ---------------------------------------------------------------------------

def _coerce_list(value: Any) -> List[str]:
    """Coerce a scalar string or list of strings to a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    return [str(value)]


def normalise_request(raw: Dict[str, Any]) -> RecommendationRequest:
    """
    Validate and normalise the raw request dict into a RecommendationRequest.

    Required fields:
      - filters.location (string or list of site codes)
      - duration_months (positive integer)
      - context.request_id (non-empty string)

    Optional but validated:
      - mode (default 'recommendation')
      - level (default 'standard')
      - context.session_id (auto-generated if absent)
      - constraints.* (defaults applied for missing keys)

    All filter values are coerced to lists.
    Raises ValidationError on missing required fields or bad types.
    """
    if not isinstance(raw, dict):
        raise ValidationError("request must be a JSON object")

    # --- mode / level ---
    mode = str(raw.get('mode', 'recommendation') or 'recommendation').strip()
    level = str(raw.get('level', 'standard') or 'standard').strip()

    # --- duration_months ---
    try:
        duration_months = int(raw['duration_months'])
    except KeyError:
        raise ValidationError("duration_months is required")
    except (TypeError, ValueError):
        raise ValidationError("duration_months must be an integer")
    if duration_months < 1:
        raise ValidationError("duration_months must be >= 1")

    # --- filters ---
    raw_filters = raw.get('filters') or {}
    if not isinstance(raw_filters, dict):
        raise ValidationError("filters must be an object")

    location_raw = raw_filters.get('location')
    if not location_raw:
        raise ValidationError("filters.location is required")
    location_list = _coerce_list(location_raw)
    if not location_list:
        raise ValidationError("filters.location must not be empty")

    filters: Dict[str, List] = {'location': location_list}
    for dim in ('unit_type', 'climate_type', 'size_range', 'unit_shape', 'pillar'):
        val = raw_filters.get(dim)
        if val is not None:
            coerced = _coerce_list(val)
            if coerced:
                filters[dim] = coerced

    # Phase 4.B.b — `mode=quote` accepts unit_id (list or scalar) and
    # optionally concession_id to lock a specific (unit, plan, concession).
    unit_id_raw = raw_filters.get('unit_id')
    if unit_id_raw is not None:
        try:
            uid_list = [int(v) for v in _coerce_list(unit_id_raw)]
            if uid_list:
                filters['unit_id'] = uid_list
        except (TypeError, ValueError):
            raise ValidationError("filters.unit_id must be integer(s)")
    concession_id_raw = raw_filters.get('concession_id')
    if concession_id_raw is not None:
        try:
            filters['concession_id'] = [int(concession_id_raw)]
        except (TypeError, ValueError):
            raise ValidationError("filters.concession_id must be an integer")

    # Coupon code — single string, normalised to upper-case for case-
    # insensitive match. Stored on filters so fetch_candidate_pool can
    # parameterise it. None when not provided.
    coupon = raw_filters.get('coupon_code')
    if coupon is not None and str(coupon).strip():
        filters['coupon_code'] = str(coupon).strip().upper()

    # case_count range: pass through as-is if numeric
    if 'case_count_min' in raw_filters:
        try:
            filters['case_count_min'] = [int(raw_filters['case_count_min'])]
        except (TypeError, ValueError):
            pass
    if 'case_count_max' in raw_filters:
        try:
            filters['case_count_max'] = [int(raw_filters['case_count_max'])]
        except (TypeError, ValueError):
            pass

    # --- context ---
    raw_ctx = raw.get('context') or {}
    if not isinstance(raw_ctx, dict):
        raise ValidationError("context must be an object")

    request_id = str(raw_ctx.get('request_id', '') or '').strip()
    if not request_id:
        raise ValidationError("context.request_id is required")

    session_id = str(raw_ctx.get('session_id', '') or '').strip()
    if not session_id:
        session_id = str(uuid.uuid4())

    context: Dict[str, Any] = {
        'channel': str(raw_ctx.get('channel', 'api') or 'api').strip(),
        'request_id': request_id,
        'session_id': session_id,
        'customer_id': raw_ctx.get('customer_id') or None,
        'previous_request_id': raw_ctx.get('previous_request_id') or None,
        'picked_slot': raw_ctx.get('picked_slot'),   # int or None
        'action': raw_ctx.get('action') or None,
    }

    # Validate picked_slot if present
    if context['picked_slot'] is not None:
        try:
            context['picked_slot'] = int(context['picked_slot'])
        except (TypeError, ValueError):
            raise ValidationError("context.picked_slot must be an integer (1, 2, or 3)")

    # --- constraints ---
    raw_constraints = raw.get('constraints') or {}
    if not isinstance(raw_constraints, dict):
        raise ValidationError("constraints must be an object")

    exclude_unit_ids = list(raw_constraints.get('exclude_unit_ids') or [])
    try:
        exclude_unit_ids = [int(x) for x in exclude_unit_ids]
    except (TypeError, ValueError):
        raise ValidationError("constraints.exclude_unit_ids must be a list of integers")

    constraints: Dict[str, Any] = {
        'max_distance_km': float(raw_constraints.get('max_distance_km', 50) or 50),
        'include_legacy': bool(raw_constraints.get('include_legacy', True)),
        'max_results': int(raw_constraints.get('max_results', 3) or 3),
        'exclude_unit_ids': exclude_unit_ids,
    }

    return RecommendationRequest(
        mode=mode,
        level=level,
        filters=filters,
        duration_months=duration_months,
        constraints=constraints,
        context=context,
    )


# ---------------------------------------------------------------------------
# Relax strategy
# ---------------------------------------------------------------------------

def relax_strategy(picked_slot: Optional[int], action: Optional[str]) -> str:
    """
    Map (picked_slot, action) to a strategy id.

    Strategy ids:
      'none'              — no relaxation (default)
      'size_plus_one'     — expand size by ±1 bucket (slot 1 + more_like_this)
      'next_nearest_site' — try next nearest site (slot 2 + more_like_this)
      'wider_size_band'   — ±2 buckets (any slot + expand_size)
      'expand_unit_type'  — remove unit_type filter (any slot + different_type)
      'different_options' — start fresh with same dims (different_options action)
    """
    if not picked_slot and not action:
        return 'none'

    action = (action or '').strip().lower()

    if action == 'different_options':
        return 'different_options'

    if action == 'more_like_this':
        if picked_slot == 1:
            return 'size_plus_one'
        if picked_slot == 2:
            return 'next_nearest_site'
        if picked_slot == 3:
            return 'size_plus_one'
        return 'size_plus_one'

    if action == 'expand_size':
        return 'wider_size_band'

    if action == 'different_type':
        return 'expand_unit_type'

    # picked_slot without a recognised action — treat as "show me more like that one"
    if picked_slot is not None:
        return 'size_plus_one'

    return 'none'


# ---------------------------------------------------------------------------
# Session resume
# ---------------------------------------------------------------------------

def resume_session(req: RecommendationRequest, db_session) -> RecommendationRequest:
    """
    If req.context['previous_request_id'] is set, fetch the prior served row,
    merge prior filters as the base (current filters override per-key), collect
    all prior slot unit_ids in this session into constraints.exclude_unit_ids,
    and apply the relax strategy based on (picked_slot, action).

    Mutates and returns req. No-op when previous_request_id is None.
    """
    prev_id = req.context.get('previous_request_id')
    if not prev_id:
        return req

    try:
        row = db_session.execute(text("""
            SELECT filters_applied,
                   slot1_unit_id, slot2_unit_id, slot3_unit_id,
                   session_id
            FROM mw_recommendations_served
            WHERE request_id = :rid
            LIMIT 1
        """), {'rid': prev_id}).mappings().first()
    except Exception as exc:
        logger.warning("resume_session: failed to fetch prior row %s: %s", prev_id, exc)
        return req

    if not row:
        logger.warning("resume_session: previous_request_id %s not found", prev_id)
        return req

    # Merge prior filters: prior is base, current overrides per-key.
    prior_filters = row['filters_applied'] or {}
    if isinstance(prior_filters, str):
        try:
            prior_filters = json.loads(prior_filters)
        except (ValueError, TypeError):
            prior_filters = {}

    merged_filters = dict(prior_filters)
    merged_filters.update(req.filters)
    req.filters = merged_filters

    # Collect all slot unit_ids served in this session so we can exclude them.
    try:
        session_rows = db_session.execute(text("""
            SELECT slot1_unit_id, slot2_unit_id, slot3_unit_id
            FROM mw_recommendations_served
            WHERE session_id = :sid
        """), {'sid': req.context['session_id']}).fetchall()
    except Exception as exc:
        logger.warning("resume_session: failed to fetch session rows: %s", exc)
        session_rows = []

    served_ids: set[int] = set(req.constraints.get('exclude_unit_ids') or [])
    for sr in session_rows:
        for uid in sr:
            if uid is not None:
                served_ids.add(int(uid))

    req.constraints['exclude_unit_ids'] = list(served_ids)

    # Apply relax strategy.
    strategy = relax_strategy(
        req.context.get('picked_slot'),
        req.context.get('action'),
    )
    req.context['_relax_strategy'] = strategy

    # Adjust filters based on strategy.
    if strategy == 'size_plus_one':
        _apply_size_plus_one(req)
    elif strategy == 'wider_size_band':
        _apply_wider_size_band(req)
    elif strategy == 'expand_unit_type':
        req.filters.pop('unit_type', None)
    # 'next_nearest_site', 'different_options', 'none' — no filter change here;
    # slot builders handle the site-hop for next_nearest_site.

    return req


def _apply_size_plus_one(req: RecommendationRequest) -> None:
    """Expand size_range filter by ±1 step using the dim table helper."""
    current_sizes = req.filters.get('size_range', [])
    if not current_sizes:
        return
    try:
        from common.size_range_window import size_range_neighbours_step
        expanded: List[str] = []
        seen: set[str] = set()
        for s in current_sizes:
            for nb in size_range_neighbours_step(s, n_steps=1):
                if nb not in seen:
                    expanded.append(nb)
                    seen.add(nb)
        if expanded:
            req.filters['size_range'] = expanded
    except Exception as exc:
        logger.warning("_apply_size_plus_one failed: %s", exc)


def _apply_wider_size_band(req: RecommendationRequest) -> None:
    """Expand size_range filter by ±2 steps using the dim table helper."""
    current_sizes = req.filters.get('size_range', [])
    if not current_sizes:
        return
    try:
        from common.size_range_window import size_range_neighbours_step
        expanded: List[str] = []
        seen: set[str] = set()
        for s in current_sizes:
            for nb in size_range_neighbours_step(s, n_steps=2):
                if nb not in seen:
                    expanded.append(nb)
                    seen.add(nb)
        if expanded:
            req.filters['size_range'] = expanded
    except Exception as exc:
        logger.warning("_apply_wider_size_band failed: %s", exc)


# ---------------------------------------------------------------------------
# Candidate pool fetch
# ---------------------------------------------------------------------------

def _build_candidate_row(r: Any) -> CandidateRow:
    """Build a CandidateRow from a SQLAlchemy mapping row."""
    smart_lock = r['smart_lock']
    if isinstance(smart_lock, str):
        try:
            smart_lock = json.loads(smart_lock)
        except (ValueError, TypeError):
            smart_lock = None

    def _dec(v: Any) -> Optional[Decimal]:
        if v is None:
            return None
        return Decimal(str(v))

    return CandidateRow(
        site_id=int(r['site_id']),
        site_code=str(r['site_code'] or ''),
        unit_id=int(r['unit_id']),
        plan_id=int(r['plan_id']),
        concession_id=int(r['concession_id']),
        unit_type=r['unit_type'] or None,
        climate_type=r['climate_type'] or None,
        size_range=r['size_range'] or None,
        std_rate=Decimal(str(r['std_rate'])) if r['std_rate'] is not None else Decimal('0'),
        std_sec_dep=_dec(r.get('std_sec_dep')),
        effective_rate=_dec(r['effective_rate']),
        smart_lock=smart_lock,
        parse_ok=bool(r['parse_ok']),
        legacy_mapped=bool(r['legacy_mapped']) if r.get('legacy_mapped') is not None else False,
        plan_name=str(r['plan_name'] or ''),
        min_duration_months=int(r['min_duration_months']) if r['min_duration_months'] is not None else None,
        max_duration_months=int(r['max_duration_months']) if r['max_duration_months'] is not None else None,
        distribution_channel=r['distribution_channel'] or None,
        hidden_rate=bool(r['hidden_rate']) if r.get('hidden_rate') is not None else None,
        coupon_code=r.get('coupon_code') or None,
        amt_type=int(r['amt_type']) if r.get('amt_type') is not None else None,
        pct_discount=_dec(r.get('pct_discount')),
        fixed_discount=_dec(r.get('fixed_discount')),
        max_amount_off=_dec(r.get('max_amount_off')),
        in_month=int(r['in_month']) if r.get('in_month') is not None else None,
        prepay=bool(r['prepay']) if r.get('prepay') is not None else None,
        prepaid_months=int(r['prepaid_months']) if r.get('prepaid_months') is not None else None,
        # Phase 3.6 — NL fields.
        concession_name=r.get('concession_name') or None,
        size_sqft=_dec(r.get('size_sqft')),
        lock_in_months=int(r['lock_in_months']) if r.get('lock_in_months') is not None else None,
        payment_terms=r.get('payment_terms') or None,
        promo_valid_until=r.get('promo_valid_until'),
        lock_in_period=r.get('lock_in_period') or None,
    )


def fetch_candidate_pool(req: RecommendationRequest, db_session) -> List[CandidateRow]:
    """
    Query mw_unit_discount_candidates for matching units.

    Uses DISTINCT ON (unit_id) with ORDER BY unit_id, effective_rate ASC NULLS LAST
    so each unit is represented by its cheapest qualifying plan × concession.

    All filters applied at SQL level. Returns empty list (never None) when
    nothing matches.
    """
    locations = req.filters.get('location', [])
    if not locations:
        return []

    exclude_ids: List[int] = req.constraints.get('exclude_unit_ids') or []
    include_legacy: bool = req.constraints.get('include_legacy', True)
    duration = req.duration_months
    channel = (req.context.get('channel') or '').strip()
    coupon = (req.filters.get('coupon_code') or '').strip().upper() or None

    # Build parameterised WHERE clauses for optional dim filters.
    where_parts = [
        "site_code = ANY(:locations)",
        "((min_duration_months IS NULL OR min_duration_months <= :dur) "
        " AND (max_duration_months IS NULL OR max_duration_months >= :dur))",
        # Hidden-rate gate: hidden plans require a matching coupon. Public
        # plans (hidden_rate IS NULL or FALSE) ignore the coupon entirely.
        # The recommender pulls hidden plans into the pool only when the
        # bot/web caller explicitly provides the unlock code.
        "(hidden_rate IS NOT TRUE "
        " OR (UPPER(coupon_code) = :coupon AND :coupon IS NOT NULL))",
        # Distribution channel gate: when the plan limits its distribution,
        # the calling channel must be in that list. Empty/null = open to all.
        # Comparison is case-insensitive — plans store 'Chatbot' (TitleCase) but
        # callers may pass 'chatbot' (lowercase). LOWER() on both sides normalises.
        # REPLACE strips spaces so 'Direct Mailing, Online' splits cleanly.
        "(distribution_channel IS NULL "
        " OR distribution_channel = '' "
        " OR :channel = '' "
        " OR LOWER(:channel) = ANY(string_to_array(LOWER(REPLACE(distribution_channel, ' ', '')), ',')))",
    ]
    params: Dict[str, Any] = {
        'locations': locations,
        'dur': duration,
        'channel': channel,
        'coupon': coupon,
    }

    # Each optional dim filter
    for dim, param_name in [
        ('unit_type', 'types'),
        ('climate_type', 'climates'),
        ('size_range', 'sizes'),
    ]:
        vals = req.filters.get(dim)
        if vals:
            where_parts.append(f"(unit_type IS NULL OR {dim} = ANY(:{param_name}))" if dim == 'unit_type'
                               else f"{dim} = ANY(:{param_name})")
            params[param_name] = vals

    # unit_type: only filter when explicitly requested
    if 'unit_type' in req.filters and req.filters['unit_type']:
        where_parts = [p for p in where_parts if 'unit_type IS NULL OR unit_type' not in p]
        where_parts.append("unit_type = ANY(:types)")
        params['types'] = req.filters['unit_type']

    # climate_type
    if 'climate_type' in req.filters and req.filters['climate_type']:
        if 'climates' not in params:
            where_parts.append("climate_type = ANY(:climates)")
            params['climates'] = req.filters['climate_type']

    # size_range
    if 'size_range' in req.filters and req.filters['size_range']:
        if 'sizes' not in params:
            where_parts.append("size_range = ANY(:sizes)")
            params['sizes'] = req.filters['size_range']

    # Phase 4.B.b — quote-mode filters (specific unit / concession).
    if 'unit_id' in req.filters and req.filters['unit_id']:
        where_parts.append("unit_id = ANY(:unit_ids)")
        params['unit_ids'] = req.filters['unit_id']
    if 'concession_id' in req.filters and req.filters['concession_id']:
        where_parts.append("concession_id = :one_concession")
        params['one_concession'] = int(req.filters['concession_id'][0])

    # Exclude already-served units
    if exclude_ids:
        where_parts.append("unit_id <> ALL(:exclude)")
        params['exclude'] = exclude_ids
    else:
        # Still need the param for the query template — use empty array
        params['exclude'] = []

    # Legacy inclusion
    if not include_legacy:
        where_parts.append("parse_ok = TRUE")

    where_sql = " AND ".join(where_parts)

    sql = text(f"""
        SELECT DISTINCT ON (unit_id)
            site_id, site_code, unit_id, plan_id, concession_id,
            unit_type, climate_type, size_range,
            std_rate, std_sec_dep, effective_rate, smart_lock,
            parse_ok,
            FALSE AS legacy_mapped,
            plan_name, min_duration_months, max_duration_months,
            distribution_channel, hidden_rate, coupon_code,
            amt_type, pct_discount, fixed_discount, max_amount_off,
            in_month, prepay, prepaid_months,
            concession_name, size_sqft, lock_in_months,
            payment_terms, lock_in_period, promo_valid_until
        FROM mw_unit_discount_candidates
        WHERE {where_sql}
        ORDER BY unit_id, effective_rate ASC NULLS LAST
    """)

    try:
        rows = db_session.execute(sql, params).mappings().all()
    except Exception as exc:
        logger.error("fetch_candidate_pool query failed: %s", exc)
        return []

    result: List[CandidateRow] = []
    for r in rows:
        try:
            result.append(_build_candidate_row(r))
        except Exception as exc:
            logger.warning("fetch_candidate_pool: skipping malformed row unit_id=%s: %s",
                           r.get('unit_id'), exc)
    return result


# ---------------------------------------------------------------------------
# Slot builders
# ---------------------------------------------------------------------------

def build_slot1(pool: List[CandidateRow], req: RecommendationRequest) -> Optional[CandidateRow]:
    """
    Slot 1 — cheapest unit that exactly matches the requested location(s) and
    all other dimension filters present in req.filters.

    Pool was already filtered at SQL level so we just pick the cheapest
    site_code-matching row.
    """
    if not pool:
        return None

    locations = set(req.filters.get('location', []))

    candidates = [r for r in pool if r.site_code in locations]
    if not candidates:
        return None

    # Pool is ordered by effective_rate ASC NULLS LAST from SQL — pick first.
    best = min(
        candidates,
        key=lambda r: (r.effective_rate is None, r.effective_rate or Decimal('0'))
    )
    return best


def build_slot2(
    pool: List[CandidateRow],
    req: RecommendationRequest,
    db_session,
) -> Optional[CandidateRow]:
    """
    Slot 2 — same dim filters as slot 1 but at a DIFFERENT site, picked by
    proximity from mw_site_distance.

    The base `pool` is restricted to req.filters.location, so we run a
    second pool fetch with location swapped to the nearest site(s) within
    max_distance_km. First neighbouring site that produces any candidate
    matching the rest of the filters wins.

    Returns None when nothing qualifies within the radius.
    """
    locations = req.filters.get('location', [])
    if not locations:
        return None

    # max_distance_km can be overridden per-request (constraint); otherwise
    # falls back to the global admin setting, otherwise 50 km.
    try:
        from web.services import recommender_settings
        global_default = recommender_settings.get_setting('slot2_max_distance_km', db_session)
    except Exception:
        global_default = 50
    max_dist = req.constraints.get('max_distance_km') or global_default

    # Step 1: order neighbouring sites by distance.
    # Distance is symmetric, so we union (from→to) and (to→from) — many
    # admin tables only seed one direction. Preserve the smaller distance
    # when both directions exist.
    try:
        dist_rows = db_session.execute(text("""
            SELECT neighbour, MIN(distance_km) AS d FROM (
                SELECT to_site_code   AS neighbour, distance_km
                FROM mw_site_distance
                WHERE from_site_code = ANY(:locs)
                UNION ALL
                SELECT from_site_code AS neighbour, distance_km
                FROM mw_site_distance
                WHERE to_site_code   = ANY(:locs)
            ) bidir
            WHERE neighbour <> ALL(:locs)
              AND distance_km <= :max_km
            GROUP BY neighbour
            ORDER BY d ASC
        """), {'locs': locations, 'max_km': max_dist}).fetchall()
    except Exception as exc:
        logger.warning("build_slot2: mw_site_distance query failed: %s", exc)
        return None

    if not dist_rows:
        return None

    # Step 2: re-run the pool query but at each neighbour in turn, taking
    # the first one that has matching inventory. Same filter set as the
    # original request — just swap the location.
    from copy import copy
    for neighbour, _dist in dist_rows:
        neighbour_req = copy(req)
        neighbour_req.filters = dict(req.filters, location=[neighbour])
        try:
            neighbour_pool = fetch_candidate_pool(neighbour_req, db_session)
        except Exception as exc:
            logger.warning("build_slot2: neighbour %s pool failed: %s", neighbour, exc)
            continue
        if neighbour_pool:
            best = min(
                neighbour_pool,
                key=lambda r: (r.effective_rate is None, r.effective_rate or Decimal('0'))
            )
            return best

    return None


def build_slot3(
    pool: List[CandidateRow],
    req: RecommendationRequest,
    slot1: Optional[CandidateRow],
    db_session,
) -> Optional[CandidateRow]:
    """
    Slot 3 — "Best Price" at the SAME site as the primary location.

    Hunts progressively wider until it finds a unit strictly cheaper than
    slot 1 (by `slot3_min_savings_pct`). Each step relaxes one more
    dimension so the bot can surface "if you're flexible on X you save Y":

        Step 1: drop unit_type (size_range still ±N% buckets, climate kept)
        Step 2: drop climate_type as well
        Step 3: drop size_range as well — any unit at the site

    The first step that yields a strictly-cheaper candidate wins. This
    keeps the suggestion as "close" to the user's intent as possible — we
    don't widen further than we need to.

    Returns None when slot1 is null, when no qualifying cheaper unit
    exists at the site, or when the cheapest is the slot1 unit itself.
    """
    if slot1 is None:
        return None

    locations = set(req.filters.get('location', []))
    if not locations:
        return None
    requested_sizes = req.filters.get('size_range', [])

    # Admin-tunable settings.
    try:
        from web.services import recommender_settings
        size_band_pct = int(recommender_settings.get_setting('slot3_size_band_pct', db_session))
        min_savings_pct = int(recommender_settings.get_setting('slot3_min_savings_pct', db_session))
    except Exception:
        size_band_pct = 20
        min_savings_pct = 0

    # ±N% size neighbour buckets (only used in steps 1 + 2).
    neighbour_sizes: List[str] = []
    if requested_sizes:
        try:
            from common.size_range_window import size_range_neighbours
            seen: set[str] = set()
            for s in requested_sizes:
                for nb in size_range_neighbours(s, radius_pct=size_band_pct):
                    if nb not in seen:
                        neighbour_sizes.append(nb)
                        seen.add(nb)
        except Exception as exc:
            logger.warning("build_slot3: size_range_neighbours failed: %s", exc)
            neighbour_sizes = list(requested_sizes)

    slot1_rate = slot1.effective_rate if slot1.effective_rate is not None else slot1.std_rate
    if slot1_rate is None:
        return None
    threshold = Decimal(slot1_rate) * (Decimal('1') - Decimal(min_savings_pct) / Decimal('100'))

    # Build the progressively-relaxed filter sets.
    base_filters = dict(req.filters)
    base_filters.pop('unit_type', None)  # always drop unit_type for slot 3

    # (filters, relaxed_dims_label) tuples. relaxed_dims is recorded on the
    # returned row so the route can put it in match_flags for the bot.
    relax_steps: List[Tuple[Dict[str, Any], List[str]]] = []

    step1 = dict(base_filters)
    if neighbour_sizes:
        step1['size_range'] = neighbour_sizes
    relax_steps.append((step1, ['unit_type']))

    step2 = dict(step1)
    step2.pop('climate_type', None)
    if step2 != step1:
        relax_steps.append((step2, ['unit_type', 'climate_type']))

    step3 = dict(step2)
    step3.pop('size_range', None)
    if step3 != step2:
        relax_steps.append((step3, ['unit_type', 'climate_type', 'size_range']))

    from copy import copy
    for filters, relaxed_dims in relax_steps:
        relaxed_req = copy(req)
        relaxed_req.filters = filters
        try:
            relaxed_pool = fetch_candidate_pool(relaxed_req, db_session)
        except Exception as exc:
            logger.warning("build_slot3: relaxed pool fetch failed: %s", exc)
            continue

        candidates = [
            r for r in relaxed_pool
            if r.unit_id != slot1.unit_id and r.site_code in locations
        ]
        if not candidates:
            continue

        best = min(
            candidates,
            key=lambda r: (r.effective_rate is None, r.effective_rate or Decimal('0'))
        )
        best_rate = best.effective_rate if best.effective_rate is not None else best.std_rate
        if best_rate is None:
            continue
        if Decimal(best_rate) < threshold:
            # Stash the relax label on the row so the route can render it.
            # CandidateRow is a frozen-ish dataclass; we tag via a dict-like
            # attribute the route knows to look for.
            try:
                best._slot3_relaxed_dims = relaxed_dims  # type: ignore[attr-defined]
            except Exception:
                pass
            return best

    return None


# ---------------------------------------------------------------------------
# Pricing quote
# ---------------------------------------------------------------------------

def quote_slot(
    row: CandidateRow,
    req: RecommendationRequest,
    db_session,
    move_in_date: Optional[date] = None,
) -> 'DurationQuote':
    """
    Look up site billing config, tax rates, admin fee, deposit, and insurance
    premium from the middleware DB. Call calculate_duration_breakdown() with the
    candidate's pricing parameters and return the DurationQuote.

    For stdrate-override rows (concession_id=0): pc_discount=0, fixed_discount=0.
    move_in_date defaults to today when not provided.
    """
    from common.movein_cost_calculator import (
        ChargeTypeTax, calculate_duration_breakdown, DurationQuote,
    )

    if move_in_date is None:
        move_in_date = date.today()

    # ---- Billing config ----
    billing_config = _load_billing_config(row.site_code, db_session)
    anniversary_billing: bool = billing_config.get('anniversary_billing', False)
    day_start_prorate_plus_next: int = billing_config.get('day_start_prorate_plus_next', 17)

    # ---- Charge descriptions (admin fee, deposit, tax rates) ----
    charge_info = _load_charge_descriptions(row.site_id, db_session)
    admin_fee: Decimal = charge_info.get('admin_fee', Decimal('0'))
    security_deposit: Decimal = charge_info.get('security_deposit', Decimal('0'))
    # SiteLink convention: when SecDep.dcPrice is 0, the deposit is the
    # unit's per-row dcStdSecDep (or its dcStdRate if dcStdSecDep is also 0).
    # Some sites have units with deposit != std_rate (premium 2x, no-deposit 0).
    # Falling back this way matches what SOAP MoveInCostRetrieve would charge.
    if security_deposit <= 0:
        if row.std_sec_dep and row.std_sec_dep > 0:
            security_deposit = Decimal(str(row.std_sec_dep))
        elif row.std_rate is not None:
            security_deposit = Decimal(str(row.std_rate))
    rent_tax = charge_info.get('rent_tax', ChargeTypeTax('Rent'))
    admin_tax = charge_info.get('admin_tax', rent_tax)
    deposit_tax = charge_info.get('deposit_tax', ChargeTypeTax('SecDep'))
    insurance_tax = charge_info.get('insurance_tax', rent_tax)

    # ---- Insurance premium ----
    insurance_premium: Decimal = _load_insurance_premium(row.site_id, db_session)

    # ---- Concession params ----
    if row.concession_id == 0:
        # stdrate-override path: no concession
        pc_discount = 0
        fixed_discount = 0
        max_amount_off = None
        concession_in_month = 1
        concession_prepay_months = 0
    else:
        pc_discount = float(row.pct_discount or 0)
        fixed_discount = float(row.fixed_discount or 0)
        max_amount_off = row.max_amount_off
        concession_in_month = int(row.in_month or 1)
        concession_prepay_months = int(row.prepaid_months or 0)

    try:
        quote = calculate_duration_breakdown(
            std_rate=row.std_rate,
            security_deposit=security_deposit,
            admin_fee=admin_fee,
            move_in_date=move_in_date,
            rent_tax=rent_tax,
            admin_tax=admin_tax,
            deposit_tax=deposit_tax,
            insurance_tax=insurance_tax,
            pc_discount=pc_discount,
            fixed_discount=fixed_discount,
            insurance_premium=insurance_premium,
            anniversary_billing=anniversary_billing,
            day_start_prorate_plus_next=day_start_prorate_plus_next,
            duration_months=req.duration_months,
            concession_in_month=concession_in_month,
            concession_prepay_months=concession_prepay_months,
            max_amount_off=max_amount_off,
            unit_id=row.unit_id,
            plan_id=row.plan_id,
            concession_id=row.concession_id,
        )
    except Exception as exc:
        logger.error(
            "quote_slot failed for unit_id=%s plan_id=%s: %s",
            row.unit_id, row.plan_id, exc,
        )
        raise

    return quote


def _load_billing_config(site_code: str, db_session) -> Dict[str, Any]:
    """Load billing config from ccws_site_billing_config via an existing session."""
    try:
        row = db_session.execute(text("""
            SELECT b_anniv_date_leasing,
                   i_day_strt_prorating,
                   i_day_strt_prorate_plus_next
            FROM ccws_site_billing_config
            WHERE "SiteCode" = :site
        """), {'site': site_code}).first()
        if not row:
            return {'anniversary_billing': False, 'day_start_prorating': 1,
                    'day_start_prorate_plus_next': 17}
        return {
            'anniversary_billing': bool(row[0]),
            'day_start_prorating': int(row[1]),
            'day_start_prorate_plus_next': int(row[2]),
        }
    except Exception as exc:
        logger.warning("_load_billing_config failed for %s: %s", site_code, exc)
        try:
            db_session.rollback()
        except Exception:
            pass
        return {'anniversary_billing': False, 'day_start_prorating': 1,
                'day_start_prorate_plus_next': 17}


def _load_charge_descriptions(site_id: int, db_session) -> Dict[str, Any]:
    """
    Load admin fee, security deposit, and tax rates from ccws_charge_descriptions.

    Returns a dict with 'admin_fee', 'security_deposit', 'rent_tax',
    'admin_tax', 'deposit_tax', 'insurance_tax' entries. Falls back to safe
    defaults (zero tax, zero fees) on any error.
    """
    from common.movein_cost_calculator import ChargeTypeTax

    defaults: Dict[str, Any] = {
        'admin_fee': Decimal('0'),
        'security_deposit': Decimal('0'),
        'rent_tax': ChargeTypeTax('Rent'),
        'admin_tax': ChargeTypeTax('AdminFee'),
        'deposit_tax': ChargeTypeTax('SecDep'),
        'insurance_tax': ChargeTypeTax('Insurance'),
    }

    try:
        rows = db_session.execute(text("""
            SELECT "sChgCategory", "dcPrice", "dcTax1Rate", "dcTax2Rate"
            FROM ccws_charge_descriptions
            WHERE "SiteID" = :sid
        """), {'sid': site_id}).fetchall()
    except Exception as exc:
        logger.warning("_load_charge_descriptions failed for site_id=%s: %s", site_id, exc)
        try:
            db_session.rollback()
        except Exception:
            pass
        return defaults

    result: Dict[str, Any] = dict(defaults)
    for cat, price, tax1, tax2 in rows:
        if not cat:
            continue
        cat = str(cat).strip()
        t1 = Decimal(str(tax1)) if tax1 is not None else Decimal('0')
        t2 = Decimal(str(tax2)) if tax2 is not None else Decimal('0')
        price_d = Decimal(str(price)) if price is not None else Decimal('0')

        if cat == 'Rent':
            result['rent_tax'] = ChargeTypeTax('Rent', tax1_rate=t1, tax2_rate=t2)
        elif cat == 'AdminFee':
            result['admin_fee'] = price_d
            result['admin_tax'] = ChargeTypeTax('AdminFee', tax1_rate=t1, tax2_rate=t2)
        elif cat == 'SecDep':
            result['security_deposit'] = price_d
            result['deposit_tax'] = ChargeTypeTax('SecDep', tax1_rate=t1, tax2_rate=t2)
        elif cat == 'Insurance':
            result['insurance_tax'] = ChargeTypeTax('Insurance', tax1_rate=t1, tax2_rate=t2)

    return result


def render_discount_summary(
    amt_type: Optional[int],
    pct_discount: Optional[Decimal],
    fixed_discount: Optional[Decimal],
    in_month: Optional[int],
    max_amount_off: Optional[Decimal],
    prepay: Optional[bool],
    prepaid_months: Optional[int],
) -> Optional[str]:
    """One-line, customer-facing summary of a concession.

    Returns None when there's effectively no discount (so the bot can
    suppress a "save 0%" line). Examples:
        "5% off first month"
        "10% off first 3 months (max $150)"
        "$50 off first month"
        "Free month — pay 11, get 12"
    `iAmtType` semantics from SiteLink:
        1 = percentage discount (use pct_discount)
        2 = fixed-dollar discount (use fixed_discount)
        3 = prepay/free-month style (uses prepaid_months)
    The recommender treats unknown amt_types best-effort by inspecting
    whichever value is non-zero.
    """
    has_pct = pct_discount is not None and Decimal(pct_discount) > 0
    has_fixed = fixed_discount is not None and Decimal(fixed_discount) > 0
    # Prepay-style is only the dominant rendering when there is NO pct/fixed
    # discount to describe. SiteLink's bPrepay flag is set on many regular
    # percentage promos (it just means "the discount is consumed in month 1
    # whether you prepay or not"), so it cannot stand alone as the trigger.
    pure_prepay = (
        not has_pct and not has_fixed
        and (amt_type == 3 or (prepaid_months and prepaid_months > 0))
    )

    months = int(in_month) if in_month and in_month > 0 else 1
    if months == 1:
        scope = 'first month'
    else:
        scope = f'first {months} months'

    if pure_prepay and prepaid_months and prepaid_months > 0:
        # "Pay 11, get 12" — total months = prepaid_months + freebie inferred from in_month
        free = max(int(in_month or 1), 1)
        paid = int(prepaid_months)
        total = paid + free
        return f"Pay {paid}, get {total} — free month"

    if has_pct:
        pct_int = int(Decimal(pct_discount))
        pct_str = f"{pct_int}%" if Decimal(pct_discount) == pct_int else f"{pct_discount}%"
        cap = ''
        if max_amount_off is not None and Decimal(max_amount_off) > 0:
            cap = f" (max ${int(Decimal(max_amount_off))})"
        return f"{pct_str} off {scope}{cap}"

    if has_fixed:
        return f"${int(Decimal(fixed_discount))} off {scope}"

    return None


def _load_insurance_options(site_id: int, db_session) -> List[Dict[str, Any]]:
    """All insurance coverage rows available at the site, cheapest first.

    Bot uses this to render a coverage picker so the customer can opt
    up/down before move-in. Each entry: id, coverage_amount, premium.
    Returns [] on error.
    """
    try:
        rows = db_session.execute(text("""
            SELECT "InsurCoverageID", "dcCoverage", "dcPremium", "sCoverageDesc"
            FROM ccws_insurance_coverage
            WHERE "SiteID" = :sid
            ORDER BY "dcPremium" ASC NULLS LAST
        """), {'sid': site_id}).fetchall()
        out = []
        for cov_id, cov_amt, prem, desc in rows:
            out.append({
                'id': int(cov_id) if cov_id is not None else None,
                'coverage_amount': float(cov_amt) if cov_amt is not None else None,
                'premium': float(prem) if prem is not None else None,
                'description': desc or None,
            })
        return out
    except Exception as exc:
        logger.warning("_load_insurance_options failed for site_id=%s: %s", site_id, exc)
        try:
            db_session.rollback()
        except Exception:
            pass
        return []


def _load_insurance_minimum(
    site_id: int,
    unit_type: Optional[str],
    db_session,
) -> Optional[float]:
    """Minimum required coverage_amount for (site, unit_type).

    Insurance minimums are SOAP-only (`InsuranceCoverageMinimumsRetrieve`)
    and not yet mirrored in the middleware DB, so on the recommend hot
    path we return None for v1. Bot can still call
    `GET /api/reservations/insurance-minimums?site_code=...` if it needs
    the per-unit-type minimum before move-in. Phase 4.B.d will sync this
    table so we can answer it locally without the SOAP hop.
    """
    return None


def _load_insurance_premium(site_id: int, db_session) -> Decimal:
    """
    Load the lowest available insurance premium from ccws_insurance_coverage
    for the site. Returns Decimal('0') when none found or on error.
    """
    try:
        row = db_session.execute(text("""
            SELECT MIN("dcPremium")
            FROM ccws_insurance_coverage
            WHERE "SiteID" = :sid
        """), {'sid': site_id}).scalar()
        if row is None:
            return Decimal('0')
        return Decimal(str(row))
    except Exception as exc:
        logger.warning("_load_insurance_premium failed for site_id=%s: %s", site_id, exc)
        try:
            db_session.rollback()
        except Exception:
            pass
        return Decimal('0')


# ---------------------------------------------------------------------------
# Log served
# ---------------------------------------------------------------------------

def log_served(
    req: RecommendationRequest,
    slots_with_quotes: List[Optional[Tuple[CandidateRow, 'DurationQuote']]],
    pool_size: int,
    total_matches: int,
    relax_strategy_used: str,
    response: Dict[str, Any],
    db_session,
) -> int:
    """
    INSERT a row into mw_recommendations_served and return the new id
    (= tracking_id). Caller is responsible for committing the session.

    slots_with_quotes is a list of up to 3 Optional[(CandidateRow, DurationQuote)].
    None entries are written as NULL columns.
    """
    def _slot_vals(idx: int) -> Dict[str, Any]:
        """Extract slot{idx} columns from slots_with_quotes (0-indexed)."""
        prefix = f'slot{idx + 1}'
        if idx >= len(slots_with_quotes) or slots_with_quotes[idx] is None:
            return {
                f'{prefix}_unit_id': None,
                f'{prefix}_plan_id': None,
                f'{prefix}_concession_id': None,
                f'{prefix}_first_month': None,
                f'{prefix}_total_contract': None,
            }
        row, quote = slots_with_quotes[idx]
        return {
            f'{prefix}_unit_id': row.unit_id,
            f'{prefix}_plan_id': row.plan_id,
            f'{prefix}_concession_id': row.concession_id,
            f'{prefix}_first_month': float(quote.first_month_total),
            f'{prefix}_total_contract': float(quote.total_contract),
        }

    ctx = req.context
    params: Dict[str, Any] = {
        'request_id': ctx['request_id'],
        'session_id': ctx['session_id'],
        'customer_id': ctx.get('customer_id'),
        'channel': ctx.get('channel', 'api'),
        'mode': req.mode,
        'level': req.level,
        'previous_request_id': ctx.get('previous_request_id'),
        'picked_slot': ctx.get('picked_slot'),
        'action': ctx.get('action'),
        'request_payload': json.dumps(_request_to_dict(req)),
        'filters_applied': json.dumps(req.filters),
        'relax_strategy': relax_strategy_used,
        'candidates_pool_size': pool_size,
        'total_matches': total_matches,
        'full_response': json.dumps(response),
    }
    params.update(_slot_vals(0))
    params.update(_slot_vals(1))
    params.update(_slot_vals(2))

    result = db_session.execute(text("""
        INSERT INTO mw_recommendations_served (
            request_id, session_id, customer_id, channel, mode, level,
            previous_request_id, picked_slot, action,
            request_payload, filters_applied, relax_strategy,
            candidates_pool_size, total_matches,
            slot1_unit_id, slot1_plan_id, slot1_concession_id,
            slot1_first_month, slot1_total_contract,
            slot2_unit_id, slot2_plan_id, slot2_concession_id,
            slot2_first_month, slot2_total_contract,
            slot3_unit_id, slot3_plan_id, slot3_concession_id,
            slot3_first_month, slot3_total_contract,
            full_response
        ) VALUES (
            :request_id, :session_id, :customer_id, :channel, :mode, :level,
            :previous_request_id, :picked_slot, :action,
            CAST(:request_payload AS jsonb),
            CAST(:filters_applied AS jsonb),
            :relax_strategy,
            :candidates_pool_size, :total_matches,
            :slot1_unit_id, :slot1_plan_id, :slot1_concession_id,
            :slot1_first_month, :slot1_total_contract,
            :slot2_unit_id, :slot2_plan_id, :slot2_concession_id,
            :slot2_first_month, :slot2_total_contract,
            :slot3_unit_id, :slot3_plan_id, :slot3_concession_id,
            :slot3_first_month, :slot3_total_contract,
            CAST(:full_response AS jsonb)
        )
        RETURNING id
    """), params)

    row = result.fetchone()
    if not row:
        raise RuntimeError("log_served: INSERT RETURNING returned no row")
    return int(row[0])


def _request_to_dict(req: RecommendationRequest) -> Dict[str, Any]:
    """Serialise RecommendationRequest to a plain dict for JSONB storage."""
    return {
        'mode': req.mode,
        'level': req.level,
        'filters': req.filters,
        'duration_months': req.duration_months,
        'constraints': {
            k: v for k, v in req.constraints.items()
            if k != 'exclude_unit_ids'  # exclude_unit_ids tracked via session; save space
        },
        'context': {
            k: v for k, v in req.context.items()
            if not k.startswith('_')  # strip internal keys like _relax_strategy
        },
    }
