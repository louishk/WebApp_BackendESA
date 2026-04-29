"""
UnitDiscountCandidatesPipeline — recombine ccws_available_units × ccws_discount ×
mw_discount_plans.linked_concessions into the per-unit candidate snapshot
(esa_middleware.mw_unit_discount_candidates).

The unit source is `ccws_available_units` — only vacant + rentable + non-deleted
inventory. The recommender doesn't surface unavailable units, so building the
candidate set off the available subset keeps it small (~10x smaller than full
inventory) and naturally drops rented units between runs. Smart-lock keypad/
padlock assignments are joined in too, so consumers don't need a second hop.

No SOAP. Pure middleware-DB SQL + Python assembly. Each row is
decomposed per SOP COM01 via common.stype_name_parser.

Scope shape (same as ccws_* pipelines):
    {"site_codes": ["L017"]}
    {"location_codes": [...]}   # from default_args
    {} or None                  # fall through to default_args

Row write strategy: per-site atomic swap — DELETE WHERE site_id = ANY(...)
followed by bulk INSERT inside a single transaction.
"""
from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from dataclasses import replace as dc_replace
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import text

from common.stype_name_parser import parse_stype_name
from sync_service.config import get_engine
from sync_service.pipelines._ccws_utils import resolve_site_codes
from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)

# Columns on mw_unit_discount_candidates in the order we bulk-insert.
_INSERT_COLS: Tuple[str, ...] = (
    'site_id', 'unit_id', 'plan_id', 'concession_id',
    'site_code', 'unit_type_id', 'stype_name',
    'size_category', 'size_range', 'unit_type',
    'climate_type', 'unit_shape', 'pillar',
    'case_count', 'parse_ok',
    'std_rate', 'std_sec_dep', 'web_rate', 'push_rate', 'board_rate', 'preferred_rate',
    'amt_type', 'fixed_discount', 'pct_discount', 'max_amount_off',
    'plan_start', 'plan_end', 'never_expires',
    'in_month', 'prepay', 'prepaid_months',
    'b_for_all_units', 'b_for_corp', 'restriction_flags',
    'exclude_if_less_than', 'exclude_if_more_than', 'max_occ_pct',
    'plan_type', 'plan_name',
    'promo_period_start', 'promo_period_end',
    'booking_period_start', 'booking_period_end',
    'move_in_range', 'lock_in_period', 'payment_terms',
    'min_duration_months', 'max_duration_months',
    'distribution_channel', 'hidden_rate', 'coupon_code',
    'discount_type', 'discount_numeric', 'discount_segmentation',
    'is_active', 'smart_lock', 'effective_rate', 'computed_at',
)


class UnitDiscountCandidatesPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        site_codes = resolve_site_codes(scope)
        if not site_codes:
            return RunResult(
                status='failed', scope=scope,
                error='No site_codes resolved (provide scope or default_args.location_codes)',
            )

        engine = get_engine('middleware')
        now = datetime.utcnow()
        timings_ms: Dict[str, int] = {}
        t_start = time.perf_counter()

        def _mark(label: str, t0: float) -> None:
            timings_ms[label] = int((time.perf_counter() - t0) * 1000)

        with engine.begin() as conn:
            # 0. Recommender-level unit_type exclusions (global).
            t = time.perf_counter()
            excluded_rows = conn.execute(text(
                "SELECT unit_type FROM mw_recommender_excluded_unit_types"
            )).fetchall()
            excluded_unit_types = {r[0] for r in excluded_rows if r[0]}
            _mark('load_exclusions', t)

            # 0b. Legacy sTypeName fallback map (for sites that haven't migrated
            # to SOP COM01 yet). Lives in esa_backend, not middleware.
            t = time.perf_counter()
            legacy_map = _load_legacy_type_map()
            _mark('load_legacy_map', t)

            # 1. Site code → site_id map. Sourced from ccws_units (full
            # inventory) so the resolver stays stable even if a site has
            # zero available units in a given run.
            t = time.perf_counter()
            site_rows = conn.execute(text("""
                SELECT DISTINCT "sLocationCode", "SiteID"
                FROM ccws_units
                WHERE "sLocationCode" = ANY(:codes)
            """), {'codes': site_codes}).fetchall()

            code_to_site_id: Dict[str, int] = {r[0]: r[1] for r in site_rows if r[0]}
            site_ids = sorted(set(code_to_site_id.values()))
            _mark('resolve_sites', t)

            if not site_ids:
                # Nothing to refresh — clear the scope so stale rows vanish.
                return RunResult(
                    status='refreshed', records=0, scope=scope,
                    metadata={'site_codes_requested': site_codes,
                              'site_ids_resolved': 0,
                              'excluded_unit_types': sorted(excluded_unit_types)},
                )

            # 2. Active plans with their JSONB fields.
            # Drop plans that are already past their promo or booking window —
            # the recommender shouldn't see candidates for a campaign that's
            # already ended. Keep upcoming plans (start_date in the future)
            # so admins can preview them; the recommender's query-time check
            # will gate them from being offered until they're live.
            t = time.perf_counter()
            plan_rows = conn.execute(text("""
                SELECT id, plan_name, plan_type,
                       applicable_sites, linked_concessions, restrictions,
                       promo_period_start, promo_period_end,
                       booking_period_start, booking_period_end,
                       period_start, period_end,
                       move_in_range, lock_in_period, payment_terms,
                       distribution_channel, hidden_rate, coupon_code,
                       discount_type, discount_numeric, discount_segmentation,
                       is_active, is_stdrate_override
                FROM mw_discount_plans
                WHERE is_active = TRUE
                  AND (period_end           IS NULL OR period_end           >= CURRENT_DATE)
                  AND (promo_period_end     IS NULL OR promo_period_end     >= CURRENT_DATE)
                  AND (booking_period_end   IS NULL OR booking_period_end   >= CURRENT_DATE)
                  AND (period_start          IS NULL OR period_start          <= CURRENT_DATE)
                  AND (promo_period_start    IS NULL OR promo_period_start    <= CURRENT_DATE)
                  AND (booking_period_start  IS NULL OR booking_period_start  <= CURRENT_DATE)
            """)).mappings().all()
            _mark('load_plans', t)

            # 3. Active concessions at the sites we care about.
            t = time.perf_counter()
            conc_rows = conn.execute(text("""
                SELECT "SiteID", "ConcessionID", "sPlanName",
                       "iAmtType", "dcFixedDiscount", "dcPCDiscount", "dcMaxAmountOff",
                       "dPlanStrt", "dPlanEnd", "bNeverExpires",
                       "iInMonth", "bPrepay", "iPrePaidMonths",
                       "bForAllUnits", "bForCorp", "iRestrictionFlags",
                       "iExcludeIfLessThanUnitsTotal",
                       "iExcludeIfMoreThanUnitsTotal",
                       "dcMaxOccPct"
                FROM ccws_discount
                WHERE "SiteID" = ANY(:sids)
                  AND "dDeleted" IS NULL
                  AND "dDisabled" IS NULL
                  AND "dArchived" IS NULL
                  AND ("bNeverExpires" = TRUE
                       OR "dPlanEnd" IS NULL
                       OR "dPlanEnd" >= :now)
            """), {'sids': site_ids, 'now': now}).mappings().all()
            _mark('load_concessions', t)

            # 4. Available units at the sites we care about. ccws_available_units
            # is already filtered to vacant + rentable + non-deleted inventory.
            t = time.perf_counter()
            unit_rows = conn.execute(text("""
                SELECT "SiteID", "UnitID", "UnitTypeID", "sLocationCode",
                       "sTypeName", "bCorporate",
                       "dcStdRate", "dcStdSecDep", "dcWebRate", "dcPushRate",
                       "dcBoardRate", "dcPreferredRate"
                FROM ccws_available_units
                WHERE "SiteID" = ANY(:sids)
            """), {'sids': site_ids}).mappings().all()
            _mark('load_available_units', t)

            # 5. Smart-lock map: (site_id, unit_id) → {keypad_ids, padlock_id}.
            # Mirrors the join shape used in /api/smart-lock/units.
            t = time.perf_counter()
            sl_rows = conn.execute(text("""
                SELECT a.site_id, a.unit_id,
                       k1.keypad_id  AS kp1,
                       k2.keypad_id  AS kp2,
                       p.padlock_id  AS pl
                FROM mw_smart_lock_unit_assignments a
                LEFT JOIN mw_smart_lock_keypads   k1 ON k1.id = a.keypad_pk
                LEFT JOIN mw_smart_lock_keypads   k2 ON k2.id = a.keypad_2_pk
                LEFT JOIN mw_smart_lock_padlocks  p  ON p.id  = a.padlock_pk
                WHERE a.site_id = ANY(:sids)
            """), {'sids': site_ids}).fetchall()
            smart_lock_map: Dict[Tuple[int, int], Dict[str, Any]] = {}
            for sid, uid, kp1, kp2, pl in sl_rows:
                keypads = [k for k in (kp1, kp2) if k]
                if keypads or pl:
                    smart_lock_map[(sid, uid)] = {
                        'keypad_ids': keypads or None,
                        'padlock_id': pl,
                    }
            _mark('load_smart_lock', t)

            # 6. Compose.
            t = time.perf_counter()
            candidates, parse_fail_count, excluded_count, restriction_drop_count, legacy_mapped_count = _compose_candidates(
                plan_rows=plan_rows,
                conc_rows=conc_rows,
                unit_rows=unit_rows,
                code_to_site_id=code_to_site_id,
                computed_at=now,
                excluded_unit_types=excluded_unit_types,
                smart_lock_map=smart_lock_map,
                legacy_type_map=legacy_map,
            )
            _mark('compose', t)

            # 7. Per-site atomic swap.
            t = time.perf_counter()
            conn.execute(
                text("DELETE FROM mw_unit_discount_candidates WHERE site_id = ANY(:sids)"),
                {'sids': site_ids},
            )
            _mark('delete', t)

            t = time.perf_counter()
            if candidates:
                col_list = ', '.join(_INSERT_COLS)
                # Cast :smart_lock to jsonb explicitly — text() params bind as
                # text by default, and JSONB columns won't auto-coerce.
                # Use CAST(...) form because SQLAlchemy's text() parser treats
                # `:name::type` as ambiguous (`::` is its own escape).
                placeholders = ', '.join(
                    f'CAST(:{c} AS jsonb)' if c == 'smart_lock' else f':{c}'
                    for c in _INSERT_COLS
                )
                conn.execute(
                    text(f"INSERT INTO mw_unit_discount_candidates ({col_list}) "
                         f"VALUES ({placeholders})"),
                    candidates,
                )
            _mark('insert', t)

        timings_ms['total'] = int((time.perf_counter() - t_start) * 1000)

        per_site_counts: Dict[int, int] = defaultdict(int)
        for c in candidates:
            per_site_counts[c['site_id']] += 1

        logger.info(
            "candidates pipeline: %d rows across %d sites in %dms (%s)",
            len(candidates), len(site_ids), timings_ms['total'], timings_ms,
        )

        return RunResult(
            status='refreshed',
            records=len(candidates),
            scope=scope,
            metadata={
                'site_codes_requested': site_codes,
                'site_ids_resolved': len(site_ids),
                'plans_considered': len(plan_rows),
                'concessions_considered': len(conc_rows),
                'available_units_loaded': len(unit_rows),
                'smart_lock_assignments_loaded': len(smart_lock_map),
                'parse_fail_count': parse_fail_count,
                'legacy_mapped_count': legacy_mapped_count,
                'excluded_unit_types': sorted(excluded_unit_types),
                'excluded_count': excluded_count,
                'restriction_drop_count': restriction_drop_count,
                'per_site_counts': dict(per_site_counts),
                'timings_ms': timings_ms,
            },
        )


# ---------------------------------------------------------------------------
# Composition helpers (pure — unit-testable without DB)
# ---------------------------------------------------------------------------

def _site_applies(applicable_sites: Any, site_code: Optional[str]) -> bool:
    """Check whether a plan's applicable_sites dict includes this site_code.

    applicable_sites shape: {"L001": true, "L003": false, ...}
    Empty/None/absent → plan applies to no sites (conservative).
    """
    if not applicable_sites or not isinstance(applicable_sites, dict):
        return False
    if not site_code:
        return False
    return bool(applicable_sites.get(site_code))


def _load_legacy_type_map() -> Dict[str, Tuple[str, Optional[str]]]:
    """Load `inventory_type_mappings` from esa_backend as a quick lookup.

    Maps a legacy `sTypeName` (e.g. "AC Walk-In", "BizPlus", "Locker") to
    a (unit_type_code, climate_code) tuple aligned with SOP dim values.
    Used as a fallback when the COM01 parser cannot decode a value because
    its site hasn't migrated to the new naming convention yet.

    Returns an empty dict on any error — legacy fallback is best-effort.
    """
    try:
        backend_engine = get_engine('backend')
        with backend_engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT source_type_name, mapped_type_code, mapped_climate_code "
                "FROM inventory_type_mappings"
            )).fetchall()
        return {
            (r[0] or '').strip(): (r[1], r[2])
            for r in rows if r[0] and r[1]
        }
    except Exception as exc:
        logger.warning("legacy type-map unavailable: %s", exc)
        return {}


def _enrich_with_legacy(parts, raw_stype: Optional[str],
                        legacy_map: Dict[str, Tuple[str, Optional[str]]]):
    """If the SOP parse failed, fill unit_type/climate_type from the legacy map.

    Returns a (parts, used_legacy: bool) tuple. parse_ok stays False so
    downstream knows it's a legacy mapping rather than a true SOP decode.
    """
    if parts.parse_ok or parts.unit_type:
        return parts, False
    if not legacy_map or not raw_stype:
        return parts, False
    hit = legacy_map.get(raw_stype.strip())
    if not hit:
        return parts, False
    return dc_replace(parts, unit_type=hit[0], climate_type=hit[1]), True


def _jsonb_or_none(value: Optional[Dict[str, Any]]) -> Optional[str]:
    """Serialise a dict to a JSON string for the JSONB column.

    SQLAlchemy `text()` + executemany doesn't auto-serialise dicts to JSONB,
    so we pre-serialise here and cast `:smart_lock::jsonb` in the INSERT.
    Returns None when there's no payload — keeps the column NULL for units
    without smart-lock assignments.
    """
    if not value:
        return None
    return json.dumps(value)


def _effective_rate(
    std: Optional[Decimal],
    pct: Optional[Decimal],
    fixed: Optional[Decimal],
) -> Optional[Decimal]:
    """Best-effort first-month effective rate for sorting.

    Priority: percentage discount wins over fixed if both are set
    (matches the SOAP calculator's behaviour). Returns None if std_rate
    is missing.
    """
    if std is None:
        return None
    rate = Decimal(std)
    if pct is not None and pct > 0:
        rate = rate * (Decimal(1) - Decimal(pct) / Decimal(100))
    elif fixed is not None and fixed > 0:
        rate = rate - Decimal(fixed)
    if rate < 0:
        rate = Decimal(0)
    return rate


def _compose_candidates(
    plan_rows: Iterable[Dict[str, Any]],
    conc_rows: Iterable[Dict[str, Any]],
    unit_rows: Iterable[Dict[str, Any]],
    code_to_site_id: Dict[str, int],
    computed_at: datetime,
    excluded_unit_types: Optional[set] = None,
    smart_lock_map: Optional[Dict[Tuple[int, int], Dict[str, Any]]] = None,
    legacy_type_map: Optional[Dict[str, Tuple[str, Optional[str]]]] = None,
) -> Tuple[List[Dict[str, Any]], int, int, int, int]:
    """Build the bulk-insert rowset.

    Returns (rows, parse_fail_count, excluded_count, restriction_drop_count,
    legacy_mapped_count). A unit whose parsed unit_type is in
    `excluded_unit_types` is dropped silently (counted). A unit that fails
    any per-plan `restrictions` dim check is dropped into
    restriction_drop_count. Units with parse_ok=False are still emitted —
    excluding them would hide legacy inventory that has not yet migrated to
    the SOP naming. When legacy_type_map is provided, parse-failed units
    have their unit_type + climate_type filled from `inventory_type_mappings`
    so plan restrictions can still target them.
    """
    excluded_unit_types = excluded_unit_types or set()
    smart_lock_map = smart_lock_map or {}
    legacy_type_map = legacy_type_map or {}
    legacy_mapped_count = 0

    # Index concessions by (SiteID, ConcessionID) for O(1) lookup.
    conc_by_key: Dict[Tuple[int, int], Dict[str, Any]] = {
        (r['SiteID'], r['ConcessionID']): r for r in conc_rows
    }

    # Index units by SiteID. ccws_available_units is already filtered to
    # vacant + rentable + non-deleted, so no further screening here.
    units_by_site: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for u in unit_rows:
        units_by_site[u['SiteID']].append(u)

    site_id_to_code: Dict[int, str] = {sid: code for code, sid in code_to_site_id.items()}

    out: List[Dict[str, Any]] = []
    parse_fail_count = 0
    excluded_count = 0
    restriction_drop_count = 0

    _DIM_FIELDS = ('size_category', 'size_range', 'unit_type',
                   'climate_type', 'unit_shape', 'pillar')

    for plan in plan_rows:
        is_stdrate = bool(plan.get('is_stdrate_override'))
        linked = plan.get('linked_concessions') or []
        # Guard is only needed for the concession path; stdrate plans don't use
        # linked_concessions at all, so a malformed value there is harmless.
        if not is_stdrate and not isinstance(linked, list):
            continue

        # Per-plan restriction allow-sets. Empty dim = no restriction; a dim
        # present with a non-empty list = unit's parsed value must be inside.
        raw_restr = plan.get('restrictions') or {}
        restrict_sets: Dict[str, set] = {}
        plan_min_duration = None
        plan_max_duration = None
        if isinstance(raw_restr, dict):
            for field in _DIM_FIELDS:
                vals = raw_restr.get(field)
                if isinstance(vals, list) and vals:
                    restrict_sets[field] = {str(v) for v in vals}
            # Duration is metadata only — not a filter. Surface as columns.
            for key in ('min_duration_months', 'max_duration_months'):
                v = raw_restr.get(key)
                if v is not None:
                    try:
                        n = int(v)
                        if key == 'min_duration_months':
                            plan_min_duration = n
                        else:
                            plan_max_duration = n
                    except (TypeError, ValueError):
                        pass
            # Wine case-count window is a filter — units with no case_count
            # (= non-wine) are dropped if either bound is set.
            plan_min_cases: Optional[int] = None
            plan_max_cases: Optional[int] = None
            if isinstance(raw_restr, dict):
                for key, target in (('min_case_count', 'min'), ('max_case_count', 'max')):
                    v = raw_restr.get(key)
                    if v is not None:
                        try:
                            n = int(v)
                            if target == 'min':
                                plan_min_cases = n
                            else:
                                plan_max_cases = n
                        except (TypeError, ValueError):
                            pass

        if is_stdrate:
            # Standard Rate plan — iterate applicable_sites directly instead of
            # linked_concessions. One row per applicable site × available unit,
            # concession_id=0, all concession-derived fields NULL, and
            # effective_rate = std_rate (no discount applied). ConcessionID=0
            # is the booking-flow sentinel for "no concession" sent to SOAP.
            applicable_sites = plan.get('applicable_sites') or {}
            if not isinstance(applicable_sites, dict):
                applicable_sites = {}
            site_iter = [
                (code, code_to_site_id[code])
                for code, flag in applicable_sites.items()
                if flag and code in code_to_site_id
            ]
            for site_code, site_id in site_iter:
                units = units_by_site.get(site_id) or []
                for u in units:
                    # No concession → no corp gate; all units pass.
                    raw_stype = u.get('sTypeName')
                    parts = parse_stype_name(raw_stype)
                    if not parts.parse_ok:
                        parse_fail_count += 1
                        parts, _used_legacy = _enrich_with_legacy(parts, raw_stype, legacy_type_map)
                        if _used_legacy:
                            legacy_mapped_count += 1
                    # Drop units whose parsed unit_type is globally excluded.
                    if parts.unit_type and parts.unit_type in excluded_unit_types:
                        excluded_count += 1
                        continue
                    # Drop units that fail any of the plan's per-dim restrictions.
                    if restrict_sets:
                        passes = True
                        for field, allowed in restrict_sets.items():
                            val = getattr(parts, field, None)
                            if val is None or val not in allowed:
                                passes = False
                                break
                        if not passes:
                            restriction_drop_count += 1
                            continue
                    # Wine case-count gate.
                    if plan_min_cases is not None or plan_max_cases is not None:
                        cc = parts.case_count
                        if cc is None:
                            restriction_drop_count += 1
                            continue
                        if plan_min_cases is not None and cc < plan_min_cases:
                            restriction_drop_count += 1
                            continue
                        if plan_max_cases is not None and cc > plan_max_cases:
                            restriction_drop_count += 1
                            continue

                    std_rate = u.get('dcStdRate')
                    out.append({
                        'site_id': site_id,
                        'unit_id': u['UnitID'],
                        'plan_id': plan['id'],
                        'concession_id': 0,
                        'site_code': u.get('sLocationCode') or site_code,
                        'unit_type_id': u.get('UnitTypeID'),
                        'stype_name': u.get('sTypeName'),
                        'size_category': parts.size_category,
                        'size_range': parts.size_range,
                        'unit_type': parts.unit_type,
                        'climate_type': parts.climate_type,
                        'unit_shape': parts.unit_shape,
                        'pillar': parts.pillar,
                        'case_count': parts.case_count,
                        'parse_ok': parts.parse_ok,
                        'std_rate': std_rate,
                        'std_sec_dep': u.get('dcStdSecDep'),
                        'web_rate': u.get('dcWebRate'),
                        'push_rate': u.get('dcPushRate'),
                        'board_rate': u.get('dcBoardRate'),
                        'preferred_rate': u.get('dcPreferredRate'),
                        # Concession-derived fields: all NULL for stdrate rows.
                        'amt_type': None,
                        'fixed_discount': None,
                        'pct_discount': None,
                        'max_amount_off': None,
                        'plan_start': None,
                        'plan_end': None,
                        'never_expires': None,
                        'in_month': None,
                        'prepay': None,
                        'prepaid_months': None,
                        'b_for_all_units': None,
                        'b_for_corp': None,
                        'restriction_flags': None,
                        'exclude_if_less_than': None,
                        'exclude_if_more_than': None,
                        'max_occ_pct': None,
                        'plan_type': plan.get('plan_type'),
                        'plan_name': plan.get('plan_name'),
                        'promo_period_start': plan.get('promo_period_start'),
                        'promo_period_end': plan.get('promo_period_end'),
                        'booking_period_start': plan.get('booking_period_start'),
                        'booking_period_end': plan.get('booking_period_end'),
                        'move_in_range': plan.get('move_in_range'),
                        'lock_in_period': plan.get('lock_in_period'),
                        'payment_terms': plan.get('payment_terms'),
                        'min_duration_months': plan_min_duration,
                        'max_duration_months': plan_max_duration,
                        'distribution_channel': plan.get('distribution_channel'),
                        'hidden_rate': plan.get('hidden_rate'),
                        'coupon_code': plan.get('coupon_code'),
                        'discount_type': plan.get('discount_type'),
                        'discount_numeric': plan.get('discount_numeric'),
                        'discount_segmentation': plan.get('discount_segmentation'),
                        'is_active': plan.get('is_active'),
                        'smart_lock': _jsonb_or_none(smart_lock_map.get((site_id, u['UnitID']))),
                        'effective_rate': std_rate,
                        'computed_at': computed_at,
                    })
        else:
            for link in linked:
                if not isinstance(link, dict):
                    continue
                try:
                    site_id = int(link.get('site_id'))
                    concession_id = int(link.get('concession_id'))
                except (TypeError, ValueError):
                    continue

                site_code = site_id_to_code.get(site_id)
                # Only emit if plan's applicable_sites allows this site.
                if not _site_applies(plan.get('applicable_sites'), site_code):
                    continue

                conc = conc_by_key.get((site_id, concession_id))
                if conc is None:
                    # Concession inactive or not synced yet — skip.
                    continue

                units = units_by_site.get(site_id) or []
                for u in units:
                    # Corporate sanity: if the concession is corporate-only and the
                    # unit is non-corporate (or vice versa), skip. Conservative —
                    # only skip on explicit mismatch; surface the flags as columns
                    # so recommender can override later.
                    conc_for_corp = conc.get('bForCorp')
                    unit_corp = u.get('bCorporate')
                    if conc_for_corp is True and unit_corp is False:
                        continue
                    if conc_for_corp is False and unit_corp is True:
                        # Concession explicitly not-for-corp, unit is corp — skip.
                        continue

                    raw_stype = u.get('sTypeName')
                    parts = parse_stype_name(raw_stype)
                    if not parts.parse_ok:
                        parse_fail_count += 1
                        parts, _used_legacy = _enrich_with_legacy(parts, raw_stype, legacy_type_map)
                        if _used_legacy:
                            legacy_mapped_count += 1
                    # Drop units whose parsed unit_type is globally excluded.
                    if parts.unit_type and parts.unit_type in excluded_unit_types:
                        excluded_count += 1
                        continue
                    # Drop units that fail any of the plan's per-dim restrictions.
                    if restrict_sets:
                        passes = True
                        for field, allowed in restrict_sets.items():
                            val = getattr(parts, field, None)
                            if val is None or val not in allowed:
                                passes = False
                                break
                        if not passes:
                            restriction_drop_count += 1
                            continue
                    # Wine case-count gate — when either bound is set, the unit
                    # must (a) have a case_count and (b) fall within the range.
                    if plan_min_cases is not None or plan_max_cases is not None:
                        cc = parts.case_count
                        if cc is None:
                            restriction_drop_count += 1
                            continue
                        if plan_min_cases is not None and cc < plan_min_cases:
                            restriction_drop_count += 1
                            continue
                        if plan_max_cases is not None and cc > plan_max_cases:
                            restriction_drop_count += 1
                            continue

                    std_rate = u.get('dcStdRate')
                    pct_discount = conc.get('dcPCDiscount')
                    fixed_discount = conc.get('dcFixedDiscount')

                    out.append({
                        'site_id': site_id,
                        'unit_id': u['UnitID'],
                        'plan_id': plan['id'],
                        'concession_id': concession_id,
                        'site_code': u.get('sLocationCode') or site_code,
                        'unit_type_id': u.get('UnitTypeID'),
                        'stype_name': u.get('sTypeName'),
                        'size_category': parts.size_category,
                        'size_range': parts.size_range,
                        'unit_type': parts.unit_type,
                        'climate_type': parts.climate_type,
                        'unit_shape': parts.unit_shape,
                        'pillar': parts.pillar,
                        'case_count': parts.case_count,
                        'parse_ok': parts.parse_ok,
                        'std_rate': std_rate,
                        'std_sec_dep': u.get('dcStdSecDep'),
                        'web_rate': u.get('dcWebRate'),
                        'push_rate': u.get('dcPushRate'),
                        'board_rate': u.get('dcBoardRate'),
                        'preferred_rate': u.get('dcPreferredRate'),
                        'amt_type': conc.get('iAmtType'),
                        'fixed_discount': fixed_discount,
                        'pct_discount': pct_discount,
                        'max_amount_off': conc.get('dcMaxAmountOff'),
                        'plan_start': conc.get('dPlanStrt'),
                        'plan_end': conc.get('dPlanEnd'),
                        'never_expires': conc.get('bNeverExpires'),
                        'in_month': conc.get('iInMonth'),
                        'prepay': conc.get('bPrepay'),
                        'prepaid_months': conc.get('iPrePaidMonths'),
                        'b_for_all_units': conc.get('bForAllUnits'),
                        'b_for_corp': conc.get('bForCorp'),
                        'restriction_flags': conc.get('iRestrictionFlags'),
                        'exclude_if_less_than': conc.get('iExcludeIfLessThanUnitsTotal'),
                        'exclude_if_more_than': conc.get('iExcludeIfMoreThanUnitsTotal'),
                        'max_occ_pct': conc.get('dcMaxOccPct'),
                        'plan_type': plan.get('plan_type'),
                        'plan_name': plan.get('plan_name'),
                        'promo_period_start': plan.get('promo_period_start'),
                        'promo_period_end': plan.get('promo_period_end'),
                        'booking_period_start': plan.get('booking_period_start'),
                        'booking_period_end': plan.get('booking_period_end'),
                        'move_in_range': plan.get('move_in_range'),
                        'lock_in_period': plan.get('lock_in_period'),
                        'payment_terms': plan.get('payment_terms'),
                        'min_duration_months': plan_min_duration,
                        'max_duration_months': plan_max_duration,
                        'distribution_channel': plan.get('distribution_channel'),
                        'hidden_rate': plan.get('hidden_rate'),
                        'coupon_code': plan.get('coupon_code'),
                        'discount_type': plan.get('discount_type'),
                        'discount_numeric': plan.get('discount_numeric'),
                        'discount_segmentation': plan.get('discount_segmentation'),
                        'is_active': plan.get('is_active'),
                        'smart_lock': _jsonb_or_none(smart_lock_map.get((site_id, u['UnitID']))),
                        'effective_rate': _effective_rate(std_rate, pct_discount, fixed_discount),
                        'computed_at': computed_at,
                    })

    # Dedup on PK — last writer wins — protects against duplicated linked_concession
    # entries within a single plan row.
    dedup: Dict[Tuple[int, int, int, int], Dict[str, Any]] = {}
    for row in out:
        dedup[(row['site_id'], row['unit_id'], row['plan_id'], row['concession_id'])] = row
    return list(dedup.values()), parse_fail_count, excluded_count, restriction_drop_count, legacy_mapped_count
