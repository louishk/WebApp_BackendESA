"""
Discount Plans routes - CRUD management, promotion brief view, and config.
"""

import json
from datetime import date
from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import asc
from web.utils.audit import audit_log, AuditEvent
from web.utils.rate_limit import rate_limit_api

discount_plans_bp = Blueprint('discount_plans', __name__, url_prefix='/discount-plans')


def get_session():
    """Get middleware DB session (discount plans live in esa_middleware)."""
    return current_app.get_middleware_session()


# =============================================================================
# Legacy sTypeName fallback — cached process-wide
# =============================================================================
_LEGACY_TYPE_MAP_CACHE: dict | None = None


def _load_legacy_type_map_cached() -> dict:
    """Load `inventory_type_mappings` once per process.

    Returns {source_type_name -> (mapped_type_code, mapped_climate_code)}.
    Used by the live unit-count endpoint to mirror the candidates pipeline's
    legacy fallback logic.
    """
    global _LEGACY_TYPE_MAP_CACHE
    if _LEGACY_TYPE_MAP_CACHE is not None:
        return _LEGACY_TYPE_MAP_CACHE
    try:
        from sqlalchemy import text as sqltext
        s = current_app.get_db_session()
        try:
            rows = s.execute(sqltext(
                "SELECT source_type_name, mapped_type_code, mapped_climate_code "
                "FROM inventory_type_mappings"
            )).fetchall()
            _LEGACY_TYPE_MAP_CACHE = {
                (r[0] or '').strip(): (r[1], r[2])
                for r in rows if r[0] and r[1]
            }
        finally:
            s.close()
    except Exception as exc:
        current_app.logger.warning(f"legacy type-map unavailable: {exc}")
        _LEGACY_TYPE_MAP_CACHE = {}
    return _LEGACY_TYPE_MAP_CACHE


def _trigger_candidates_refresh(site_codes: list) -> None:
    """Fire-and-forget refresh of mw_unit_discount_candidates for given sites.

    Spawns a daemon thread that runs the pipeline scoped to the affected
    site_codes. Returns immediately so the web request stays snappy.
    Failures are logged but never bubble up — the 4h scheduled run will
    eventually catch up.
    """
    if not site_codes:
        return
    codes = sorted({str(c).strip() for c in site_codes if c and str(c).strip()})
    if not codes:
        return

    import threading
    # Capture the real app object — `current_app` is request-scoped and not
    # safe to dereference inside the worker thread.
    app = current_app._get_current_object()

    def _run():
        try:
            with app.app_context():
                from sync_service.executor import get_executor
                result = get_executor().run(
                    pipeline_name='mw_unit_discount_candidates',
                    scope={'site_codes': codes},
                    triggered_by='discount_plan_save',
                )
                app.logger.info(
                    f"candidates refresh after plan save: status={result.status} "
                    f"records={result.records} sites={codes}"
                )
        except Exception as exc:
            app.logger.warning(
                f"candidates refresh failed for {codes}: {exc}"
            )

    threading.Thread(target=_run, daemon=True, name='candidates-refresh').start()


def _site_codes_from_plan(plan) -> list:
    """Extract applicable_sites codes (where flag is True) from a plan."""
    sites = (plan.applicable_sites or {}) if plan else {}
    if not isinstance(sites, dict):
        return []
    return [code for code, flag in sites.items() if flag]


_pbi_engine = None


def _get_pbi_engine():
    global _pbi_engine
    if _pbi_engine is None:
        from common.config_loader import get_database_url
        from sqlalchemy import create_engine
        pbi_url = get_database_url('pbi')
        _pbi_engine = create_engine(pbi_url, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=300)
    return _pbi_engine


def _get_pbi_session():
    """Get PBI database session for Site queries."""
    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=_get_pbi_engine())
    return Session()


def _get_sites_by_country():
    """
    Query SiteInfo table from PBI DB. Returns:
    - sites_by_country: {'Hong Kong': [{'code': 'L004', 'name': 'Yau Tong (YT)'}, ...], ...}
    - all_site_codes: ['L001', 'L003', ...] (for backward compat)
    """
    try:
        from common.models import SiteInfo
        session = _get_pbi_session()
        try:
            sites = (session.query(SiteInfo.SiteCode, SiteInfo.Name, SiteInfo.InternalLabel, SiteInfo.Country)
                     .filter(SiteInfo.SiteCode.isnot(None))
                     .order_by(SiteInfo.SiteCode)
                     .all())
            by_country = {}
            all_codes = []
            for code, name, label, country in sites:
                if not code:
                    continue
                country = country or 'Unknown'
                display = f"{name} ({label})" if label else name or code
                if country not in by_country:
                    by_country[country] = []
                by_country[country].append({'code': code, 'name': display})
                all_codes.append(code)
            return by_country, all_codes
        finally:
            session.close()
    except Exception as e:
        current_app.logger.warning(f"Could not load sites from PBI DB: {e}")
        fallback = ['L001', 'L003', 'L004', 'L005', 'L006', 'L007', 'L008', 'L009', 'L010']
        return {'Sites': [{'code': c, 'name': c} for c in fallback]}, fallback


def _get_config_options():
    """Load active config options grouped by field_name."""
    from web.models.discount_plan_config import DiscountPlanConfig
    db_session = get_session()
    try:
        options = (db_session.query(DiscountPlanConfig)
                   .filter_by(is_active=True)
                   .order_by(DiscountPlanConfig.field_name, DiscountPlanConfig.sort_order)
                   .all())
        grouped = {}
        for opt in options:
            grouped.setdefault(opt.field_name, []).append(opt.option_value)
        return grouped
    finally:
        db_session.close()


def _require_config_permission(f):
    """Require config management permission for editing discount plans."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))
        if not current_user.can_manage_configs():
            flash('Config management access required to manage discount plans.', 'error')
            return redirect(url_for('main.dashboard'))
        return f(*args, **kwargs)
    return decorated


PLAN_TYPES = ['Evergreen', 'Tactical', 'Seasonal']
DISCOUNT_TYPES = ['none', 'percentage', 'fixed_amount', 'free_period']
ELIGIBILITY_OPTIONS = ['Not Eligible', 'Eligible']


def _parse_json_field(form_value, default=None):
    """Safely parse a JSON string from a form field."""
    if not form_value or not form_value.strip():
        return default
    try:
        return json.loads(form_value)
    except (json.JSONDecodeError, TypeError):
        return default


def _parse_date_field(form_value):
    """Parse a date string (YYYY-MM-DD) from form input."""
    if not form_value or not form_value.strip():
        return None
    try:
        return date.fromisoformat(form_value.strip())
    except ValueError:
        return None


def _derive_discount_segmentation(discount_type, discount_numeric):
    """Bucket a percentage discount into the canonical segmentation labels.

    Canonical buckets (from mw_discount_plan_config.field_name='discount_segmentation'):
        < 5%, >= 5% < 10%, >= 10% < 20%, >= 20% < 30%, >= 30% < 40%, >= 40%

    Only percentage-type discounts are bucketed. Other discount types return
    None — the recommender shouldn't treat a fixed-amount or free-period plan
    as a percentage bucket without knowing the base rate.
    """
    if discount_type != 'percentage':
        return None
    try:
        n = float(discount_numeric) if discount_numeric is not None else None
    except (TypeError, ValueError):
        return None
    if n is None:
        return None
    if n < 5:
        return '< 5%'
    if n < 10:
        return '>= 5% < 10%'
    if n < 20:
        return '>= 10% < 20%'
    if n < 30:
        return '>= 20% < 30%'
    if n < 40:
        return '>= 30% < 40%'
    return '>= 40%'


def _stdrate_links_for_sites(site_codes: list) -> list[dict]:
    """Synthesize linked_concessions entries for a Standard Rate plan.

    Resolves each SiteCode to its SiteID via mw_siteinfo and returns
    `[{site_id, concession_id: 0, site_code}, ...]`. Codes that don't
    resolve are dropped silently.
    """
    codes = [c for c in (site_codes or []) if c]
    if not codes:
        return []
    from sqlalchemy import text as sqltext
    session = get_session()
    try:
        rows = session.execute(sqltext(
            'SELECT "SiteID", "SiteCode" FROM mw_siteinfo WHERE "SiteCode" = ANY(:codes)'
        ), {'codes': codes}).mappings().all()
        return [
            {'site_id': r['SiteID'], 'concession_id': 0, 'site_code': r['SiteCode']}
            for r in rows
        ]
    except Exception as exc:
        current_app.logger.warning(f"_stdrate_links_for_sites failed: {exc}")
        return []
    finally:
        session.close()


def _build_plan_from_form(form, plan=None, config_options=None):
    """Extract discount plan fields from the submitted form."""
    from web.models.discount_plan import DiscountPlan

    if plan is None:
        plan = DiscountPlan()

    if config_options is None:
        config_options = _get_config_options()

    def _validate_config_value(value, field_name):
        """Validate value against config allowlist. Returns None if invalid."""
        if not value:
            return None
        allowed = config_options.get(field_name, [])
        if allowed and value not in allowed:
            return None
        return value

    # Identification
    plan.plan_type = form.get('plan_type', '').strip()
    plan.plan_name = form.get('plan_name', '').strip()
    plan.group_name = form.get('group_name', '').strip() or None

    # Description
    plan.notes = form.get('notes', '').strip() or None
    plan.objective = form.get('objective', '').strip() or None

    # Availability
    plan.promo_period_start = _parse_date_field(form.get('promo_period_start'))
    plan.promo_period_end = _parse_date_field(form.get('promo_period_end'))
    plan.booking_period_start = _parse_date_field(form.get('booking_period_start'))
    plan.booking_period_end = _parse_date_field(form.get('booking_period_end'))
    plan.move_in_range = _validate_config_value(form.get('move_in_range', '').strip(), 'move_in_range')

    # Lock-in period
    plan.lock_in_period = _validate_config_value(form.get('lock_in_period', '').strip(), 'lock_in_period')

    # Sites - build from checkboxes (dynamic codes from form)
    site_codes = form.getlist('site_code')
    sites = {}
    for code in site_codes:
        sites[code] = True
    plan.applicable_sites = sites

    # Storage type (validated against config)
    plan.storage_type = _validate_config_value(form.get('storage_type', '').strip(), 'storage_type')

    # Discount details
    plan.discount_type = form.get('discount_type', '').strip() or None
    raw_numeric = form.get('discount_numeric', '').strip()
    try:
        plan.discount_numeric = float(raw_numeric) if raw_numeric else None
    except ValueError:
        plan.discount_numeric = None
    # Segmentation is auto-derived from (discount_type, discount_numeric).
    plan.discount_segmentation = _derive_discount_segmentation(
        plan.discount_type, plan.discount_numeric,
    )
    plan.clawback_condition = form.get('clawback_condition', '').strip() or None

    # Offers (JSON)
    plan.offers = _parse_json_field(form.get('offers_json'), [])

    # Terms
    plan.deposit = _validate_config_value(form.get('deposit', '').strip(), 'deposit')
    plan.payment_terms = _validate_config_value(form.get('payment_terms', '').strip(), 'payment_terms')
    plan.termination_notice = _validate_config_value(form.get('termination_notice', '').strip(), 'termination_notice')
    plan.extra_offer = form.get('extra_offer', '').strip() or None

    # T&Cs - dynamic array-style fields with labels
    tcs_label_raw = form.getlist('tc_label')
    tcs_en_raw = form.getlist('tc_en')
    tcs_cn_raw = form.getlist('tc_cn')
    # Filter out empty rows (where EN text is blank)
    tcs_labels = []
    tcs_en = []
    tcs_cn = []
    for i in range(len(tcs_en_raw)):
        en_text = tcs_en_raw[i].strip() if i < len(tcs_en_raw) else ''
        if not en_text:
            continue
        tcs_en.append(en_text)
        tcs_cn.append(tcs_cn_raw[i].strip() if i < len(tcs_cn_raw) else '')
        tcs_labels.append(tcs_label_raw[i].strip() if i < len(tcs_label_raw) else '')
    plan.terms_conditions = tcs_en if tcs_en else None
    plan.terms_conditions_cn = [t for t in tcs_cn if t] if any(tcs_cn) else None
    plan.tc_labels = tcs_labels if any(tcs_labels) else None

    # Promotion brief fields
    plan.hidden_rate = form.get('hidden_rate') == 'on'
    plan.coupon_code = (form.get('coupon_code') or '').strip().upper() or None
    plan.switch_to_us = _validate_config_value(form.get('switch_to_us', '').strip(), 'switch_to_us') or 'Not Eligible'
    plan.referral_program = _validate_config_value(form.get('referral_program', '').strip(), 'referral_program') or 'Not Eligible'
    # Distribution channel (multi-choice checkboxes, stored comma-separated, validated against config)
    allowed_channels = set(config_options.get('distribution_channel', []))
    dist_channels = form.getlist('distribution_channel')
    valid_channels = [c.strip() for c in dist_channels if c.strip() and (not allowed_channels or c.strip() in allowed_channels)]
    plan.distribution_channel = ', '.join(valid_channels) or None

    # Standard Rate override — when ticked, the plan represents stdrate
    # (ConcessionID=0). linked_concessions is auto-synthesized from
    # applicable_sites below, regardless of what the picker submitted.
    plan.is_stdrate_override = form.get('is_stdrate_override') == 'on'

    # Linked Sitelink concessions
    if plan.is_stdrate_override:
        plan.linked_concessions = _stdrate_links_for_sites(list(sites.keys()))
    else:
        plan.linked_concessions = _parse_json_field(form.get('linked_concessions_json'), [])

    # Unit-level restrictions (SOP COM01). Stored as {dim: [codes], min/max_duration_months: int}.
    # Only the known dim keys are persisted — unknown keys from client are dropped.
    raw_restr = _parse_json_field(form.get('restrictions_json'), {})
    cleaned_restr: dict = {}
    if isinstance(raw_restr, dict):
        for field in _DIM_FIELDS:
            vals = raw_restr.get(field)
            if isinstance(vals, list):
                cleaned_restr[field] = [str(v).strip() for v in vals if str(v).strip()]

    def _parse_month_int(s):
        s = (s or '').strip()
        if not s:
            return None
        try:
            v = int(s)
            return v if v >= 0 else None
        except ValueError:
            return None

    min_dur = _parse_month_int(form.get('min_duration_months'))
    max_dur = _parse_month_int(form.get('max_duration_months'))
    # Swap if user inverted them; keep None when blank.
    if min_dur is not None and max_dur is not None and min_dur > max_dur:
        min_dur, max_dur = max_dur, min_dur
    if min_dur is not None:
        cleaned_restr['min_duration_months'] = min_dur
    if max_dur is not None:
        cleaned_restr['max_duration_months'] = max_dur

    # Wine case-count restriction (only meaningful for wine sTypeName).
    # When set, candidates whose unit has no case_count (= non-wine) are
    # dropped, naturally narrowing the plan to wine inventory.
    min_cases = _parse_month_int(form.get('min_case_count'))
    max_cases = _parse_month_int(form.get('max_case_count'))
    if min_cases is not None and max_cases is not None and min_cases > max_cases:
        min_cases, max_cases = max_cases, min_cases
    if min_cases is not None:
        cleaned_restr['min_case_count'] = min_cases
    if max_cases is not None:
        cleaned_restr['max_case_count'] = max_cases

    plan.restrictions = cleaned_restr

    # Status
    plan.is_active = form.get('is_active') == 'on'
    raw_sort = form.get('sort_order', '0').strip()
    plan.sort_order = int(raw_sort) if raw_sort.isdigit() else 0

    return plan


def _edit_tpl_kwargs():
    """Build common template kwargs for create/edit pages."""
    from web.models.discount_plan import DiscountPlan

    sites_by_country, all_site_codes = _get_sites_by_country()
    # Existing group names for the <datalist> autocomplete on the edit form.
    existing_groups: list[str] = []
    try:
        session = get_session()
        try:
            rows = (session.query(DiscountPlan.group_name)
                    .filter(DiscountPlan.group_name.isnot(None))
                    .filter(DiscountPlan.group_name != '')
                    .distinct()
                    .order_by(DiscountPlan.group_name)
                    .all())
            existing_groups = [r[0] for r in rows if r[0]]
        finally:
            session.close()
    except Exception as e:
        current_app.logger.warning(f"Could not load existing groups: {e}")
    return dict(
        sites_by_country=sites_by_country, site_codes=all_site_codes,
        plan_types=PLAN_TYPES, discount_types=DISCOUNT_TYPES,
        eligibility_options=ELIGIBILITY_OPTIONS,
        config_options=_get_config_options(),
        existing_groups=existing_groups,
    )


# =============================================================================
# List
# =============================================================================

@discount_plans_bp.route('/')
@login_required
def list_plans():
    """List all discount plans with per-plan candidate counts + setup audit."""
    from web.models.discount_plan import DiscountPlan
    from sqlalchemy import text as sqltext

    sites_by_country, all_site_codes = _get_sites_by_country()
    db_session = get_session()
    try:
        plans = (db_session.query(DiscountPlan)
                 .order_by(asc(DiscountPlan.sort_order), asc(DiscountPlan.plan_name))
                 .all())

        # Candidate counts per plan id — how many units each plan reaches
        # after all restriction + exclusion filtering by the pipeline.
        cand_counts: dict = {}
        try:
            rows = db_session.execute(sqltext("""
                SELECT plan_id, COUNT(*) AS n
                FROM mw_unit_discount_candidates
                GROUP BY plan_id
            """)).fetchall()
            cand_counts = {r[0]: r[1] for r in rows}
        except Exception as e:
            current_app.logger.warning(f"candidate count query failed: {e}")

        # Per-plan audit — flag missing setup.
        audits = {p.id: _audit_plan(p) for p in plans}

        return render_template('admin/discount_plans/list.html',
                               plans=plans,
                               sites_by_country=sites_by_country,
                               site_codes=all_site_codes,
                               cand_counts=cand_counts,
                               audits=audits)
    finally:
        db_session.close()


def _audit_plan(plan) -> dict:
    """Return a simple health report for a plan.

    status: "ok" | "warn" | "error"
    issues: list of short strings explaining what's missing or inconsistent
    """
    issues: list[str] = []
    # Sites
    applicable = plan.applicable_sites or {}
    has_sites = any(bool(v) for v in applicable.values()) if isinstance(applicable, dict) else False
    if not has_sites:
        issues.append('no applicable sites')
    # Linked concessions — skipped for stdrate-override plans (no real concession by design).
    if not getattr(plan, 'is_stdrate_override', False):
        linked = plan.linked_concessions or []
        if not isinstance(linked, list) or not linked:
            issues.append('no linked SiteLink concessions')
    # Discount numeric
    if plan.discount_numeric is None:
        issues.append('no discount_numeric')
    # Discount type
    if not plan.discount_type:
        issues.append('no discount_type')
    # Period dates
    if not plan.promo_period_start and not plan.promo_period_end:
        issues.append('no promo period')
    # Hidden rate must have a coupon — otherwise the recommender silently
    # excludes it from every channel and the plan is effectively dead.
    if getattr(plan, 'hidden_rate', False) and not (plan.coupon_code or '').strip():
        issues.append('hidden_rate set but no coupon_code')

    # Duration
    restr = plan.restrictions or {}
    if isinstance(restr, dict):
        mn = restr.get('min_duration_months')
        mx = restr.get('max_duration_months')
        if mn is None and mx is None:
            issues.append('no min/max duration')
        elif mn is not None and mx is not None and mn > mx:
            issues.append('min > max duration')

    if not issues:
        status = 'ok'
    elif any(k in ' '.join(issues) for k in ('no linked', 'min > max')):
        status = 'error'
    else:
        status = 'warn'
    return {'status': status, 'issues': issues}


# =============================================================================
# Create
# =============================================================================

@discount_plans_bp.route('/create', methods=['GET', 'POST'])
@login_required
@_require_config_permission
def create_plan():
    """Create a new discount plan."""
    from web.models.discount_plan import DiscountPlan

    tpl_kwargs = _edit_tpl_kwargs()

    if request.method == 'POST':
        db_session = get_session()
        try:
            plan = _build_plan_from_form(request.form, config_options=tpl_kwargs.get('config_options'))
            plan.created_by = current_user.username

            if not plan.plan_type or not plan.plan_name:
                flash('Plan Type and Plan Name are required.', 'error')
                return render_template('admin/discount_plans/edit.html',
                                       plan=None, form_data=request.form, **tpl_kwargs)

            existing = db_session.query(DiscountPlan).filter_by(plan_name=plan.plan_name).first()
            if existing:
                flash('A plan with this name already exists.', 'error')
                return render_template('admin/discount_plans/edit.html',
                                       plan=None, form_data=request.form, **tpl_kwargs)

            db_session.add(plan)
            db_session.commit()

            audit_log(AuditEvent.CONFIG_UPDATED, f"Created discount plan '{plan.plan_name}'")
            _trigger_candidates_refresh(_site_codes_from_plan(plan))
            flash(f'Discount plan "{plan.plan_name}" created.', 'success')
            return redirect(url_for('discount_plans.list_plans'))
        except Exception as e:
            db_session.rollback()
            current_app.logger.error(f"Error creating discount plan: {e}")
            flash('An error occurred while creating the plan.', 'error')
        finally:
            db_session.close()

    return render_template('admin/discount_plans/edit.html',
                           plan=None, form_data=request.form, **tpl_kwargs)


# =============================================================================
# Edit
# =============================================================================

@discount_plans_bp.route('/<int:plan_id>/edit', methods=['GET', 'POST'])
@login_required
@_require_config_permission
def edit_plan(plan_id):
    """Edit an existing discount plan."""
    from web.models.discount_plan import DiscountPlan

    tpl_kwargs = _edit_tpl_kwargs()

    db_session = get_session()
    try:
        plan = db_session.query(DiscountPlan).get(plan_id)
        if not plan:
            flash('Discount plan not found.', 'error')
            return redirect(url_for('discount_plans.list_plans'))

        if request.method == 'POST':
            old_name = plan.plan_name
            old_sites = _site_codes_from_plan(plan)
            _build_plan_from_form(request.form, plan, config_options=tpl_kwargs.get('config_options'))
            plan.updated_by = current_user.username

            if not plan.plan_type or not plan.plan_name:
                flash('Plan Type and Plan Name are required.', 'error')
                return render_template('admin/discount_plans/edit.html',
                                       plan=plan, form_data=request.form, **tpl_kwargs)

            # Check uniqueness if name changed
            if plan.plan_name != old_name:
                existing = db_session.query(DiscountPlan).filter_by(plan_name=plan.plan_name).first()
                if existing and existing.id != plan_id:
                    flash('A plan with this name already exists.', 'error')
                    return render_template('admin/discount_plans/edit.html',
                                           plan=plan, form_data=request.form, **tpl_kwargs)

            db_session.commit()
            audit_log(AuditEvent.CONFIG_UPDATED, f"Updated discount plan '{plan.plan_name}' (id={plan_id})")
            # Refresh candidates for the union of old + new sites so removed
            # sites get their stale rows wiped on the next per-site DELETE+INSERT.
            new_sites = _site_codes_from_plan(plan)
            _trigger_candidates_refresh(sorted(set(old_sites) | set(new_sites)))
            flash('Discount plan updated.', 'success')
            return redirect(url_for('discount_plans.list_plans'))

        return render_template('admin/discount_plans/edit.html',
                               plan=plan, form_data=request.form, **tpl_kwargs)
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error editing discount plan: {e}")
        flash('An error occurred.', 'error')
        return redirect(url_for('discount_plans.list_plans'))
    finally:
        db_session.close()


# =============================================================================
# Delete
# =============================================================================

# =============================================================================
# AI-assisted extraction from pasted promo brief
# =============================================================================

_COUNTRY_NAME_MAP = {
    'SG': 'Singapore',
    'MY': 'Malaysia',
    'KR': 'South Korea',
    'HK': 'Hong Kong',
    'JP': 'Japan',
    'TW': 'Taiwan',
    'TH': 'Thailand',
}


def _site_codes_for_country(country_iso: str) -> list[str]:
    """Map a 2-letter country code to SiteCodes via SiteInfo."""
    from common.models import SiteInfo
    session = _get_pbi_session()
    try:
        name = _COUNTRY_NAME_MAP.get(country_iso)
        if not name:
            return []
        rows = (session.query(SiteInfo.SiteCode)
                .filter(SiteInfo.Country == name)
                .filter(SiteInfo.SiteCode.isnot(None))
                .all())
        return [r[0] for r in rows if r[0]]
    finally:
        session.close()


def _resolve_sitelink_name_to_concessions(plan_name: str, site_codes: list[str]) -> list[dict]:
    """Find active ccws_discount rows matching `plan_name` within `site_codes`.

    Returns JSONB-ready link entries: {site_id, concession_id, site_code, plan_name}.
    Exact match first; falls back to case-insensitive substring if no exact hit.
    """
    from sqlalchemy import text as sqltext
    from datetime import datetime
    if not plan_name or not site_codes:
        return []

    session = get_session()
    try:
        now = datetime.utcnow()
        params = {
            'codes': site_codes,
            'now': now,
            'name_exact': plan_name,
            'name_like': f'%{plan_name}%',
        }
        rows = session.execute(sqltext("""
            WITH site_ids AS (
                SELECT "SiteID", "SiteCode"
                FROM mw_siteinfo
                WHERE "SiteCode" = ANY(:codes)
            ),
            matches AS (
                SELECT d."SiteID", d."ConcessionID", d."sPlanName",
                       CASE WHEN d."sPlanName" = :name_exact THEN 1 ELSE 2 END AS priority
                FROM ccws_discount d
                WHERE d."SiteID" IN (SELECT "SiteID" FROM site_ids)
                  AND d."dDeleted" IS NULL
                  AND d."dDisabled" IS NULL
                  AND d."dArchived" IS NULL
                  AND (d."bNeverExpires" = TRUE
                       OR d."dPlanEnd" IS NULL
                       OR d."dPlanEnd" >= :now)
                  AND (d."sPlanName" = :name_exact
                       OR d."sPlanName" ILIKE :name_like)
            ),
            ranked AS (
                SELECT m.*, s."SiteCode",
                       MIN(priority) OVER (PARTITION BY m."SiteID") AS best
                FROM matches m
                JOIN site_ids s ON s."SiteID" = m."SiteID"
            )
            SELECT "SiteID", "ConcessionID", "sPlanName", "SiteCode"
            FROM ranked
            WHERE priority = best
            ORDER BY "SiteCode", "ConcessionID"
        """), params).mappings().all()

        return [
            {
                'site_id': r['SiteID'],
                'concession_id': r['ConcessionID'],
                'site_code': r['SiteCode'] or '',
                'plan_name': r['sPlanName'] or '',
            }
            for r in rows
        ]
    except Exception as e:
        current_app.logger.warning(
            f"_resolve_sitelink_name_to_concessions failed for {plan_name!r}: {e}"
        )
        return []
    finally:
        session.close()


# =============================================================================
# Restrictions helpers — multi-select + live vacant-unit count
# =============================================================================

_DIM_FIELDS = ('size_category', 'size_range', 'unit_type',
               'climate_type', 'unit_shape', 'pillar')
_DIM_TABLE = {
    'size_category': ('mw_dim_size_category', 'code', 'description', 'sort_order'),
    'size_range':    ('mw_dim_size_range',    'range_code', 'description', 'sort_order'),
    'unit_type':     ('mw_dim_unit_type',     'code', 'description', 'sort_order'),
    'climate_type':  ('mw_dim_climate_type',  'code', 'description', 'sort_order'),
    'unit_shape':    ('mw_dim_unit_shape',    'code', 'description', 'sort_order'),
    'pillar':        ('mw_dim_pillar',        'code', 'description', 'sort_order'),
}


@discount_plans_bp.route('/api/dim-options')
@login_required
def api_dim_options():
    """Return the canonical option lists for every SOP COM01 dim.

    Shape: {"size_category": [{code, description}, ...], ...}
    """
    from sqlalchemy import text as sqltext
    session = get_session()
    try:
        out: dict = {}
        for field, (table, code_col, desc_col, sort_col) in _DIM_TABLE.items():
            rows = session.execute(sqltext(
                f'SELECT "{code_col}" AS code, "{desc_col}" AS description '
                f'FROM {table} ORDER BY "{sort_col}"'
            )).mappings().all()
            out[field] = [{'code': r['code'], 'description': r['description']} for r in rows]
        return jsonify(out)
    finally:
        session.close()


@discount_plans_bp.route('/api/unit-count', methods=['POST'])
@login_required
@rate_limit_api(max_requests=60, window_seconds=60)
def api_unit_count():
    """Count vacant units matching the given site + restriction combo.

    Body JSON: {"site_codes": [...], "restrictions": {dim: [codes]}}.
    Returns: {"count": N, "by_site": {"L001": n, ...}, "parse_fail": N}.

    Reads `ccws_available_units` (vacant units only — rented units aren't
    impacted by plan-level restrictions) and applies the sTypeName parser
    per row. Restrictions with empty/missing value lists are ignored.
    """
    from sqlalchemy import text as sqltext
    from common.stype_name_parser import parse_stype_name

    body = request.get_json(silent=True) or {}
    site_codes = body.get('site_codes') or []
    restrictions = body.get('restrictions') or {}
    if not isinstance(site_codes, list) or not site_codes:
        return jsonify({'count': 0, 'by_site': {}, 'parse_fail': 0, 'note': 'no sites'})

    # Only keep known dim keys + string lists.
    clean: dict = {}
    for field in _DIM_FIELDS:
        vals = restrictions.get(field)
        if isinstance(vals, list):
            s = [str(v).strip() for v in vals if str(v).strip()]
            if s:
                clean[field] = set(s)

    # Optional wine case-count window. Either bound may be unset.
    def _to_int(x):
        try:
            return int(x) if x is not None and str(x).strip() != '' else None
        except (TypeError, ValueError):
            return None
    min_cases = _to_int(restrictions.get('min_case_count'))
    max_cases = _to_int(restrictions.get('max_case_count'))

    session = get_session()
    try:
        rows = session.execute(sqltext("""
            SELECT u."sLocationCode" AS site_code, u."sTypeName" AS stype_name
            FROM ccws_available_units u
            WHERE u."sLocationCode" = ANY(:codes)
        """), {'codes': site_codes}).mappings().all()

        # Legacy fallback map — only loaded when needed.
        legacy_map = _load_legacy_type_map_cached() if clean else {}

        by_site: dict = {code: 0 for code in site_codes}
        parse_fail = 0
        legacy_mapped = 0
        total = 0
        for r in rows:
            parts = parse_stype_name(r['stype_name'])
            if not parts.parse_ok:
                parse_fail += 1
                # Legacy fallback: map unit_type + climate_type when SOP failed.
                if legacy_map:
                    hit = legacy_map.get((r['stype_name'] or '').strip())
                    if hit:
                        from dataclasses import replace as _dc_replace
                        parts = _dc_replace(parts, unit_type=hit[0], climate_type=hit[1])
                        legacy_mapped += 1
            # Apply each active restriction. A row passes only if its parsed
            # value is IN the selected set for every restricted dim.
            passes = True
            for field, allowed in clean.items():
                val = getattr(parts, field, None)
                if val is None or val not in allowed:
                    passes = False
                    break
            # Apply case-count range when set. Units with no case_count
            # (= non-wine) are dropped if either bound is set.
            if passes and (min_cases is not None or max_cases is not None):
                cc = parts.case_count
                if cc is None:
                    passes = False
                elif min_cases is not None and cc < min_cases:
                    passes = False
                elif max_cases is not None and cc > max_cases:
                    passes = False
            if passes:
                total += 1
                by_site[r['site_code']] = by_site.get(r['site_code'], 0) + 1

        return jsonify({
            'count': total,
            'by_site': by_site,
            'parse_fail': parse_fail,
            'legacy_mapped': legacy_mapped,
            'sites_queried': len(site_codes),
        })
    finally:
        session.close()


@discount_plans_bp.route('/ai-extract', methods=['GET', 'POST'])
@login_required
@_require_config_permission
def ai_extract():
    """Paste promo document → LLM returns N candidate plans."""
    if request.method == 'GET':
        return render_template('admin/discount_plans/ai_extract.html')

    pasted = (request.form.get('pasted_text') or '').strip()
    if not pasted:
        flash('Paste the promo brief text before extracting.', 'error')
        return render_template('admin/discount_plans/ai_extract.html')

    from web.utils.promo_extractor import extract_plans
    try:
        candidates = extract_plans(pasted)
    except Exception as e:
        current_app.logger.error(f"AI extract failed: {e}")
        flash('AI extraction failed. See server logs.', 'error')
        return render_template('admin/discount_plans/ai_extract.html',
                               pasted_text=pasted)

    if not candidates:
        flash('The LLM returned zero plans. Check the paste and try again.', 'error')
        return render_template('admin/discount_plans/ai_extract.html',
                               pasted_text=pasted)

    # Enrich each candidate with a preview of the resolved fields.
    for c in candidates:
        site_codes = (
            _site_codes_for_country(c.get('country') or '') if c.get('country') else []
        )
        c['_applicable_site_codes'] = site_codes
        c['_matched_concessions'] = (
            _resolve_sitelink_name_to_concessions(c.get('sitelink_plan_name') or '', site_codes)
            if c.get('sitelink_plan_name') and site_codes else []
        )

    return render_template('admin/discount_plans/ai_picker.html',
                           candidates=candidates, pasted_text=pasted)


def _build_draft_plan_from_ai_candidate(db_session, data: dict):
    """Build + persist one inactive DiscountPlan from an AI candidate dict.

    Handles unique-name collision, country → applicable_sites, and
    sitelink_plan_name → linked_concessions resolution. Returns the saved
    plan on success, or raises on failure (caller manages the transaction).
    """
    from web.models.discount_plan import DiscountPlan

    base_name = (data.get('plan_name') or 'AI Draft').strip() or 'AI Draft'
    candidate_name = base_name
    suffix = 2
    while db_session.query(DiscountPlan).filter_by(plan_name=candidate_name).first():
        candidate_name = f"{base_name} ({suffix})"
        suffix += 1
        if suffix > 50:
            raise RuntimeError(f"could not find a unique name for '{base_name}'")

    plan = DiscountPlan(
        plan_name=candidate_name,
        plan_type=data.get('plan_type') or 'Tactical',
        group_name=(data.get('group_name') or '').strip() or None,
        objective=data.get('objective') or None,
        storage_type=data.get('storage_type') or None,
        promo_period_start=_parse_date_field(data.get('promo_period_start')),
        promo_period_end=_parse_date_field(data.get('promo_period_end')),
        booking_period_start=_parse_date_field(data.get('booking_period_start')),
        booking_period_end=_parse_date_field(data.get('booking_period_end')),
        discount_type=data.get('discount_type') or None,
        discount_numeric=data.get('discount_numeric'),
        discount_segmentation=_derive_discount_segmentation(
            data.get('discount_type'), data.get('discount_numeric'),
        ),
        payment_terms=data.get('payment_terms') or None,
        deposit=data.get('deposit') or None,
        lock_in_period=data.get('lock_in_period') or None,
        distribution_channel=data.get('distribution_channel') or None,
        hidden_rate=bool(data.get('hidden_rate')),
        coupon_code=((data.get('coupon_code') or '').strip().upper() or None),
        switch_to_us=data.get('switch_to_us') or 'Not Eligible',
        referral_program=data.get('referral_program') or 'Not Eligible',
        terms_conditions=data.get('terms_conditions') or None,
        tc_labels=data.get('tc_labels') or None,
        rate_rules=data.get('rate_rules') or None,
        rate_rules_sites=data.get('rate_rules_sites') or None,
        notes=data.get('notes') or None,
        is_active=False,
        created_by=current_user.username,
        updated_by=current_user.username,
    )

    country = (data.get('country') or '').strip().upper()
    applicable: dict = {}
    site_codes_for_plan: list[str] = []
    if country:
        site_codes_for_plan = _site_codes_for_country(country)
        for code in site_codes_for_plan:
            applicable[code] = True
    plan.applicable_sites = applicable

    sitelink_name = (data.get('sitelink_plan_name') or '').strip()
    if sitelink_name and site_codes_for_plan:
        plan.linked_concessions = _resolve_sitelink_name_to_concessions(
            sitelink_name, site_codes_for_plan,
        )
    else:
        plan.linked_concessions = []

    db_session.add(plan)
    return plan


@discount_plans_bp.route('/ai-create', methods=['POST'])
@login_required
@_require_config_permission
def ai_create():
    """Create an inactive draft plan from a single AI candidate and open it."""
    raw_payload = request.form.get('payload', '').strip()
    if not raw_payload:
        flash('Missing candidate payload.', 'error')
        return redirect(url_for('discount_plans.ai_extract'))

    try:
        data = json.loads(raw_payload)
    except (json.JSONDecodeError, TypeError):
        flash('Invalid candidate payload.', 'error')
        return redirect(url_for('discount_plans.ai_extract'))

    db_session = get_session()
    try:
        plan = _build_draft_plan_from_ai_candidate(db_session, data)
        db_session.commit()
        audit_log(
            AuditEvent.CONFIG_UPDATED,
            f"AI-drafted discount plan '{plan.plan_name}' (id={plan.id})",
        )
        _trigger_candidates_refresh(_site_codes_from_plan(plan))
        flash(f'Draft "{plan.plan_name}" created. Review and save to activate.', 'success')
        return redirect(url_for('discount_plans.edit_plan', plan_id=plan.id))
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error creating AI draft plan: {e}")
        flash('Could not create draft. Check server logs.', 'error')
        return redirect(url_for('discount_plans.ai_extract'))
    finally:
        db_session.close()


@discount_plans_bp.route('/ai-create-all', methods=['POST'])
@login_required
@_require_config_permission
def ai_create_all():
    """Bulk-create inactive draft plans for every AI candidate.

    Body form field `payloads` holds a JSON array of candidate dicts. Each
    successful creation lands in the list page with a summary flash; failed
    rows are logged but don't block the batch.
    """
    raw_payloads = request.form.get('payloads', '').strip()
    if not raw_payloads:
        flash('Missing candidate payloads.', 'error')
        return redirect(url_for('discount_plans.ai_extract'))

    try:
        candidates = json.loads(raw_payloads)
    except (json.JSONDecodeError, TypeError):
        flash('Invalid candidate payloads.', 'error')
        return redirect(url_for('discount_plans.ai_extract'))

    if not isinstance(candidates, list) or not candidates:
        flash('No candidates to create.', 'error')
        return redirect(url_for('discount_plans.ai_extract'))

    created: list[str] = []
    failed: list[str] = []
    affected_sites: set = set()
    db_session = get_session()
    try:
        for i, data in enumerate(candidates, start=1):
            if not isinstance(data, dict):
                failed.append(f'#{i} (not a dict)')
                continue
            try:
                plan = _build_draft_plan_from_ai_candidate(db_session, data)
                db_session.flush()  # get id + surface UNIQUE conflicts inline
                created.append(plan.plan_name)
                affected_sites.update(_site_codes_from_plan(plan))
            except Exception as e:
                db_session.rollback()
                current_app.logger.error(
                    f"ai_create_all: candidate #{i} failed: {e}"
                )
                failed.append(f'#{i} {(data.get("plan_name") or "unnamed")}: {e}')
        if created:
            db_session.commit()
            _trigger_candidates_refresh(sorted(affected_sites))
            audit_log(
                AuditEvent.CONFIG_UPDATED,
                f"AI-drafted {len(created)} discount plans: {', '.join(created[:10])}"
                + (f" …(+{len(created) - 10})" if len(created) > 10 else '')
            )
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"ai_create_all transaction failed: {e}")
        flash('Batch-create transaction failed. Check server logs.', 'error')
        return redirect(url_for('discount_plans.ai_extract'))
    finally:
        db_session.close()

    if created and not failed:
        flash(f'Created {len(created)} draft(s): {", ".join(created)}. All inactive — review and activate.', 'success')
    elif created and failed:
        flash(f'Created {len(created)} draft(s); {len(failed)} failed. See server logs for details.', 'info')
    else:
        flash(f'No drafts created — {len(failed)} failed.', 'error')
    return redirect(url_for('discount_plans.list_plans'))


@discount_plans_bp.route('/<int:plan_id>/duplicate', methods=['POST'])
@login_required
@_require_config_permission
def duplicate_plan(plan_id):
    """Duplicate an existing plan as an inactive draft and open it for editing.

    Copies every column except id/created/updated bookkeeping. Name becomes
    "Copy of <original>" (with a numeric suffix if that's already taken).
    """
    from web.models.discount_plan import DiscountPlan
    from sqlalchemy import inspect as sa_inspect

    db_session = get_session()
    try:
        orig = db_session.query(DiscountPlan).get(plan_id)
        if not orig:
            flash('Discount plan not found.', 'error')
            return redirect(url_for('discount_plans.list_plans'))

        # Build a fresh name that doesn't collide with the unique index.
        base_name = f"Copy of {orig.plan_name}"
        candidate = base_name
        suffix = 2
        while db_session.query(DiscountPlan).filter_by(plan_name=candidate).first():
            candidate = f"{base_name} ({suffix})"
            suffix += 1
            if suffix > 50:
                flash('Could not find a free name for the copy.', 'error')
                return redirect(url_for('discount_plans.list_plans'))

        # Shallow-copy all mapped columns except the primary key and audit fields.
        skip = {'id', 'created_at', 'updated_at', 'created_by', 'updated_by', 'plan_name'}
        data = {}
        for col in sa_inspect(DiscountPlan).c.keys():
            if col in skip:
                continue
            data[col] = getattr(orig, col)

        copy = DiscountPlan(plan_name=candidate, **data)
        copy.is_active = False  # drafts default inactive
        copy.created_by = current_user.username
        copy.updated_by = current_user.username

        db_session.add(copy)
        db_session.commit()

        audit_log(
            AuditEvent.CONFIG_UPDATED,
            f"Duplicated discount plan '{orig.plan_name}' (id={plan_id}) → '{candidate}' (id={copy.id})",
        )
        # Duplicate is inactive by default → no candidates produced until
        # user activates and saves. Skip the refresh here.
        flash(f'Plan duplicated as "{candidate}" (inactive). Review and save to activate.', 'success')
        return redirect(url_for('discount_plans.edit_plan', plan_id=copy.id))
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error duplicating discount plan: {e}")
        flash('Could not duplicate plan. Check server logs.', 'error')
        return redirect(url_for('discount_plans.list_plans'))
    finally:
        db_session.close()


@discount_plans_bp.route('/<int:plan_id>/delete', methods=['POST'])
@login_required
@_require_config_permission
def delete_plan(plan_id):
    """Delete a discount plan."""
    from web.models.discount_plan import DiscountPlan

    db_session = get_session()
    try:
        plan = db_session.query(DiscountPlan).get(plan_id)
        if plan:
            name = plan.plan_name
            sites_to_refresh = _site_codes_from_plan(plan)
            db_session.delete(plan)
            db_session.commit()
            audit_log(AuditEvent.CONFIG_UPDATED, f"Deleted discount plan '{name}' (id={plan_id})")
            _trigger_candidates_refresh(sites_to_refresh)
            flash(f'Discount plan "{name}" deleted.', 'success')
        else:
            flash('Discount plan not found.', 'error')
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error deleting discount plan: {e}")
        flash('An error occurred.', 'error')
    finally:
        db_session.close()

    return redirect(url_for('discount_plans.list_plans'))


# =============================================================================
# Promotion Brief View (presentable read-only view)
# =============================================================================

@discount_plans_bp.route('/<int:plan_id>/brief')
@login_required
def view_brief(plan_id):
    """Show the promotion brief view for a discount plan."""
    from web.models.discount_plan import DiscountPlan

    sites_by_country, all_site_codes = _get_sites_by_country()
    # Build a flat code->name lookup for the brief
    site_name_map = {}
    for country_sites in sites_by_country.values():
        for s in country_sites:
            site_name_map[s['code']] = s['name']

    db_session = get_session()
    try:
        plan = db_session.query(DiscountPlan).get(plan_id)
        if not plan:
            flash('Discount plan not found.', 'error')
            return redirect(url_for('discount_plans.list_plans'))

        # Resolve linked concession details from PBI DB
        linked_details = []
        if plan.is_stdrate_override:
            # Standard Rate: synthesize one row per applicable site, no ccws lookup.
            applicable_codes_iter = sorted(
                (c for c, v in (plan.applicable_sites or {}).items() if v)
            )
            for code in applicable_codes_iter:
                linked_details.append({
                    'site_id': None,
                    'site_code': code,
                    'site_name': site_name_map.get(code, code),
                    'concession_id': 0,
                    'plan_name': 'Standard Rate (no concession)',
                    'discount_pct': None,
                    'start': None,
                    'end': None,
                })
        elif plan.linked_concessions:
            try:
                from common.models import CcwsDiscount, Site, SiteInfo
                pbi_session = _get_pbi_session()
                try:
                    # Collect all site IDs for batch lookup
                    link_site_ids = list({link.get('site_id') for link in plan.linked_concessions if link.get('site_id')})
                    # Build SiteID -> SiteCode map (prefer SiteInfo for standardized L001 format)
                    sid_to_code = {}
                    if link_site_ids:
                        info_rows = pbi_session.query(SiteInfo.SiteID, SiteInfo.SiteCode).filter(SiteInfo.SiteID.in_(link_site_ids)).all()
                        sid_to_code = {si.SiteID: si.SiteCode for si in info_rows if si.SiteCode}
                        missing = [sid for sid in link_site_ids if sid not in sid_to_code]
                        if missing:
                            site_rows = pbi_session.query(Site.SiteID, Site.sLocationCode).filter(Site.SiteID.in_(missing)).all()
                            for s in site_rows:
                                if s.sLocationCode:
                                    sid_to_code[s.SiteID] = s.sLocationCode

                    for link in plan.linked_concessions:
                        cc = (pbi_session.query(CcwsDiscount)
                              .filter_by(SiteID=link.get('site_id'), ConcessionID=link.get('concession_id'))
                              .first())
                        if cc:
                            sc = link.get('site_code') or sid_to_code.get(cc.SiteID, f'L{str(cc.SiteID).zfill(3)}')
                            linked_details.append({
                                'site_id': cc.SiteID,
                                'site_code': sc,
                                'site_name': site_name_map.get(sc, f'Site {cc.SiteID}'),
                                'concession_id': cc.ConcessionID,
                                'plan_name': cc.sPlanName or cc.sDefPlanName or '-',
                                'discount_pct': float(cc.dcPCDiscount) if cc.dcPCDiscount else None,
                                'start': cc.dPlanStrt.strftime('%Y-%m-%d') if cc.dPlanStrt else None,
                                'end': cc.dPlanEnd.strftime('%Y-%m-%d') if cc.dPlanEnd else None,
                            })
                finally:
                    pbi_session.close()
            except Exception as e:
                current_app.logger.warning(f"Could not resolve linked concessions: {e}")

        # Build comparison: applicable sites vs sitelink sites
        applicable_codes = set((plan.applicable_sites or {}).keys())
        sitelink_codes = {ld['site_code'] for ld in linked_details if ld.get('site_code')}
        comparison = {
            'matched': sorted(applicable_codes & sitelink_codes),
            'gaps': sorted(applicable_codes - sitelink_codes),
            'extras': sorted(sitelink_codes - applicable_codes),
        }

        return render_template('admin/discount_plans/brief.html',
                               plan=plan,
                               sites_by_country=sites_by_country,
                               site_codes=all_site_codes,
                               site_name_map=site_name_map,
                               linked_details=linked_details,
                               comparison=comparison)
    finally:
        db_session.close()


# =============================================================================
# JSON API endpoints (for programmatic access)
# =============================================================================

@discount_plans_bp.route('/api/list')
@login_required
def api_list():
    """JSON list of all plans (for AJAX use)."""
    from web.models.discount_plan import DiscountPlan

    db_session = get_session()
    try:
        plans = (db_session.query(DiscountPlan)
                 .order_by(asc(DiscountPlan.sort_order), asc(DiscountPlan.plan_name))
                 .all())
        return jsonify([p.to_dict() for p in plans])
    finally:
        db_session.close()


@discount_plans_bp.route('/api/<int:plan_id>')
@login_required
def api_get(plan_id):
    """Get a single plan as JSON."""
    from web.models.discount_plan import DiscountPlan

    db_session = get_session()
    try:
        plan = db_session.query(DiscountPlan).get(plan_id)
        if not plan:
            return jsonify({'error': 'Not found'}), 404
        return jsonify(plan.to_dict())
    finally:
        db_session.close()


# =============================================================================
# Sitelink Concession Search (for autocomplete in edit form)
# =============================================================================

@discount_plans_bp.route('/api/concessions/search')
@login_required
@rate_limit_api(max_requests=30, window_seconds=60)
def api_search_concessions():
    """
    Search ccws_discount entries by plan name.
    Query params: q (search text), site_id (optional), site_code (optional).
    Reads middleware — PBI's weekly ccws_discount sync was decommissioned.
    """
    q = request.args.get('q', '').strip()
    site_id = request.args.get('site_id', '').strip()
    site_code = request.args.get('site_code', '').strip()
    site_codes_raw = request.args.get('site_codes', '').strip()
    site_codes = [c.strip() for c in site_codes_raw.split(',') if c.strip()] if site_codes_raw else []

    if len(q) < 2 and not site_id and not site_code and not site_codes:
        return jsonify([])

    from sqlalchemy import text as sqltext
    from datetime import datetime
    try:
        session = get_session()
        try:
            clauses = [
                '"dDeleted" IS NULL',
                '"dDisabled" IS NULL',
                '"dArchived" IS NULL',
                '("bNeverExpires" = TRUE OR "dPlanEnd" IS NULL OR "dPlanEnd" >= :now)',
            ]
            params = {'now': datetime.utcnow()}

            if q:
                clauses.append('"sPlanName" ILIKE :q')
                params['q'] = f'%{q}%'
            if site_id:
                if not site_id.isdigit():
                    return jsonify({'error': 'Invalid site_id'}), 400
                clauses.append('"SiteID" = :sid')
                params['sid'] = int(site_id)
            if site_code:
                clauses.append('"SiteID" IN (SELECT "SiteID" FROM mw_siteinfo WHERE "SiteCode" = :scode)')
                params['scode'] = site_code
            if site_codes:
                clauses.append('"SiteID" IN (SELECT "SiteID" FROM mw_siteinfo WHERE "SiteCode" = ANY(:scodes))')
                params['scodes'] = site_codes

            sql = sqltext(f"""
                SELECT "ConcessionID", "SiteID", "sPlanName", "sDefPlanName",
                       "dcPCDiscount", "dPlanStrt", "dPlanEnd"
                FROM ccws_discount
                WHERE {' AND '.join(clauses)}
                ORDER BY "SiteID", "sPlanName"
                LIMIT 50
            """)
            rows = session.execute(sql, params).mappings().all()

            site_ids = sorted({r['SiteID'] for r in rows})
            site_map: dict = {}
            if site_ids:
                info_rows = session.execute(sqltext("""
                    SELECT "SiteID", "SiteCode", "Name"
                    FROM mw_siteinfo
                    WHERE "SiteID" = ANY(:sids)
                """), {'sids': site_ids}).mappings().all()
                for s in info_rows:
                    site_map[s['SiteID']] = {
                        'name': s.get('Name') or f"Site {s['SiteID']}",
                        'code': s.get('SiteCode') or '',
                    }

            return jsonify([{
                'concession_id': r['ConcessionID'],
                'site_id': r['SiteID'],
                'site_name': site_map.get(r['SiteID'], {}).get('name', f"Site {r['SiteID']}"),
                'site_code': site_map.get(r['SiteID'], {}).get('code', ''),
                'plan_name': r['sPlanName'] or r['sDefPlanName'] or '-',
                'discount_pct': float(r['dcPCDiscount']) if r['dcPCDiscount'] is not None else None,
                'start': r['dPlanStrt'].strftime('%Y-%m-%d') if r['dPlanStrt'] else None,
                'end': r['dPlanEnd'].strftime('%Y-%m-%d') if r['dPlanEnd'] else None,
            } for r in rows])
        finally:
            session.close()
    except Exception as e:
        current_app.logger.error(f"Concession search error: {e}")
        return jsonify({'error': 'Concession search failed'}), 500


@discount_plans_bp.route('/api/concessions/by-plan-name')
@login_required
@rate_limit_api(max_requests=30, window_seconds=60)
def api_concessions_by_plan_name():
    """
    Get all concessions matching an exact Sitelink plan name.
    Query params: name (exact plan name).
    """
    name = request.args.get('name', '').strip()
    if not name:
        return jsonify([])

    try:
        from common.models import CcwsDiscount, Site
        pbi_session = _get_pbi_session()
        try:
            from sqlalchemy import or_
            from datetime import datetime
            now = datetime.utcnow()
            results = (pbi_session.query(
                CcwsDiscount.ConcessionID, CcwsDiscount.SiteID,
                CcwsDiscount.sPlanName, CcwsDiscount.sDefPlanName,
                CcwsDiscount.dcPCDiscount, CcwsDiscount.dPlanStrt, CcwsDiscount.dPlanEnd,
            )
            .filter(CcwsDiscount.sPlanName == name)
            .filter(CcwsDiscount.dDeleted.is_(None))
            .filter(CcwsDiscount.dDisabled.is_(None))
            .filter(CcwsDiscount.dArchived.is_(None))
            .filter(or_(
                CcwsDiscount.dPlanEnd.is_(None),
                CcwsDiscount.bNeverExpires.is_(True),
                CcwsDiscount.dPlanEnd >= now,
            ))
            .order_by(CcwsDiscount.SiteID)
            .all())

            # Get site names + site codes via SiteInfo (preferred) and Site (fallback)
            site_ids = list({r.SiteID for r in results})
            site_map = {}
            if site_ids:
                from common.models import SiteInfo
                # SiteInfo.SiteCode is the standardized L001 format used by form checkboxes
                infos = pbi_session.query(SiteInfo.SiteID, SiteInfo.SiteCode, SiteInfo.Name).filter(SiteInfo.SiteID.in_(site_ids)).all()
                for si in infos:
                    site_map[si.SiteID] = {'name': si.Name or f'Site {si.SiteID}', 'code': si.SiteCode}
                # Fallback to Site table for any SiteIDs not in SiteInfo
                missing = [sid for sid in site_ids if sid not in site_map]
                if missing:
                    sites = pbi_session.query(Site.SiteID, Site.sSiteName, Site.sLocationCode).filter(Site.SiteID.in_(missing)).all()
                    for s in sites:
                        site_map[s.SiteID] = {'name': s.sSiteName or f'Site {s.SiteID}', 'code': s.sLocationCode}

            return jsonify([{
                'concession_id': r.ConcessionID,
                'site_id': r.SiteID,
                'site_name': site_map.get(r.SiteID, {}).get('name', f'Site {r.SiteID}'),
                'site_code': site_map.get(r.SiteID, {}).get('code', ''),
                'plan_name': r.sPlanName or r.sDefPlanName or '-',
                'discount_pct': float(r.dcPCDiscount) if r.dcPCDiscount else None,
                'start': r.dPlanStrt.strftime('%Y-%m-%d') if r.dPlanStrt else None,
                'end': r.dPlanEnd.strftime('%Y-%m-%d') if r.dPlanEnd else None,
            } for r in results])
        finally:
            pbi_session.close()
    except Exception as e:
        current_app.logger.error(f"Concessions by plan name error: {e}")
        return jsonify({'error': 'Failed to fetch concessions'}), 500


@discount_plans_bp.route('/api/concessions/by-ids')
@login_required
@rate_limit_api(max_requests=30, window_seconds=60)
def api_concessions_by_ids():
    """Hydrate linked_concessions entries with site_code + sPlanName.

    Query param `items` is a comma-separated list of "site_id:concession_id"
    pairs (e.g. `100:900,101:500`). Reads middleware `ccws_discount` +
    `mw_siteinfo`. Used by the edit page to render existing links with
    readable labels even when the stored JSONB predates the upgraded
    picker format.
    """
    items_raw = request.args.get('items', '').strip()
    if not items_raw:
        return jsonify([])

    pairs: list[tuple[int, int]] = []
    for token in items_raw.split(','):
        token = token.strip()
        if not token or ':' not in token:
            continue
        sid_s, cid_s = token.split(':', 1)
        if not sid_s.isdigit() or not cid_s.isdigit():
            continue
        pairs.append((int(sid_s), int(cid_s)))

    if not pairs:
        return jsonify([])

    if len(pairs) > 500:
        return jsonify({'error': 'Too many items (max 500)'}), 400

    try:
        from sqlalchemy import text
        session = get_session()
        try:
            site_ids = list({sid for sid, _ in pairs})
            conc_ids = list({cid for _, cid in pairs})
            rows = session.execute(text("""
                SELECT "SiteID", "ConcessionID", "sPlanName", "sDefPlanName",
                       "dcPCDiscount", "dPlanStrt", "dPlanEnd"
                FROM ccws_discount
                WHERE "SiteID" = ANY(:sids)
                  AND "ConcessionID" = ANY(:cids)
            """), {'sids': site_ids, 'cids': conc_ids}).mappings().all()

            # site_code lookup via mw_siteinfo
            sites = session.execute(text("""
                SELECT "SiteID", "SiteCode"
                FROM mw_siteinfo
                WHERE "SiteID" = ANY(:sids)
            """), {'sids': site_ids}).mappings().all()
            site_code_map = {s['SiteID']: s['SiteCode'] for s in sites}

            by_key = {(r['SiteID'], r['ConcessionID']): r for r in rows}

            out = []
            for sid, cid in pairs:
                r = by_key.get((sid, cid))
                if r is None:
                    # Concession not found — still return the pair so the UI
                    # shows a placeholder rather than dropping the selection.
                    out.append({
                        'site_id': sid,
                        'concession_id': cid,
                        'site_code': site_code_map.get(sid, ''),
                        'plan_name': '(concession not found)',
                        'discount_pct': None,
                        'start': None,
                        'end': None,
                    })
                    continue
                out.append({
                    'site_id': sid,
                    'concession_id': cid,
                    'site_code': site_code_map.get(sid, ''),
                    'plan_name': r['sPlanName'] or r['sDefPlanName'] or '-',
                    'discount_pct': float(r['dcPCDiscount']) if r['dcPCDiscount'] else None,
                    'start': r['dPlanStrt'].strftime('%Y-%m-%d') if r['dPlanStrt'] else None,
                    'end': r['dPlanEnd'].strftime('%Y-%m-%d') if r['dPlanEnd'] else None,
                })
            return jsonify(out)
        finally:
            session.close()
    except Exception as e:
        current_app.logger.error(f"Concessions by-ids error: {e}")
        return jsonify({'error': 'Failed to fetch concessions'}), 500


# =============================================================================
# AI Translation endpoint
# =============================================================================

@discount_plans_bp.route('/<int:plan_id>/translate', methods=['POST'])
@login_required
@_require_config_permission
def translate_tcs(plan_id):
    """Translate T&Cs between languages via Azure OpenAI."""
    from web.models.discount_plan import DiscountPlan

    db_session = get_session()
    try:
        plan = db_session.query(DiscountPlan).get(plan_id)
        if not plan:
            return jsonify({'error': 'Plan not found'}), 404

        # Accept source/target language params from JSON body
        from web.utils.translation import ALL_LANGUAGES
        VALID_LANG_CODES = set(ALL_LANGUAGES.keys())

        body = request.get_json(silent=True) or {}
        source_lang = body.get('source_lang', 'en')
        target_langs = body.get('target_langs')  # list or None for all

        if source_lang not in VALID_LANG_CODES:
            return jsonify({'error': 'Invalid source language'}), 400
        if target_langs is not None:
            if not isinstance(target_langs, list):
                return jsonify({'error': 'target_langs must be a list'}), 400
            if not all(lang in VALID_LANG_CODES for lang in target_langs):
                return jsonify({'error': 'Invalid target language code'}), 400

        tcs = plan.terms_conditions
        if not tcs:
            return jsonify({'error': 'No T&Cs to translate'}), 400

        try:
            from web.utils.translation import translate_terms_all_languages
            new_translations = translate_terms_all_languages(tcs, source_lang=source_lang, target_langs=target_langs)
        except Exception as e:
            current_app.logger.error(f"Translation error: {e}")
            return jsonify({'error': 'Translation service unavailable'}), 500

        # Merge new translations into existing (preserve unrelated languages)
        existing = plan.terms_conditions_translations or {}
        existing.update(new_translations)
        plan.terms_conditions_translations = existing
        plan.updated_by = current_user.username
        db_session.commit()

        return jsonify({
            'success': True,
            'translations': existing,
        })
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Translation save error: {e}")
        return jsonify({'error': 'Failed to save translations'}), 500
    finally:
        db_session.close()


# =============================================================================
# Config CRUD
# =============================================================================

@discount_plans_bp.route('/config')
@login_required
@_require_config_permission
def config_page():
    """Manage translatable dropdown options for discount plan fields."""
    from web.models.discount_plan_config import DiscountPlanConfig

    db_session = get_session()
    try:
        all_options = (db_session.query(DiscountPlanConfig)
                       .order_by(DiscountPlanConfig.field_name, DiscountPlanConfig.sort_order)
                       .all())
        grouped = {}
        for opt in all_options:
            grouped.setdefault(opt.field_name, []).append(opt)
        return render_template('admin/discount_plans/config.html',
                               grouped=grouped,
                               field_names=DiscountPlanConfig.FIELD_NAMES)
    finally:
        db_session.close()


@discount_plans_bp.route('/config/save', methods=['POST'])
@login_required
@_require_config_permission
def config_save():
    """Create or update a config option."""
    from web.models.discount_plan_config import DiscountPlanConfig

    opt_id = request.form.get('id', '').strip()
    field_name = request.form.get('field_name', '').strip()
    option_value = request.form.get('option_value', '').strip()
    sort_order = request.form.get('sort_order', '0').strip()
    is_active = request.form.get('is_active') == 'on'

    if not field_name or not option_value:
        flash('Field name and option value are required.', 'error')
        return redirect(url_for('discount_plans.config_page'))

    if field_name not in DiscountPlanConfig.FIELD_NAMES:
        flash('Invalid field name.', 'error')
        return redirect(url_for('discount_plans.config_page'))

    # Build translations dict from form
    translations = {}
    for lc in ('ko', 'zh_cn', 'zh_tw', 'ms', 'ja'):
        val = request.form.get(f'trans_{lc}', '').strip()
        if val:
            translations[lc] = val

    db_session = get_session()
    try:
        if opt_id:
            opt = db_session.query(DiscountPlanConfig).get(int(opt_id))
            if not opt:
                flash('Config option not found.', 'error')
                return redirect(url_for('discount_plans.config_page'))
            opt.field_name = field_name
            opt.option_value = option_value
            opt.translations = translations
            opt.sort_order = int(sort_order) if sort_order.isdigit() else 0
            opt.is_active = is_active
        else:
            opt = DiscountPlanConfig(
                field_name=field_name,
                option_value=option_value,
                translations=translations,
                sort_order=int(sort_order) if sort_order.isdigit() else 0,
                is_active=is_active,
            )
            db_session.add(opt)

        db_session.commit()
        audit_log(AuditEvent.CONFIG_UPDATED, f"Saved discount plan config: {field_name}={option_value}")
        flash(f'Option "{option_value}" saved.', 'success')
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Config save error: {e}")
        flash('An error occurred.', 'error')
    finally:
        db_session.close()

    return redirect(url_for('discount_plans.config_page'))


@discount_plans_bp.route('/config/<int:opt_id>/delete', methods=['POST'])
@login_required
@_require_config_permission
def config_delete(opt_id):
    """Delete a config option."""
    from web.models.discount_plan_config import DiscountPlanConfig

    db_session = get_session()
    try:
        opt = db_session.query(DiscountPlanConfig).get(opt_id)
        if opt:
            label = f"{opt.field_name}={opt.option_value}"
            db_session.delete(opt)
            db_session.commit()
            audit_log(AuditEvent.CONFIG_UPDATED, f"Deleted discount plan config: {label}")
            flash(f'Option deleted.', 'success')
        else:
            flash('Config option not found.', 'error')
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Config delete error: {e}")
        flash('An error occurred.', 'error')
    finally:
        db_session.close()

    return redirect(url_for('discount_plans.config_page'))


@discount_plans_bp.route('/api/translate-text', methods=['POST'])
@login_required
@_require_config_permission
def api_translate_text():
    """Translate a single text string to all target languages in one API call."""
    from web.utils.translation import ALL_LANGUAGES, translate_single_text_all

    body = request.get_json(silent=True) or {}
    text = body.get('text', '').strip()
    source_lang = body.get('source_lang', 'en')

    VALID_LANG_CODES = set(ALL_LANGUAGES.keys())
    if not text:
        return jsonify({'error': 'No text provided'}), 400
    if len(text) > 500:
        return jsonify({'error': 'Text too long (max 500 characters)'}), 400
    if source_lang not in VALID_LANG_CODES:
        return jsonify({'error': 'Invalid source language'}), 400

    try:
        translations = translate_single_text_all(text, source_lang)
    except Exception as e:
        current_app.logger.error(f"Config translate failed: {e}")
        return jsonify({'error': 'Translation service error'}), 500

    return jsonify({'success': True, 'translations': translations})


@discount_plans_bp.route('/api/config-options/<field_name>')
@login_required
def api_config_options(field_name):
    """JSON API: get active options for a specific field."""
    from web.models.discount_plan_config import DiscountPlanConfig

    if field_name not in DiscountPlanConfig.FIELD_NAMES:
        return jsonify({'error': 'Invalid field name'}), 400

    db_session = get_session()
    try:
        options = (db_session.query(DiscountPlanConfig)
                   .filter_by(field_name=field_name, is_active=True)
                   .order_by(DiscountPlanConfig.sort_order)
                   .all())
        return jsonify([o.to_dict() for o in options])
    finally:
        db_session.close()
