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
    """Get database session from app context."""
    return current_app.get_db_session()


_pbi_engine = None


def _get_pbi_engine():
    global _pbi_engine
    if _pbi_engine is None:
        from common.config_loader import get_database_url
        from sqlalchemy import create_engine
        pbi_url = get_database_url('pbi')
        _pbi_engine = create_engine(pbi_url, pool_size=5, max_overflow=10)
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


def _get_sitelink_discount_names():
    """Get distinct Sitelink discount plan names from ccws_discount (PBI DB).
    Only returns active plans (not deleted/disabled/archived) with valid periods.
    """
    try:
        from common.models import CcwsDiscount
        from sqlalchemy import distinct, or_
        from datetime import datetime
        session = _get_pbi_session()
        try:
            now = datetime.utcnow()
            rows = (session.query(distinct(CcwsDiscount.sPlanName))
                    .filter(CcwsDiscount.sPlanName.isnot(None))
                    .filter(CcwsDiscount.dDeleted.is_(None))
                    .filter(CcwsDiscount.dDisabled.is_(None))
                    .filter(CcwsDiscount.dArchived.is_(None))
                    .filter(or_(
                        CcwsDiscount.dPlanEnd.is_(None),
                        CcwsDiscount.bNeverExpires.is_(True),
                        CcwsDiscount.dPlanEnd >= now,
                    ))
                    .order_by(CcwsDiscount.sPlanName)
                    .all())
            return [r[0] for r in rows if r[0] and r[0].strip()]
        finally:
            session.close()
    except Exception as e:
        current_app.logger.warning(f"Could not load sitelink discount names: {e}")
        return []


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
    plan.sitelink_discount_name = form.get('sitelink_discount_name', '').strip() or None

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
    plan.discount_value = form.get('discount_value', '').strip() or None
    plan.discount_type = form.get('discount_type', '').strip() or None
    raw_numeric = form.get('discount_numeric', '').strip()
    try:
        plan.discount_numeric = float(raw_numeric) if raw_numeric else None
    except ValueError:
        plan.discount_numeric = None
    plan.discount_segmentation = _validate_config_value(form.get('discount_segmentation', '').strip(), 'discount_segmentation')
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
    plan.available_for_chatbot = form.get('available_for_chatbot') == 'on'
    plan.chatbot_notes = form.get('chatbot_notes', '').strip() or None
    plan.switch_to_us = _validate_config_value(form.get('switch_to_us', '').strip(), 'switch_to_us') or 'Not Eligible'
    plan.referral_program = _validate_config_value(form.get('referral_program', '').strip(), 'referral_program') or 'Not Eligible'
    # Distribution channel (multi-choice checkboxes, stored comma-separated, validated against config)
    allowed_channels = set(config_options.get('distribution_channel', []))
    dist_channels = form.getlist('distribution_channel')
    valid_channels = [c.strip() for c in dist_channels if c.strip() and (not allowed_channels or c.strip() in allowed_channels)]
    plan.distribution_channel = ', '.join(valid_channels) or None

    # Custom fields - dynamic key/value pairs from the form
    cf_keys = form.getlist('cf_key')
    cf_vals = form.getlist('cf_value')
    custom = {}
    for k, v in zip(cf_keys, cf_vals):
        k = k.strip()
        v = v.strip()
        if k:
            custom[k] = v
    plan.custom_fields = custom if custom else {}

    # Linked Sitelink concessions
    plan.linked_concessions = _parse_json_field(form.get('linked_concessions_json'), [])

    # Status
    plan.is_active = form.get('is_active') == 'on'
    raw_sort = form.get('sort_order', '0').strip()
    plan.sort_order = int(raw_sort) if raw_sort.isdigit() else 0

    return plan


def _edit_tpl_kwargs():
    """Build common template kwargs for create/edit pages."""
    sites_by_country, all_site_codes = _get_sites_by_country()
    return dict(
        sites_by_country=sites_by_country, site_codes=all_site_codes,
        plan_types=PLAN_TYPES, discount_types=DISCOUNT_TYPES,
        eligibility_options=ELIGIBILITY_OPTIONS,
        sitelink_discount_names=_get_sitelink_discount_names(),
        config_options=_get_config_options(),
    )


# =============================================================================
# List
# =============================================================================

@discount_plans_bp.route('/')
@login_required
def list_plans():
    """List all discount plans."""
    from web.models.discount_plan import DiscountPlan

    sites_by_country, all_site_codes = _get_sites_by_country()
    db_session = get_session()
    try:
        plans = (db_session.query(DiscountPlan)
                 .order_by(asc(DiscountPlan.sort_order), asc(DiscountPlan.plan_name))
                 .all())
        return render_template('admin/discount_plans/list.html',
                               plans=plans,
                               sites_by_country=sites_by_country,
                               site_codes=all_site_codes)
    finally:
        db_session.close()


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
            flash(f'Discount plan "{plan.plan_name}" created.', 'success')
            return redirect(url_for('discount_plans.list_plans'))
        except Exception as e:
            db_session.rollback()
            current_app.logger.error(f"Error creating discount plan: {e}")
            flash('An error occurred while creating the plan.', 'error')
        finally:
            db_session.close()

    return render_template('admin/discount_plans/edit.html',
                           plan=None, form_data={}, **tpl_kwargs)


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
            flash('Discount plan updated.', 'success')
            return redirect(url_for('discount_plans.list_plans'))

        return render_template('admin/discount_plans/edit.html',
                               plan=plan, form_data={}, **tpl_kwargs)
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
            db_session.delete(plan)
            db_session.commit()
            audit_log(AuditEvent.CONFIG_UPDATED, f"Deleted discount plan '{name}' (id={plan_id})")
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
        if plan.linked_concessions:
            try:
                from common.models import CcwsDiscount, Site, SiteInfo
                pbi_session = _get_pbi_session()
                try:
                    # Collect all site IDs for batch lookup
                    link_site_ids = list({link.get('site_id') for link in plan.linked_concessions if link.get('site_id')})
                    # Build SiteID -> SiteCode map
                    sid_to_code = {}
                    if link_site_ids:
                        site_rows = pbi_session.query(Site.SiteID, Site.sLocationCode).filter(Site.SiteID.in_(link_site_ids)).all()
                        sid_to_code = {s.SiteID: s.sLocationCode for s in site_rows if s.sLocationCode}
                        missing = [sid for sid in link_site_ids if sid not in sid_to_code]
                        if missing:
                            info_rows = pbi_session.query(SiteInfo.SiteID, SiteInfo.SiteCode).filter(SiteInfo.SiteID.in_(missing)).all()
                            for si in info_rows:
                                sid_to_code[si.SiteID] = si.SiteCode

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
    Query params: q (search text), site_id (optional filter).
    """
    q = request.args.get('q', '').strip()
    site_id = request.args.get('site_id', '').strip()

    if len(q) < 2 and not site_id:
        return jsonify([])

    try:
        from common.models import CcwsDiscount, Site
        pbi_session = _get_pbi_session()
        try:
            query = pbi_session.query(
                CcwsDiscount.ConcessionID, CcwsDiscount.SiteID,
                CcwsDiscount.sPlanName, CcwsDiscount.sDefPlanName,
                CcwsDiscount.dcPCDiscount, CcwsDiscount.dPlanStrt, CcwsDiscount.dPlanEnd,
            )
            if q:
                query = query.filter(CcwsDiscount.sPlanName.ilike(f'%{q}%'))
            if site_id:
                if not site_id.isdigit():
                    return jsonify({'error': 'Invalid site_id'}), 400
                query = query.filter(CcwsDiscount.SiteID == int(site_id))
            from sqlalchemy import or_
            from datetime import datetime
            now = datetime.utcnow()
            query = (query
                .filter(CcwsDiscount.dDeleted.is_(None))
                .filter(CcwsDiscount.dDisabled.is_(None))
                .filter(CcwsDiscount.dArchived.is_(None))
                .filter(or_(
                    CcwsDiscount.dPlanEnd.is_(None),
                    CcwsDiscount.bNeverExpires.is_(True),
                    CcwsDiscount.dPlanEnd >= now,
                )))
            results = query.order_by(CcwsDiscount.SiteID, CcwsDiscount.sPlanName).limit(50).all()

            # Get site names
            site_ids = list({r.SiteID for r in results})
            site_map = {}
            if site_ids:
                sites = pbi_session.query(Site.SiteID, Site.sSiteName, Site.sLocationCode).filter(Site.SiteID.in_(site_ids)).all()
                site_map = {s.SiteID: {'name': s.sSiteName, 'code': s.sLocationCode} for s in sites}

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

            # Get site names + site codes via Site and SiteInfo
            site_ids = list({r.SiteID for r in results})
            site_map = {}
            if site_ids:
                from common.models import SiteInfo
                sites = pbi_session.query(Site.SiteID, Site.sSiteName, Site.sLocationCode).filter(Site.SiteID.in_(site_ids)).all()
                site_map = {s.SiteID: {'name': s.sSiteName, 'code': s.sLocationCode} for s in sites}
                # Fallback: if sLocationCode is missing, try SiteInfo.SiteCode
                missing = [sid for sid in site_ids if not site_map.get(sid, {}).get('code')]
                if missing:
                    infos = pbi_session.query(SiteInfo.SiteID, SiteInfo.SiteCode).filter(SiteInfo.SiteID.in_(missing)).all()
                    for si in infos:
                        if si.SiteID in site_map:
                            site_map[si.SiteID]['code'] = si.SiteCode
                        else:
                            site_map[si.SiteID] = {'name': f'Site {si.SiteID}', 'code': si.SiteCode}

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
