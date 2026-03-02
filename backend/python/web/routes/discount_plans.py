"""
Discount Plans routes - CRUD management and promotion brief view.
"""

import json
from datetime import date
from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import asc, desc
from web.utils.audit import audit_log, AuditEvent

discount_plans_bp = Blueprint('discount_plans', __name__, url_prefix='/discount-plans')


def get_session():
    """Get database session from app context."""
    return current_app.get_db_session()


def _get_pbi_session():
    """Get PBI database session for Site queries."""
    from common.config_loader import get_database_url
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    pbi_url = get_database_url('pbi')
    engine = create_engine(pbi_url)
    Session = sessionmaker(bind=engine)
    return Session()


def _get_sites_by_country():
    """
    Query Site table from PBI DB. Returns:
    - sites_by_country: {'Hong Kong': [{'code': 'L004', 'name': 'Yau Tong'}, ...], ...}
    - all_site_codes: ['L001', 'L003', ...] (for backward compat)
    """
    try:
        from common.models import Site
        session = _get_pbi_session()
        try:
            sites = (session.query(Site.sLocationCode, Site.sSiteName, Site.sSiteCountry)
                     .filter(Site.sLocationCode.isnot(None))
                     .order_by(Site.sLocationCode)
                     .all())
            by_country = {}
            all_codes = []
            for code, name, country in sites:
                if not code:
                    continue
                country = country or 'Unknown'
                if country not in by_country:
                    by_country[country] = []
                by_country[country].append({'code': code, 'name': name or code})
                all_codes.append(code)
            return by_country, all_codes
        finally:
            session.close()
    except Exception as e:
        current_app.logger.warning(f"Could not load sites from PBI DB: {e}")
        # Fallback to hardcoded list
        fallback = ['L001', 'L003', 'L004', 'L005', 'L006', 'L007', 'L008', 'L009', 'L010']
        return {'Sites': [{'code': c, 'name': c} for c in fallback]}, fallback


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


def _build_plan_from_form(form, plan=None):
    """Extract discount plan fields from the submitted form."""
    from web.models.discount_plan import DiscountPlan

    if plan is None:
        plan = DiscountPlan()

    # Identification
    plan.plan_type = form.get('plan_type', '').strip()
    plan.plan_name = form.get('plan_name', '').strip()
    plan.sitelink_discount_name = form.get('sitelink_discount_name', '').strip() or None

    # Description
    plan.notes = form.get('notes', '').strip() or None
    plan.objective = form.get('objective', '').strip() or None

    # Availability
    plan.period_range = form.get('period_range', '').strip() or None
    plan.period_start = _parse_date_field(form.get('period_start'))
    plan.period_end = _parse_date_field(form.get('period_end'))
    plan.move_in_range = form.get('move_in_range', '').strip() or None

    # Lock-in period
    plan.lock_in_period = form.get('lock_in_period', '').strip() or None

    # Sites - build from checkboxes (dynamic codes from form)
    site_codes = form.getlist('site_code')
    sites = {}
    for code in site_codes:
        sites[code] = True
    plan.applicable_sites = sites

    # Discount details
    plan.discount_value = form.get('discount_value', '').strip() or None
    plan.discount_type = form.get('discount_type', '').strip() or None
    raw_numeric = form.get('discount_numeric', '').strip()
    plan.discount_numeric = float(raw_numeric) if raw_numeric else None
    plan.discount_segmentation = form.get('discount_segmentation', '').strip() or None
    plan.clawback_condition = form.get('clawback_condition', '').strip() or None

    # Offers (JSON)
    plan.offers = _parse_json_field(form.get('offers_json'), [])

    # Terms
    plan.deposit = form.get('deposit', '').strip() or None
    plan.payment_terms = form.get('payment_terms', '').strip() or None
    plan.termination_notice = form.get('termination_notice', '').strip() or None
    plan.extra_offer = form.get('extra_offer', '').strip() or None

    # T&Cs - dynamic array-style fields
    tcs_en_raw = form.getlist('tc_en')
    tcs_cn_raw = form.getlist('tc_cn')
    tcs_en = [t.strip() for t in tcs_en_raw if t.strip()]
    tcs_cn = [t.strip() for t in tcs_cn_raw if t.strip()]
    plan.terms_conditions = tcs_en if tcs_en else None
    plan.terms_conditions_cn = tcs_cn if tcs_cn else None

    # Promotion brief fields
    plan.hidden_rate = form.get('hidden_rate') == 'on'
    plan.available_for_chatbot = form.get('available_for_chatbot') == 'on'
    plan.chatbot_notes = form.get('chatbot_notes', '').strip() or None
    plan.sales_extra_discount = form.get('sales_extra_discount', 'Not Eligible')
    plan.switch_to_us = form.get('switch_to_us', 'Not Eligible')
    plan.referral_program = form.get('referral_program', 'Not Eligible')
    plan.distribution_channel = form.get('distribution_channel', '').strip() or None

    # Departmental
    plan.rate_rules = form.get('rate_rules', '').strip() or None
    plan.rate_rules_sites = form.get('rate_rules_sites', '').strip() or None
    plan.promotion_codes = _parse_json_field(form.get('promotion_codes_json'), [])
    plan.collateral_url = form.get('collateral_url', '').strip() or None
    plan.registration_flow = form.get('registration_flow', '').strip() or None
    plan.department_notes = _parse_json_field(form.get('department_notes_json'), {})

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

    sites_by_country, all_site_codes = _get_sites_by_country()
    tpl_kwargs = dict(
        sites_by_country=sites_by_country, site_codes=all_site_codes,
        plan_types=PLAN_TYPES, discount_types=DISCOUNT_TYPES,
        eligibility_options=ELIGIBILITY_OPTIONS,
    )

    if request.method == 'POST':
        db_session = get_session()
        try:
            plan = _build_plan_from_form(request.form)
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

    sites_by_country, all_site_codes = _get_sites_by_country()
    tpl_kwargs = dict(
        sites_by_country=sites_by_country, site_codes=all_site_codes,
        plan_types=PLAN_TYPES, discount_types=DISCOUNT_TYPES,
        eligibility_options=ELIGIBILITY_OPTIONS,
    )

    db_session = get_session()
    try:
        plan = db_session.query(DiscountPlan).get(plan_id)
        if not plan:
            flash('Discount plan not found.', 'error')
            return redirect(url_for('discount_plans.list_plans'))

        if request.method == 'POST':
            old_name = plan.plan_name
            _build_plan_from_form(request.form, plan)
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
                from common.models import CCDiscount
                pbi_session = _get_pbi_session()
                try:
                    for link in plan.linked_concessions:
                        cc = (pbi_session.query(CCDiscount)
                              .filter_by(SiteID=link.get('site_id'), ConcessionID=link.get('concession_id'))
                              .first())
                        if cc:
                            linked_details.append({
                                'site_id': cc.SiteID,
                                'site_name': site_name_map.get(f'L{str(cc.SiteID).zfill(3)}', f'Site {cc.SiteID}'),
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

        return render_template('admin/discount_plans/brief.html',
                               plan=plan,
                               sites_by_country=sites_by_country,
                               site_codes=all_site_codes,
                               site_name_map=site_name_map,
                               linked_details=linked_details)
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
def api_search_concessions():
    """
    Search cc_discount entries by plan name.
    Query params: q (search text), site_id (optional filter).
    """
    q = request.args.get('q', '').strip()
    site_id = request.args.get('site_id', '').strip()

    if len(q) < 2 and not site_id:
        return jsonify([])

    try:
        from common.models import CCDiscount, Site
        pbi_session = _get_pbi_session()
        try:
            query = pbi_session.query(
                CCDiscount.ConcessionID, CCDiscount.SiteID,
                CCDiscount.sPlanName, CCDiscount.sDefPlanName,
                CCDiscount.dcPCDiscount, CCDiscount.dPlanStrt, CCDiscount.dPlanEnd,
            )
            if q:
                query = query.filter(CCDiscount.sPlanName.ilike(f'%{q}%'))
            if site_id:
                query = query.filter(CCDiscount.SiteID == int(site_id))
            query = query.filter(CCDiscount.dDeleted.is_(None))
            results = query.order_by(CCDiscount.SiteID, CCDiscount.sPlanName).limit(50).all()

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


# =============================================================================
# AI Translation endpoint
# =============================================================================

@discount_plans_bp.route('/<int:plan_id>/translate', methods=['POST'])
@login_required
@_require_config_permission
def translate_tcs(plan_id):
    """Translate English T&Cs to multiple languages via Azure OpenAI."""
    from web.models.discount_plan import DiscountPlan

    db_session = get_session()
    try:
        plan = db_session.query(DiscountPlan).get(plan_id)
        if not plan:
            return jsonify({'error': 'Plan not found'}), 404

        tcs = plan.terms_conditions
        if not tcs:
            return jsonify({'error': 'No English T&Cs to translate'}), 400

        try:
            from web.utils.translation import translate_terms_all_languages
            translations = translate_terms_all_languages(tcs)
        except Exception as e:
            current_app.logger.error(f"Translation error: {e}")
            return jsonify({'error': 'Translation service unavailable'}), 500

        plan.terms_conditions_translations = translations
        plan.updated_by = current_user.username
        db_session.commit()

        return jsonify({
            'success': True,
            'translations': translations,
        })
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Translation save error: {e}")
        return jsonify({'error': 'Failed to save translations'}), 500
    finally:
        db_session.close()
