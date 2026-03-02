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


# Known site codes for the checkbox grid
SITE_CODES = ['L001', 'L003', 'L004', 'L005', 'L006', 'L007', 'L008', 'L009', 'L010']

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

    # Sites - build from checkboxes
    sites = {}
    for code in SITE_CODES:
        sites[code] = form.get(f'site_{code}') == 'on'
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

    # T&Cs - collect from numbered textarea fields
    tcs_en = []
    for i in range(1, 9):
        tc = form.get(f'tc_{i}', '').strip()
        if tc:
            tcs_en.append(tc)
    plan.terms_conditions = tcs_en if tcs_en else None

    tcs_cn = []
    for i in range(1, 9):
        tc = form.get(f'tc_cn_{i}', '').strip()
        if tc:
            tcs_cn.append(tc)
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

    db_session = get_session()
    try:
        plans = (db_session.query(DiscountPlan)
                 .order_by(asc(DiscountPlan.sort_order), asc(DiscountPlan.plan_name))
                 .all())
        return render_template('admin/discount_plans/list.html',
                               plans=plans,
                               site_codes=SITE_CODES)
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

    if request.method == 'POST':
        db_session = get_session()
        try:
            plan = _build_plan_from_form(request.form)
            plan.created_by = current_user.username

            if not plan.plan_type or not plan.plan_name:
                flash('Plan Type and Plan Name are required.', 'error')
                return render_template('admin/discount_plans/edit.html',
                                       plan=None, form_data=request.form,
                                       site_codes=SITE_CODES, plan_types=PLAN_TYPES,
                                       discount_types=DISCOUNT_TYPES,
                                       eligibility_options=ELIGIBILITY_OPTIONS)

            existing = db_session.query(DiscountPlan).filter_by(plan_name=plan.plan_name).first()
            if existing:
                flash('A plan with this name already exists.', 'error')
                return render_template('admin/discount_plans/edit.html',
                                       plan=None, form_data=request.form,
                                       site_codes=SITE_CODES, plan_types=PLAN_TYPES,
                                       discount_types=DISCOUNT_TYPES,
                                       eligibility_options=ELIGIBILITY_OPTIONS)

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
                           plan=None, form_data={},
                           site_codes=SITE_CODES, plan_types=PLAN_TYPES,
                           discount_types=DISCOUNT_TYPES,
                           eligibility_options=ELIGIBILITY_OPTIONS)


# =============================================================================
# Edit
# =============================================================================

@discount_plans_bp.route('/<int:plan_id>/edit', methods=['GET', 'POST'])
@login_required
@_require_config_permission
def edit_plan(plan_id):
    """Edit an existing discount plan."""
    from web.models.discount_plan import DiscountPlan

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
                                       plan=plan, form_data=request.form,
                                       site_codes=SITE_CODES, plan_types=PLAN_TYPES,
                                       discount_types=DISCOUNT_TYPES,
                                       eligibility_options=ELIGIBILITY_OPTIONS)

            # Check uniqueness if name changed
            if plan.plan_name != old_name:
                existing = db_session.query(DiscountPlan).filter_by(plan_name=plan.plan_name).first()
                if existing and existing.id != plan_id:
                    flash('A plan with this name already exists.', 'error')
                    return render_template('admin/discount_plans/edit.html',
                                           plan=plan, form_data=request.form,
                                           site_codes=SITE_CODES, plan_types=PLAN_TYPES,
                                           discount_types=DISCOUNT_TYPES,
                                           eligibility_options=ELIGIBILITY_OPTIONS)

            db_session.commit()
            audit_log(AuditEvent.CONFIG_UPDATED, f"Updated discount plan '{plan.plan_name}' (id={plan_id})")
            flash('Discount plan updated.', 'success')
            return redirect(url_for('discount_plans.list_plans'))

        return render_template('admin/discount_plans/edit.html',
                               plan=plan, form_data={},
                               site_codes=SITE_CODES, plan_types=PLAN_TYPES,
                               discount_types=DISCOUNT_TYPES,
                               eligibility_options=ELIGIBILITY_OPTIONS)
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

    db_session = get_session()
    try:
        plan = db_session.query(DiscountPlan).get(plan_id)
        if not plan:
            flash('Discount plan not found.', 'error')
            return redirect(url_for('discount_plans.list_plans'))

        return render_template('admin/discount_plans/brief.html',
                               plan=plan, site_codes=SITE_CODES)
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
