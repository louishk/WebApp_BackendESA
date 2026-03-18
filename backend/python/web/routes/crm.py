"""
CRM API routes — SugarCRM lead management for walk-in/visit workflows.

Internal API for web UI staff (session auth, not JWT).
Exposes lead search, create, get, and update operations.
"""

import logging
import re

from flask import Blueprint, jsonify, request, current_app
from flask_login import current_user, login_required

from web.auth.decorators import inventory_tools_access_required
from web.utils.rate_limit import rate_limit_api
from web.utils.audit import audit_log, AuditEvent

logger = logging.getLogger(__name__)

crm_bp = Blueprint('crm', __name__, url_prefix='/api/crm')

# Lazy-initialized SugarCRM client (shared across requests within a worker)
_sugar_client = None


def _get_sugar_client():
    """Get or create SugarCRM client instance."""
    global _sugar_client
    if _sugar_client is None:
        from common.sugarcrm_client import SugarCRMClient
        _sugar_client = SugarCRMClient.from_env()
    return _sugar_client


# Fields to return from lead queries (limit data exposure)
LEAD_FIELDS = [
    'id', 'first_name', 'last_name', 'full_name',
    'phone_mobile', 'phone_work', 'phone_home',
    'email', 'webtolead_email1',
    'status', 'lead_source', 'description',
    'primary_address_street', 'primary_address_city',
    'primary_address_country',
    'date_entered', 'date_modified',
    'assigned_user_name',
]

# Allowed fields for lead update (whitelist to prevent overwriting sensitive fields)
UPDATABLE_LEAD_FIELDS = {
    'first_name', 'last_name',
    'phone_mobile', 'phone_work', 'phone_home',
    'email', 'webtolead_email1',
    'status', 'lead_source', 'description',
    'primary_address_street', 'primary_address_city',
    'primary_address_country',
}

# Valid UUID pattern for SugarCRM IDs
_UUID_PATTERN = re.compile(r'^[a-f0-9\-]{36}$', re.IGNORECASE)


def _validate_uuid(lead_id):
    """Validate that a string looks like a SugarCRM UUID."""
    return bool(_UUID_PATTERN.match(lead_id))


def _sanitize_phone(phone):
    """Strip non-digit chars except + for country code."""
    if not phone:
        return phone
    return re.sub(r'[^\d+]', '', phone.strip())


# =============================================================================
# Routes
# =============================================================================

@crm_bp.route('/leads/search', methods=['GET'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=30, window_seconds=60)
def search_leads():
    """Search leads by mobile, email, or name."""
    q = request.args.get('q', '').strip()
    if not q or len(q) < 2:
        return jsonify({'error': 'Search query must be at least 2 characters'}), 400

    if len(q) > 100:
        return jsonify({'error': 'Search query too long'}), 400

    try:
        client = _get_sugar_client()

        # Build OR filter across phone, email, and name
        or_conditions = []

        # Phone search (strip formatting)
        cleaned = _sanitize_phone(q)
        if cleaned and len(cleaned) >= 4:
            or_conditions.append({'phone_mobile': {'$contains': cleaned}})
            or_conditions.append({'phone_work': {'$contains': cleaned}})

        # Email search
        if '@' in q:
            or_conditions.append({'email': {'$contains': q}})
            or_conditions.append({'webtolead_email1': {'$contains': q}})

        # Name search (always included)
        or_conditions.append({'first_name': {'$contains': q}})
        or_conditions.append({'last_name': {'$contains': q}})

        filter_expr = [{'$or': or_conditions}]

        result, error = client.filter_records(
            module='Leads',
            filter_expr=filter_expr,
            fields=LEAD_FIELDS,
            max_num=20,
            order_by='date_modified:DESC',
        )

        if error:
            logger.error("CRM lead search failed: %s", error)
            return jsonify({'error': 'Failed to search leads'}), 502

        leads = result.get('records', [])

        audit_log(
            AuditEvent.CRM_LEAD_SEARCHED,
            f"Searched leads: q='{q}', results={len(leads)}",
        )

        return jsonify({'status': 'success', 'data': leads})

    except Exception:
        logger.exception("CRM lead search error")
        return jsonify({'error': 'Failed to search leads'}), 500


@crm_bp.route('/leads', methods=['POST'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=10, window_seconds=60)
def create_lead():
    """Create a new lead in SugarCRM."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    first_name = (data.get('first_name') or '').strip()
    last_name = (data.get('last_name') or '').strip()
    phone_mobile = _sanitize_phone(data.get('phone_mobile'))
    email = (data.get('email') or '').strip().lower()

    # Validation
    if not last_name:
        return jsonify({'error': 'Last name is required'}), 400

    if not phone_mobile and not email:
        return jsonify({'error': 'Either phone or email is required'}), 400

    if email and not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
        return jsonify({'error': 'Invalid email format'}), 400

    if phone_mobile and len(phone_mobile) < 6:
        return jsonify({'error': 'Phone number too short'}), 400

    try:
        client = _get_sugar_client()

        fields = {
            'first_name': first_name,
            'last_name': last_name,
            'status': 'New',
            'lead_source': 'Walk-in',
        }
        if phone_mobile:
            fields['phone_mobile'] = phone_mobile
        if email:
            fields['email'] = [{'email_address': email, 'primary_address': True}]

        record, error = client.create_record('Leads', fields)
        if error:
            logger.error("CRM lead create failed: %s", error)
            return jsonify({'error': 'Failed to create lead'}), 502

        audit_log(
            AuditEvent.CRM_LEAD_CREATED,
            f"Created lead: id={record.get('id')}, name={first_name} {last_name}",
        )

        return jsonify({'status': 'success', 'data': record}), 201

    except Exception:
        logger.exception("CRM lead create error")
        return jsonify({'error': 'Failed to create lead'}), 500


@crm_bp.route('/leads/<lead_id>', methods=['GET'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=30, window_seconds=60)
def get_lead(lead_id):
    """Get a single lead by SugarCRM UUID."""
    if not _validate_uuid(lead_id):
        return jsonify({'error': 'Invalid lead ID format'}), 400

    try:
        client = _get_sugar_client()

        record, error = client.get_record('Leads', lead_id, fields=LEAD_FIELDS)
        if error:
            logger.error("CRM lead get failed: %s", error)
            if '404' in str(error):
                return jsonify({'error': 'Lead not found'}), 404
            return jsonify({'error': 'Failed to retrieve lead'}), 502

        return jsonify({'status': 'success', 'data': record})

    except Exception:
        logger.exception("CRM lead get error")
        return jsonify({'error': 'Failed to retrieve lead'}), 500


@crm_bp.route('/leads/<lead_id>', methods=['PATCH'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=10, window_seconds=60)
def update_lead(lead_id):
    """Update lead fields (status, notes, etc.)."""
    if not _validate_uuid(lead_id):
        return jsonify({'error': 'Invalid lead ID format'}), 400

    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    # Whitelist updatable fields
    update_fields = {}
    for key, value in data.items():
        if key in UPDATABLE_LEAD_FIELDS:
            if isinstance(value, str):
                value = value.strip()
            update_fields[key] = value

    if not update_fields:
        return jsonify({'error': 'No valid fields to update'}), 400

    try:
        client = _get_sugar_client()

        record, error = client.update_record('Leads', lead_id, update_fields)
        if error:
            logger.error("CRM lead update failed: %s", error)
            if '404' in str(error):
                return jsonify({'error': 'Lead not found'}), 404
            return jsonify({'error': 'Failed to update lead'}), 502

        audit_log(
            AuditEvent.CRM_LEAD_UPDATED,
            f"Updated lead: id={lead_id}, fields={list(update_fields.keys())}",
        )

        return jsonify({'status': 'success', 'data': record})

    except Exception:
        logger.exception("CRM lead update error")
        return jsonify({'error': 'Failed to update lead'}), 500
