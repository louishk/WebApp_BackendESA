"""
CRM API routes — SugarCRM lead management for walk-in/visit workflows.

Internal API for web UI staff (session auth, not JWT).
Exposes lead search, create, get, and update operations.
"""

import logging
import re
import threading

from flask import Blueprint, jsonify, request, current_app
from flask_login import current_user, login_required

from web.auth.decorators import inventory_tools_access_required
from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api
from web.utils.audit import audit_log, AuditEvent

logger = logging.getLogger(__name__)

crm_bp = Blueprint('crm', __name__, url_prefix='/api/crm')

# Lazy-initialized SugarCRM client (shared across requests within a worker)
_sugar_client = None
_sugar_client_lock = threading.Lock()


def _get_sugar_client():
    """Get or create SugarCRM client instance (thread-safe)."""
    global _sugar_client
    if _sugar_client is None:
        with _sugar_client_lock:
            if _sugar_client is None:
                from common.sugarcrm_client import SugarCRMClient
                _sugar_client = SugarCRMClient.from_env()
    return _sugar_client


# Fields to return from lead queries (limit data exposure)
LEAD_FIELDS = [
    'id', 'salutation', 'first_name', 'last_name', 'full_name',
    'phone_mobile', 'phone_work', 'phone_home',
    'email', 'webtolead_email1',
    'status', 'lead_source', 'description',
    'primary_address_street', 'primary_address_city',
    'primary_address_country',
    'date_entered', 'date_modified',
    'assigned_user_name',
    # ESA custom fields
    'es_storage_type_c', 'es_storage_duration_c',
    'es_storage_size_c', 'es_storage_location_c',
    'tbot_note_c', 'ai_inferred_type_of_goods_c',
]

# Allowed fields for lead update (whitelist to prevent overwriting sensitive fields)
UPDATABLE_LEAD_FIELDS = {
    'first_name', 'last_name',
    'phone_mobile', 'phone_work', 'phone_home',
    'email', 'webtolead_email1',
    'status', 'lead_source', 'description',
    'primary_address_street', 'primary_address_city',
    'primary_address_country',
    # ESA custom fields
    'es_storage_type_c', 'es_storage_duration_c',
    'es_storage_size_c', 'es_storage_location_c',
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

        # ESA custom fields — pass through if provided
        for es_field in ('es_storage_type_c', 'es_storage_duration_c',
                         'es_storage_size_c', 'es_storage_location_c'):
            val = (data.get(es_field) or '').strip()
            if val:
                fields[es_field] = val

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


# Fields returned from contact/account queries
CONTACT_FIELDS = [
    'id', 'salutation', 'first_name', 'last_name', 'full_name',
    'phone_mobile', 'phone_work', 'phone_home',
    'email', 'title', 'department',
    'primary_address_street', 'primary_address_city',
    'primary_address_country',
    'account_id', 'account_name',
    'date_entered', 'date_modified',
    'assigned_user_name',
]

ACCOUNT_FIELDS = [
    'id', 'name', 'phone_office', 'email',
    'billing_address_street', 'billing_address_city',
    'billing_address_country',
    'date_entered', 'date_modified',
    'assigned_user_name',
]

UPDATABLE_CONTACT_FIELDS = {
    'first_name', 'last_name', 'salutation',
    'phone_mobile', 'phone_work', 'phone_home',
    'email', 'title', 'department',
    'primary_address_street', 'primary_address_city',
    'primary_address_country',
}

UPDATABLE_ACCOUNT_FIELDS = {
    'name', 'phone_office', 'email',
    'billing_address_street', 'billing_address_city',
    'billing_address_country',
}

# Allowed fields for Calls module creation
CALL_FIELDS_ALLOWED = {
    'name', 'description', 'status', 'direction',
    'date_start', 'duration_hours', 'duration_minutes',
    'parent_type', 'parent_id', 'assigned_user_id',
}

VALID_PARENT_TYPES = {'Leads', 'Contacts', 'Accounts'}


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


# =============================================================================
# Call logging
# =============================================================================

@crm_bp.route('/calls', methods=['POST'])
@require_auth
@require_api_scope('crm:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def create_call():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    if not data.get('name', '').strip():
        return jsonify({'error': 'name is required'}), 400

    parent_type = data.get('parent_type')
    if parent_type and parent_type not in VALID_PARENT_TYPES:
        return jsonify({'error': f'parent_type must be one of: {", ".join(VALID_PARENT_TYPES)}'}), 400

    parent_id = data.get('parent_id')
    if parent_id and not _validate_uuid(parent_id):
        return jsonify({'error': 'Invalid parent_id format'}), 400

    fields = {}
    for key, value in data.items():
        if key in CALL_FIELDS_ALLOWED:
            fields[key] = value.strip() if isinstance(value, str) else value

    try:
        client = _get_sugar_client()

        record, error = client.create_record('Calls', fields)
        if error:
            logger.error("CRM call create failed: %s", error)
            return jsonify({'error': 'Failed to create call record'}), 502

        audit_log(
            AuditEvent.CRM_CALL_CREATED,
            f"Created call: id={record.get('id')}, name={fields.get('name')}",
        )

        return jsonify({'status': 'success', 'data': record}), 201

    except Exception:
        logger.exception("CRM call create error")
        return jsonify({'error': 'Failed to create call record'}), 500


# =============================================================================
# Lead conversion
# =============================================================================

@crm_bp.route('/leads/<lead_id>/convert', methods=['POST'])
@require_auth
@require_api_scope('crm:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def convert_lead(lead_id):
    if not _validate_uuid(lead_id):
        return jsonify({'error': 'Invalid lead ID format'}), 400

    data = request.get_json(silent=True) or {}
    contact_data = data.get('contact_data') or None
    account_data = data.get('account_data') or None

    if contact_data is not None and not isinstance(contact_data, dict):
        return jsonify({'error': 'contact_data must be an object'}), 400
    if account_data is not None and not isinstance(account_data, dict):
        return jsonify({'error': 'account_data must be an object'}), 400

    try:
        client = _get_sugar_client()

        record, error = client.convert_lead(lead_id, contact_data=contact_data, account_data=account_data)
        if error:
            logger.error("CRM lead convert failed: %s", error)
            if '404' in str(error):
                return jsonify({'error': 'Lead not found'}), 404
            return jsonify({'error': 'Failed to convert lead'}), 502

        audit_log(
            AuditEvent.CRM_LEAD_CONVERTED,
            f"Converted lead: id={lead_id}",
        )

        # Extract contact/account IDs from the response if SugarCRM returns them
        response_data = {
            'lead': record,
            'contact_id': (record or {}).get('contact_id'),
            'account_id': (record or {}).get('account_id'),
        }

        return jsonify({'status': 'success', 'data': response_data})

    except Exception:
        logger.exception("CRM lead convert error")
        return jsonify({'error': 'Failed to convert lead'}), 500


# =============================================================================
# Contacts
# =============================================================================

@crm_bp.route('/contacts/<contact_id>', methods=['GET'])
@require_auth
@require_api_scope('crm:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def get_contact(contact_id):
    if not _validate_uuid(contact_id):
        return jsonify({'error': 'Invalid contact ID format'}), 400

    try:
        client = _get_sugar_client()

        record, error = client.get_record('Contacts', contact_id, fields=CONTACT_FIELDS)
        if error:
            logger.error("CRM contact get failed: %s", error)
            if '404' in str(error):
                return jsonify({'error': 'Contact not found'}), 404
            return jsonify({'error': 'Failed to retrieve contact'}), 502

        return jsonify({'status': 'success', 'data': record})

    except Exception:
        logger.exception("CRM contact get error")
        return jsonify({'error': 'Failed to retrieve contact'}), 500


@crm_bp.route('/contacts/search', methods=['GET'])
@require_auth
@require_api_scope('crm:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def search_contacts():
    email = request.args.get('email', '').strip().lower()
    if not email:
        return jsonify({'error': 'email query parameter is required'}), 400

    if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
        return jsonify({'error': 'Invalid email format'}), 400

    try:
        client = _get_sugar_client()

        filter_expr = [{'email': {'$contains': email}}]
        result, error = client.filter_records(
            module='Contacts',
            filter_expr=filter_expr,
            fields=CONTACT_FIELDS,
            max_num=20,
            order_by='date_modified:DESC',
        )

        if error:
            logger.error("CRM contact search failed: %s", error)
            return jsonify({'error': 'Failed to search contacts'}), 502

        return jsonify({'status': 'success', 'data': result.get('records', [])})

    except Exception:
        logger.exception("CRM contact search error")
        return jsonify({'error': 'Failed to search contacts'}), 500


@crm_bp.route('/contacts', methods=['POST'])
@require_auth
@require_api_scope('crm:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def create_contact():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    last_name = (data.get('last_name') or '').strip()
    if not last_name:
        return jsonify({'error': 'last_name is required'}), 400

    fields = {}
    for key, value in data.items():
        if key in UPDATABLE_CONTACT_FIELDS:
            fields[key] = value.strip() if isinstance(value, str) else value

    if not fields:
        return jsonify({'error': 'No valid fields provided'}), 400

    try:
        client = _get_sugar_client()

        record, error = client.create_record('Contacts', fields)
        if error:
            logger.error("CRM contact create failed: %s", error)
            return jsonify({'error': 'Failed to create contact'}), 502

        audit_log(
            AuditEvent.CRM_CONTACT_CREATED,
            f"Created contact: id={record.get('id')}, name={fields.get('first_name', '')} {last_name}",
        )

        return jsonify({'status': 'success', 'data': record}), 201

    except Exception:
        logger.exception("CRM contact create error")
        return jsonify({'error': 'Failed to create contact'}), 500


@crm_bp.route('/contacts/<contact_id>', methods=['PATCH'])
@require_auth
@require_api_scope('crm:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def update_contact(contact_id):
    if not _validate_uuid(contact_id):
        return jsonify({'error': 'Invalid contact ID format'}), 400

    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    update_fields = {}
    for key, value in data.items():
        if key in UPDATABLE_CONTACT_FIELDS:
            update_fields[key] = value.strip() if isinstance(value, str) else value

    if not update_fields:
        return jsonify({'error': 'No valid fields to update'}), 400

    try:
        client = _get_sugar_client()

        record, error = client.update_record('Contacts', contact_id, update_fields)
        if error:
            logger.error("CRM contact update failed: %s", error)
            if '404' in str(error):
                return jsonify({'error': 'Contact not found'}), 404
            return jsonify({'error': 'Failed to update contact'}), 502

        audit_log(
            AuditEvent.CRM_CONTACT_UPDATED,
            f"Updated contact: id={contact_id}, fields={list(update_fields.keys())}",
        )

        return jsonify({'status': 'success', 'data': record})

    except Exception:
        logger.exception("CRM contact update error")
        return jsonify({'error': 'Failed to update contact'}), 500


# =============================================================================
# Accounts
# =============================================================================

@crm_bp.route('/accounts/<account_id>', methods=['GET'])
@require_auth
@require_api_scope('crm:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def get_account(account_id):
    if not _validate_uuid(account_id):
        return jsonify({'error': 'Invalid account ID format'}), 400

    try:
        client = _get_sugar_client()

        record, error = client.get_record('Accounts', account_id, fields=ACCOUNT_FIELDS)
        if error:
            logger.error("CRM account get failed: %s", error)
            if '404' in str(error):
                return jsonify({'error': 'Account not found'}), 404
            return jsonify({'error': 'Failed to retrieve account'}), 502

        return jsonify({'status': 'success', 'data': record})

    except Exception:
        logger.exception("CRM account get error")
        return jsonify({'error': 'Failed to retrieve account'}), 500


@crm_bp.route('/accounts/search', methods=['GET'])
@require_auth
@require_api_scope('crm:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def search_accounts():
    name = request.args.get('name', '').strip()
    if not name or len(name) < 2:
        return jsonify({'error': 'name query parameter must be at least 2 characters'}), 400

    if len(name) > 100:
        return jsonify({'error': 'name query parameter too long'}), 400

    try:
        client = _get_sugar_client()

        filter_expr = [{'name': {'$contains': name}}]
        result, error = client.filter_records(
            module='Accounts',
            filter_expr=filter_expr,
            fields=ACCOUNT_FIELDS,
            max_num=20,
            order_by='date_modified:DESC',
        )

        if error:
            logger.error("CRM account search failed: %s", error)
            return jsonify({'error': 'Failed to search accounts'}), 502

        return jsonify({'status': 'success', 'data': result.get('records', [])})

    except Exception:
        logger.exception("CRM account search error")
        return jsonify({'error': 'Failed to search accounts'}), 500


@crm_bp.route('/accounts', methods=['POST'])
@require_auth
@require_api_scope('crm:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def create_account():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'name is required'}), 400

    fields = {}
    for key, value in data.items():
        if key in UPDATABLE_ACCOUNT_FIELDS:
            fields[key] = value.strip() if isinstance(value, str) else value

    if not fields:
        return jsonify({'error': 'No valid fields provided'}), 400

    try:
        client = _get_sugar_client()

        record, error = client.create_record('Accounts', fields)
        if error:
            logger.error("CRM account create failed: %s", error)
            return jsonify({'error': 'Failed to create account'}), 502

        audit_log(
            AuditEvent.CRM_ACCOUNT_CREATED,
            f"Created account: id={record.get('id')}, name={name}",
        )

        return jsonify({'status': 'success', 'data': record}), 201

    except Exception:
        logger.exception("CRM account create error")
        return jsonify({'error': 'Failed to create account'}), 500


@crm_bp.route('/accounts/<account_id>', methods=['PATCH'])
@require_auth
@require_api_scope('crm:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def update_account(account_id):
    if not _validate_uuid(account_id):
        return jsonify({'error': 'Invalid account ID format'}), 400

    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    update_fields = {}
    for key, value in data.items():
        if key in UPDATABLE_ACCOUNT_FIELDS:
            update_fields[key] = value.strip() if isinstance(value, str) else value

    if not update_fields:
        return jsonify({'error': 'No valid fields to update'}), 400

    try:
        client = _get_sugar_client()

        record, error = client.update_record('Accounts', account_id, update_fields)
        if error:
            logger.error("CRM account update failed: %s", error)
            if '404' in str(error):
                return jsonify({'error': 'Account not found'}), 404
            return jsonify({'error': 'Failed to update account'}), 502

        audit_log(
            AuditEvent.CRM_ACCOUNT_UPDATED,
            f"Updated account: id={account_id}, fields={list(update_fields.keys())}",
        )

        return jsonify({'status': 'success', 'data': record})

    except Exception:
        logger.exception("CRM account update error")
        return jsonify({'error': 'Failed to update account'}), 500
