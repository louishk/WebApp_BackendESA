"""
Tenant search and management API blueprint.

Dedicated blueprint for SOAP CallCenterWs tenant operations:
- Search tenants (TenantSearchDetailed)
- Get tenant info (TenantInfoByTenantID)
- Lookup tenant by unit (TenantIDByUnitNameOrAccessCode)
- List all tenants (TenantListDetailed_v3)
- Retrieve/insert tenant notes
- Update tenant details (TenantUpdate_v3)
- Schedule rate changes (ScheduleTenantRateChange_v2)
"""

import logging

from flask import Blueprint, jsonify, request

from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api
from web.utils.audit import audit_log, AuditEvent
from web.utils.soap_helpers import (
    CC_NS, cc_soap_action, get_cc_soap_client, validate_site_code,
    safe_int, sanitize_log, clamp,
)

logger = logging.getLogger(__name__)

tenants_bp = Blueprint('tenants', __name__, url_prefix='/api/tenants')


# =============================================================================
# GET /api/tenants/search — TenantSearchDetailed
# =============================================================================

@tenants_bp.route('/search')
@require_auth
@require_api_scope('tenants:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def tenant_search():
    """
    Search tenants by name, phone, or email via TenantSearchDetailed.

    Query parameters:
        site_code  — location code  [required]
        first_name — first name     [optional]
        last_name  — last name      [optional]
        phone      — phone number   [optional]
        email      — email address  [optional]
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    first_name = clamp(request.args.get('first_name', '').strip(), 100)
    last_name = clamp(request.args.get('last_name', '').strip(), 100)
    phone = clamp(request.args.get('phone', '').strip(), 20)
    email = clamp(request.args.get('email', '').strip(), 100)

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="TenantSearchDetailed",
            parameters={
                "sLocationCode": site_code,
                "sTenantFirstName": first_name,
                "sTenantLastName": last_name,
                "sPhoneNumber": phone,
                "sEmailAddress": email,
            },
            soap_action=cc_soap_action("TenantSearchDetailed"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'data': results or [],
            'count': len(results) if results else 0,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault TenantSearchDetailed: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error TenantSearchDetailed: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# GET /api/tenants/<tenant_id> — TenantInfoByTenantID
# =============================================================================

@tenants_bp.route('/<int:tenant_id>')
@require_auth
@require_api_scope('tenants:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def tenant_info(tenant_id):
    """
    Get tenant details by TenantID via TenantInfoByTenantID.

    Path parameter:
        tenant_id — the tenant ID

    Query parameters:
        site_code — location code [required]
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="TenantInfoByTenantID",
            parameters={
                "sLocationCode": site_code,
                "iTenantID": str(tenant_id),
            },
            soap_action=cc_soap_action("TenantInfoByTenantID"),
            namespace=CC_NS,
            result_tag="Table",
        )

        if not results:
            return jsonify({'error': 'Tenant not found'}), 404

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'data': results[0],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault TenantInfoByTenantID: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error TenantInfoByTenantID: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# GET /api/tenants/by-unit — TenantIDByUnitNameOrAccessCode
# =============================================================================

@tenants_bp.route('/by-unit')
@require_auth
@require_api_scope('tenants:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def tenant_by_unit():
    """
    Lookup tenant by unit name or access code via TenantIDByUnitNameOrAccessCode.

    Query parameters:
        site_code   — location code                        [required]
        unit_name   — unit name (e.g. "A101")              [one required]
        access_code — access code                          [one required]
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    unit_name = clamp(request.args.get('unit_name', '').strip(), 50)
    access_code = clamp(request.args.get('access_code', '').strip(), 50)

    if not unit_name and not access_code:
        return jsonify({'error': 'unit_name or access_code is required'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="TenantIDByUnitNameOrAccessCode",
            parameters={
                "sLocationCode": site_code,
                "sUnitName": unit_name,
                "sAccessCode": access_code,
            },
            soap_action=cc_soap_action("TenantIDByUnitNameOrAccessCode"),
            namespace=CC_NS,
            result_tag="RT",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'data': results or [],
            'count': len(results) if results else 0,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault TenantIDByUnitNameOrAccessCode: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error TenantIDByUnitNameOrAccessCode: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# GET /api/tenants/list — TenantListDetailed_v3
# =============================================================================

@tenants_bp.route('/list')
@require_auth
@require_api_scope('tenants:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def tenant_list():
    """
    List all tenants for a site via TenantListDetailed_v3.

    Query parameters:
        site_code — location code [required]
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="TenantListDetailed_v3",
            parameters={
                "sLocationCode": site_code,
            },
            soap_action=cc_soap_action("TenantListDetailed_v3"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'data': results or [],
            'count': len(results) if results else 0,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault TenantListDetailed_v3: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error TenantListDetailed_v3: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# GET /api/tenants/<tenant_id>/notes — TenantNotesRetrieve
# =============================================================================

@tenants_bp.route('/<int:tenant_id>/notes')
@require_auth
@require_api_scope('tenants:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def tenant_notes_get(tenant_id):
    """
    Retrieve notes for a tenant via TenantNotesRetrieve.

    Path parameter:
        tenant_id — the tenant ID (kept for URL shape; not sent to SMD)

    Query parameters:
        site_code — location code [required]
        ledger_id — ledger ID     [required] (WSDL expects iLedgerID)
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    ledger_id, lid_err = safe_int(request.args.get('ledger_id'), min_val=1)
    if lid_err:
        return jsonify({'error': f'ledger_id: {lid_err}'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="TenantNotesRetrieve",
            parameters={
                "sLocationCode": site_code,
                "iLedgerID": str(ledger_id),
            },
            soap_action=cc_soap_action("TenantNotesRetrieve"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'ledger_id': ledger_id,
            'data': results or [],
            'count': len(results) if results else 0,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault TenantNotesRetrieve: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error TenantNotesRetrieve: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# POST /api/tenants/<tenant_id>/notes — TenantNoteInsert_v2
# =============================================================================

@tenants_bp.route('/<int:tenant_id>/notes', methods=['POST'])
@require_auth
@require_api_scope('tenants:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def tenant_notes_add(tenant_id):
    """
    Add a note to a tenant via TenantNoteInsert_v2.

    Path parameter:
        tenant_id — the tenant ID (kept for URL shape; not sent to SMD)

    JSON body:
        site_code     — location code              [required]
        ledger_id     — ledger ID (int)             [required] (WSDL expects iLedgerID)
        note          — note text (max 2000 chars)  [required]
        note_category — note type ID               [optional, default 0] (WSDL: iNoteType)
    """
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    note = clamp(data.get('note', '').strip(), 2000)
    note_category = clamp(str(data.get('note_category', '0')).strip(), 10)

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not note:
        return jsonify({'error': 'note is required'}), 400

    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    ledger_id, lid_err = safe_int(data.get('ledger_id'), min_val=1)
    if lid_err:
        return jsonify({'error': f'ledger_id: {lid_err}'}), 400

    # Validate note_category is an integer
    cat_id, cat_err = safe_int(note_category, default=0, min_val=0)
    if cat_err:
        return jsonify({'error': f'note_category: {cat_err}'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="TenantNoteInsert_v2",
            parameters={
                "sLocationCode": site_code,
                "iLedgerID": str(ledger_id),
                "sNote": note,
                "iNoteType": str(cat_id),
            },
            soap_action=cc_soap_action("TenantNoteInsert_v2"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = None
        ret_msg = None
        if results:
            ret_code = results[0].get('Ret_Code')
            ret_msg = results[0].get('Ret_Msg')

        if ret_code is not None and str(ret_code) == '-1':
            logger.error(
                f"SMD rejected TenantNoteInsert_v2: site={site_code} "
                f"ledger_id={ledger_id} ret_msg={ret_msg}"
            )
            return jsonify({'error': 'Note insert rejected by SMD', 'detail': ret_msg}), 502

        logger.info(f"Note inserted for tenant {tenant_id} ledger {ledger_id} at {site_code}")
        audit_log(
            AuditEvent.TENANT_NOTE_ADDED,
            f"site={site_code} tenant_id={tenant_id} ledger_id={ledger_id} "
            f"note_category={cat_id} note={sanitize_log(note)}"
        )

        return jsonify({
            'status': 'success',
            'tenant_id': tenant_id,
            'ledger_id': ledger_id,
            'ret_code': ret_code,
            'message': ret_msg or 'Note added',
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault TenantNoteInsert_v2: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error TenantNoteInsert_v2: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# PUT /api/tenants/<tenant_id> — TenantUpdate_v3
# =============================================================================

@tenants_bp.route('/<int:tenant_id>', methods=['PUT'])
@require_auth
@require_api_scope('tenants:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def tenant_update(tenant_id):
    """
    Update tenant details via TenantUpdate_v3.

    Path parameter:
        tenant_id — the tenant ID

    JSON body:
        site_code   — location code    [required]
        first_name  — first name       [optional]
        last_name   — last name        [optional]
        email       — email            [optional]
        phone       — phone            [optional]
        mobile      — mobile           [optional]
        address     — street address   [optional]
        city        — city             [optional]
        postal_code — postal code      [optional]
        country     — country code     [optional]
        company     — company name     [optional]
    """
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    # Optional fields — pass empty string for fields not provided
    first_name = clamp(data.get('first_name', '').strip(), 100)
    last_name = clamp(data.get('last_name', '').strip(), 100)
    email = clamp(data.get('email', '').strip(), 100)
    phone = clamp(data.get('phone', '').strip(), 20)
    mobile = clamp(data.get('mobile', '').strip(), 20)
    address = clamp(data.get('address', '').strip(), 200)
    city = clamp(data.get('city', '').strip(), 100)
    postal_code = clamp(data.get('postal_code', '').strip(), 20)
    country = clamp(data.get('country', '').strip(), 10)
    company = clamp(data.get('company', '').strip(), 100)

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="TenantUpdate_v3",
            parameters={
                "sLocationCode": site_code,
                "iTenantID": str(tenant_id),
                "sFName": first_name,
                "sLName": last_name,
                "sEmail": email,
                "sPhone": phone,
                "sMobile": mobile,
                "sAddr1": address,
                "sCity": city,
                "sPostalCode": postal_code,
                "sCountry": country,
                "sCompany": company,
            },
            soap_action=cc_soap_action("TenantUpdate_v3"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = None
        ret_msg = None
        if results:
            ret_code = results[0].get('Ret_Code')
            ret_msg = results[0].get('Ret_Msg')

        logger.info(f"Tenant updated: tenant_id={tenant_id} site={site_code}")
        audit_log(
            AuditEvent.TENANT_UPDATED,
            f"site={site_code} tenant_id={tenant_id} "
            f"name={sanitize_log(first_name)} {sanitize_log(last_name)}"
        )

        return jsonify({
            'status': 'success',
            'tenant_id': tenant_id,
            'ret_code': ret_code,
            'message': ret_msg or 'Tenant updated',
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault TenantUpdate_v3: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error TenantUpdate_v3: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# POST /api/tenants/<tenant_id>/rate-change — ScheduleTenantRateChange_v2
# =============================================================================

@tenants_bp.route('/<int:tenant_id>/rate-change', methods=['POST'])
@require_auth
@require_api_scope('tenants:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def tenant_rate_change(tenant_id):
    """
    Schedule a tenant rate change via ScheduleTenantRateChange (v1).

    Path parameter:
        tenant_id — the tenant ID (kept for URL shape; not sent to SMD)

    JSON body:
        site_code      — location code               [required]
        ledger_id      — ledger ID (int)              [required] (WSDL: LedgerID)
        new_rate       — new rate (decimal, 2dp)       [required] (WSDL: dcNewRate)
        effective_date — effective date (YYYY-MM-DD)   [required] (WSDL: dScheduledChange, sent as YYYY-MM-DDT00:00:00)
    """
    from common.soap_client import SOAPFaultError
    from datetime import datetime

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    # Validate ledger_id
    ledger_id, lid_err = safe_int(data.get('ledger_id'), min_val=1)
    if lid_err:
        return jsonify({'error': f'ledger_id: {lid_err}'}), 400

    # Validate new_rate
    raw_rate = data.get('new_rate')
    if raw_rate is None:
        return jsonify({'error': 'new_rate is required'}), 400
    try:
        new_rate = float(raw_rate)
    except (ValueError, TypeError):
        return jsonify({'error': 'new_rate must be a number'}), 400
    if new_rate < 0 or new_rate > 1_000_000:
        return jsonify({'error': 'new_rate out of range (0–1000000)'}), 400
    rate_str = f"{new_rate:.2f}"

    # Validate effective_date — accept YYYY-MM-DD, send as full .NET datetime
    effective_date = data.get('effective_date', '').strip()
    if not effective_date:
        return jsonify({'error': 'effective_date is required'}), 400
    try:
        datetime.strptime(effective_date, '%Y-%m-%d')
    except (ValueError, TypeError):
        return jsonify({'error': 'effective_date must be YYYY-MM-DD format'}), 400
    scheduled_change = f"{effective_date}T00:00:00"

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="ScheduleTenantRateChange",
            parameters={
                "sLocationCode": site_code,
                "LedgerID": str(ledger_id),
                "dcNewRate": rate_str,
                "dScheduledChange": scheduled_change,
            },
            soap_action=cc_soap_action("ScheduleTenantRateChange"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = None
        ret_msg = None
        if results:
            ret_code = results[0].get('Ret_Code')
            ret_msg = results[0].get('Ret_Msg')

        if ret_code is not None and str(ret_code) == '-1':
            logger.error(
                f"SMD rejected ScheduleTenantRateChange: site={site_code} "
                f"ledger_id={ledger_id} new_rate={rate_str} "
                f"effective={effective_date} ret_msg={ret_msg}"
            )
            return jsonify({'error': 'Rate change rejected by SMD', 'detail': ret_msg}), 502

        logger.info(
            f"Rate change scheduled: tenant_id={tenant_id} site={site_code} "
            f"ledger_id={ledger_id} new_rate={rate_str} effective={effective_date}"
        )
        audit_log(
            AuditEvent.TENANT_RATE_CHANGE_SCHEDULED,
            f"site={site_code} tenant_id={tenant_id} ledger_id={ledger_id} "
            f"new_rate={rate_str} effective_date={effective_date}"
        )

        return jsonify({
            'status': 'success',
            'tenant_id': tenant_id,
            'ledger_id': ledger_id,
            'new_rate': rate_str,
            'effective_date': effective_date,
            'ret_code': ret_code,
            'message': ret_msg or 'Rate change scheduled',
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ScheduleTenantRateChange: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error ScheduleTenantRateChange: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()
