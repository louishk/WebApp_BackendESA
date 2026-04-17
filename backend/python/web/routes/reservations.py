"""
Reservations API routes.

Dedicated blueprint for SOAP CallCenterWs reservation operations:
- Reserve unit: TenantNewDetailed_v3 → ReservationNewWithSource_v6
- List/get reservations (ReservationList_v3)
- Update/cancel reservation (ReservationUpdate_v4)
- Retrieve/insert notes
- Fee retrieve

IMPORTANT: SMD SOAP date fields must never be empty strings — the server
crashes with HTTP 500 on <dDOB></dDOB> etc. Always provide a real datetime
value or use the _default_date() / _PLACEHOLDER_DOB helpers.
"""

import logging
from datetime import datetime, timedelta, timezone

from flask import Blueprint, g, jsonify, request
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api
from web.utils.audit import audit_log
from web.utils.soap_helpers import (
    get_pbi_session, CC_NS, cc_soap_action, get_cc_soap_client,
    validate_site_code, safe_int, safe_rate, sanitize_log, clamp,
    default_date, parse_date, require_date,
)

logger = logging.getLogger(__name__)

reservations_bp = Blueprint('reservations', __name__, url_prefix='/api/reservations')

# Placeholder DOB — SMD requires a non-empty datetime for dDOB
_PLACEHOLDER_DOB = "1900-01-01T00:00:00"

# Backward-compat aliases (private names used throughout this file)
_cc_soap_action = cc_soap_action
_get_cc_soap_client = get_cc_soap_client
_validate_site_code = validate_site_code
_safe_int = safe_int
_safe_rate = safe_rate
_sanitize_log = sanitize_log
_clamp = clamp
_default_date = default_date
_parse_date = parse_date
_require_date = require_date


def _record_reservation(**kwargs):
    """Upsert reservation tracking record into esa_pbi. Best-effort, never raises."""
    try:
        session = get_pbi_session()
        try:
            session.execute(text("""
                INSERT INTO api_reservations (
                    site_code, unit_id, first_name, last_name, email, phone,
                    mobile, quoted_rate, concession_id, needed_date, source_name,
                    comment, tenant_id, waiting_id, global_waiting_num,
                    source, gclid, gid, botid, api_key_id, api_user,
                    reserved_at, status
                ) VALUES (
                    :site_code, :unit_id, :first_name, :last_name, :email, :phone,
                    :mobile, :quoted_rate, :concession_id, :needed_date, :source_name,
                    :comment, :tenant_id, :waiting_id, :global_waiting_num,
                    :source, :gclid, :gid, :botid, :api_key_id, :api_user,
                    NOW(), :status
                )
                ON CONFLICT (site_code, waiting_id) WHERE waiting_id IS NOT NULL
                DO UPDATE SET
                    unit_id = EXCLUDED.unit_id,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    mobile = EXCLUDED.mobile,
                    quoted_rate = EXCLUDED.quoted_rate,
                    concession_id = EXCLUDED.concession_id,
                    needed_date = EXCLUDED.needed_date,
                    source_name = EXCLUDED.source_name,
                    comment = EXCLUDED.comment,
                    tenant_id = COALESCE(EXCLUDED.tenant_id, api_reservations.tenant_id),
                    global_waiting_num = COALESCE(EXCLUDED.global_waiting_num, api_reservations.global_waiting_num),
                    source = EXCLUDED.source,
                    gclid = COALESCE(EXCLUDED.gclid, api_reservations.gclid),
                    gid = COALESCE(EXCLUDED.gid, api_reservations.gid),
                    botid = COALESCE(EXCLUDED.botid, api_reservations.botid),
                    updated_at = NOW()
            """), kwargs)
            session.commit()
        finally:
            session.close()
    except Exception as e:
        logger.warning(f"Failed to record reservation tracking: {e}")


_LIFECYCLE_DATE_COLUMNS = frozenset({'reserved_at', 'moved_in_at', 'cancelled_at', 'expired_at'})


def _update_reservation_status(site_code, waiting_id, status, date_column):
    """Update reservation status and lifecycle date. Best-effort, never raises."""
    if date_column not in _LIFECYCLE_DATE_COLUMNS:
        logger.error(f"Invalid date_column rejected: {date_column!r}")
        return 0
    try:
        session = get_pbi_session()
        try:
            # safe: date_column validated against _LIFECYCLE_DATE_COLUMNS allowlist above
            result = session.execute(text(f"""
                UPDATE api_reservations
                SET status = :status, {date_column} = NOW(), updated_at = NOW()
                WHERE site_code = :site_code AND waiting_id = :waiting_id
            """), {'status': status, 'site_code': site_code, 'waiting_id': waiting_id})
            session.commit()
            return result.rowcount
        finally:
            session.close()
    except Exception as e:
        logger.warning(f"Failed to update reservation status: {e}")
        return 0


def _get_caller_info():
    """Extract API caller identity from Flask g."""
    user = getattr(g, 'current_user', None) or {}
    return user.get('key_id'), user.get('sub')


# =============================================================================
# POST /api/reservations/reserve — full flow: create tenant + reservation
# =============================================================================

@reservations_bp.route('/reserve', methods=['POST'])
@require_auth
@require_api_scope('reservations:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def reservation_reserve():
    """
    Reserve a unit: creates a tenant via TenantNewDetailed_v3, then creates
    a reservation via ReservationNewWithSource_v6.

    JSON body:
        site_code    — location code (e.g. "LSETUP")  [required]
        unit_id      — unit ID to reserve              [required]
        first_name   — tenant first name               [required]
        last_name    — tenant last name                [required]
        phone        — contact phone                   [required]
        email        — contact email                   [optional]
        mobile       — mobile phone                    [optional]
        address      — street address                  [optional]
        city         — city                            [optional]
        postal_code  — postal code                     [optional]
        country      — country code (e.g. "SG")        [optional]
        comment      — reservation comment             [optional]
        quoted_rate  — quoted rate                      [optional, default: 0]
        needed_date  — move-in date (YYYY-MM-DD)       [optional, default: tomorrow]
        source_name  — lead source                     [optional, default: "ESA Backend"]
    """
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    # Validate required fields
    site_code = data.get('site_code', '').strip()
    unit_id = data.get('unit_id')
    first_name = data.get('first_name', '').strip()
    last_name = data.get('last_name', '').strip()
    phone = data.get('phone', '').strip()

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not unit_id:
        return jsonify({'error': 'unit_id is required'}), 400
    if not first_name:
        return jsonify({'error': 'first_name is required'}), 400
    if not last_name:
        return jsonify({'error': 'last_name is required'}), 400
    if not phone:
        return jsonify({'error': 'phone is required'}), 400

    try:
        unit_id = int(unit_id)
    except (ValueError, TypeError):
        return jsonify({'error': 'unit_id must be an integer'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    # Optional fields with length limits
    email = _clamp(data.get('email', '').strip(), 100)
    mobile = _clamp(data.get('mobile', '').strip(), 20) or phone
    address = _clamp(data.get('address', '').strip(), 200)
    city = _clamp(data.get('city', '').strip(), 100)
    postal_code = _clamp(data.get('postal_code', '').strip(), 20)
    country = _clamp(data.get('country', '').strip(), 10)
    comment = _clamp(data.get('comment', '').strip(), 500)
    source_name = _clamp(data.get('source_name', 'ESA Backend'), 64)
    needed = _parse_date(data.get('needed_date'), 1)

    quoted_rate, rate_err = _safe_rate(data.get('quoted_rate', 0))
    if rate_err:
        return jsonify({'error': f'quoted_rate: {rate_err}'}), 400

    concession_id, cid_err = _safe_int(data.get('concession_id', 0), min_val=0)
    if cid_err:
        return jsonify({'error': f'concession_id: {cid_err}'}), 400

    # Distribution tracking fields
    source = _clamp(data.get('source', 'api'), 50)
    gclid = _clamp(data.get('gclid', ''), 255) or None
    gid = _clamp(data.get('gid', ''), 255) or None
    botid = _clamp(data.get('botid', ''), 255) or None

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        # Step 1: Create tenant via TenantNewDetailed_v3
        tenant_results = soap_client.call(
            operation="TenantNewDetailed_v3",
            parameters={
                "sLocationCode": site_code,
                "sWebPassword": "",
                "sMrMrs": "",
                "sFName": first_name,
                "sMI": "",
                "sLName": last_name,
                "sCompany": "",
                "sAddr1": address,
                "sAddr2": "",
                "sCity": city,
                "sRegion": "",
                "sPostalCode": postal_code,
                "sCountry": country,
                "sPhone": phone,
                "sMrMrsAlt": "", "sFNameAlt": "", "sMIAlt": "", "sLNameAlt": "",
                "sAddr1Alt": "", "sAddr2Alt": "", "sCityAlt": "", "sRegionAlt": "",
                "sPostalCodeAlt": "", "sCountryAlt": "", "sPhoneAlt": "",
                "sMrMrsBus": "", "sFNameBus": "", "sMIBus": "", "sLNameBus": "",
                "sCompanyBus": "", "sAddr1Bus": "", "sAddr2Bus": "", "sCityBus": "",
                "sRegionBus": "", "sPostalCodeBus": "", "sCountryBus": "", "sPhoneBus": "",
                "sFax": "",
                "sEmail": email,
                "sPager": "",
                "sMobile": mobile,
                "bCommercial": "false",
                "bCompanyIsTenant": "false",
                "dDOB": _PLACEHOLDER_DOB,
                "sTenNote": comment or f"Reserved unit via {source_name}",
                "sLicense": "",
                "sLicRegion": "",
                "sSSN": "",
                "sGateCode": "",
                "sEmailAlt": "",
                "sRelationshipAlt": "",
                "sTaxID": "",
                "bSMSOptIn": "false",
                "sCountryCode": country,
            },
            soap_action=_cc_soap_action("TenantNewDetailed_v3"),
            namespace=CC_NS,
            result_tag="RT",
        )

        tenant_id = None
        if tenant_results:
            tenant_id = tenant_results[0].get('TenantID')

        if not tenant_id:
            logger.error(f"TenantNewDetailed_v3 failed: {tenant_results}")
            return jsonify({'success': False, 'error': 'Failed to create tenant'}), 502

        logger.info(f"TenantNewDetailed_v3 site={site_code}: TenantID={tenant_id}")

        # Step 2: Create reservation via ReservationNewWithSource_v6
        res_results = soap_client.call(
            operation="ReservationNewWithSource_v6",
            parameters={
                "sLocationCode": site_code,
                "sTenantID": str(tenant_id),
                "sUnitID": str(unit_id),
                "dNeeded": needed,
                "sComment": comment,
                "iSource": "0",
                "sSource": source_name,
                "QTRentalTypeID": "0",
                "iInquiryType": "0",
                "dcQuotedRate": quoted_rate,
                "dExpires": _default_date(14),
                "dFollowUp": _default_date(3),
                "sTrackingCode": "",
                "sCallerID": "",
                "ConcessionID": str(concession_id),
                "PromoGlobalNum": "0",
            },
            soap_action=_cc_soap_action("ReservationNewWithSource_v6"),
            namespace=CC_NS,
            result_tag="RT",
        )

        # v6 returns WaitingID as Ret_Code, GlobalWaitingNum as Ret_Msg
        waiting_id = None
        global_waiting_num = None
        if res_results:
            waiting_id = res_results[0].get('Ret_Code')
            global_waiting_num = res_results[0].get('Ret_Msg')

        logger.info(
            f"ReservationNewWithSource_v6 unit={unit_id} site={site_code}: "
            f"tenant_id={tenant_id}, waiting_id={waiting_id}"
        )
        audit_log(
            'RESERVATION_CREATED',
            f"site={site_code} unit={unit_id} tenant={tenant_id} "
            f"waiting_id={waiting_id} name={_sanitize_log(first_name)} {_sanitize_log(last_name)}"
        )

        # Record for distribution analytics
        api_key_id, api_user = _get_caller_info()
        _record_reservation(
            site_code=site_code, unit_id=unit_id,
            first_name=first_name, last_name=last_name,
            email=email, phone=phone, mobile=mobile,
            quoted_rate=quoted_rate, concession_id=concession_id,
            needed_date=needed, source_name=source_name,
            comment=comment, tenant_id=tenant_id,
            waiting_id=waiting_id, global_waiting_num=global_waiting_num,
            source=source, gclid=gclid, gid=gid, botid=botid,
            api_key_id=api_key_id, api_user=api_user,
            status='created',
        )

        return jsonify({
            'success': True,
            'site_code': site_code,
            'unit_id': unit_id,
            'tenant_id': tenant_id,
            'waiting_id': waiting_id,
            'global_waiting_num': global_waiting_num,
            'message': 'Reservation created',
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault in reserve flow: {e}")
        return jsonify({'success': False, 'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error in reserve flow: {e}")
        return jsonify({'success': False, 'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# POST /api/reservations/create — ReservationNewWithSource_v6 (existing tenant)
# =============================================================================

@reservations_bp.route('/create', methods=['POST'])
@require_auth
@require_api_scope('reservations:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def reservation_create():
    """
    Create a reservation for an existing tenant via ReservationNewWithSource_v6.

    JSON body:
        site_code      — location code (e.g. "LSETUP")  [required]
        unit_id        — unit ID to reserve              [required]
        tenant_id      — existing tenant ID              [required]
        needed_date    — move-in date (YYYY-MM-DD)       [default: today+1]
        expires_date   — expiry date (YYYY-MM-DD)        [default: today+14]
        followup_date  — follow-up date (YYYY-MM-DD)     [default: today+3]
        quoted_rate    — quoted rate                      [default: 0]
        comment        — reservation comment              [default: ""]
        source_name    — source name                      [default: "ESA Backend"]
        concession_id  — concession/discount plan ID      [default: 0]
    """
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    unit_id = data.get('unit_id')
    tenant_id = data.get('tenant_id')

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not unit_id:
        return jsonify({'error': 'unit_id is required'}), 400
    if not tenant_id:
        return jsonify({'error': 'tenant_id is required'}), 400

    try:
        unit_id = int(unit_id)
        tenant_id = int(tenant_id)
    except (ValueError, TypeError):
        return jsonify({'error': 'unit_id and tenant_id must be integers'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    needed = _parse_date(data.get('needed_date'), 1)
    expires = _parse_date(data.get('expires_date'), 14)
    followup = _parse_date(data.get('followup_date'), 3)

    quoted_rate, rate_err = _safe_rate(data.get('quoted_rate', 0))
    if rate_err:
        return jsonify({'error': f'quoted_rate: {rate_err}'}), 400

    concession_id, cid_err = _safe_int(data.get('concession_id', 0), min_val=0)
    if cid_err:
        return jsonify({'error': f'concession_id: {cid_err}'}), 400

    # Distribution tracking fields
    source = _clamp(data.get('source', 'api'), 50)
    gclid = _clamp(data.get('gclid', ''), 255) or None
    gid = _clamp(data.get('gid', ''), 255) or None
    botid = _clamp(data.get('botid', ''), 255) or None

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        results = soap_client.call(
            operation="ReservationNewWithSource_v6",
            parameters={
                "sLocationCode": site_code,
                "sTenantID": str(tenant_id),
                "sUnitID": str(unit_id),
                "dNeeded": needed,
                "sComment": _clamp(data.get('comment', ''), 500),
                "iSource": "0",
                "sSource": _clamp(data.get('source_name', 'ESA Backend'), 64),
                "QTRentalTypeID": "0",
                "iInquiryType": "0",
                "dcQuotedRate": quoted_rate,
                "dExpires": expires,
                "dFollowUp": followup,
                "sTrackingCode": "",
                "sCallerID": "",
                "ConcessionID": str(concession_id),
                "PromoGlobalNum": "0",
            },
            soap_action=_cc_soap_action("ReservationNewWithSource_v6"),
            namespace=CC_NS,
            result_tag="RT",
        )

        waiting_id = None
        global_waiting_num = None
        if results:
            waiting_id = results[0].get('Ret_Code')
            global_waiting_num = results[0].get('Ret_Msg')

        logger.info(
            f"ReservationNewWithSource_v6 unit={unit_id} site={site_code}: "
            f"tenant_id={tenant_id}, waiting_id={waiting_id}"
        )
        audit_log(
            'RESERVATION_CREATED',
            f"site={site_code} unit={unit_id} tenant={tenant_id} waiting_id={waiting_id}"
        )

        # Record for distribution analytics
        api_key_id, api_user = _get_caller_info()
        _record_reservation(
            site_code=site_code, unit_id=unit_id,
            first_name='', last_name='',
            email='', phone='', mobile='',
            quoted_rate=quoted_rate, concession_id=concession_id,
            needed_date=needed, source_name=_clamp(data.get('source_name', 'ESA Backend'), 64),
            comment=_clamp(data.get('comment', ''), 500), tenant_id=tenant_id,
            waiting_id=waiting_id, global_waiting_num=global_waiting_num,
            source=source, gclid=gclid, gid=gid, botid=botid,
            api_key_id=api_key_id, api_user=api_user,
            status='created',
        )

        return jsonify({
            'success': True,
            'site_code': site_code,
            'unit_id': unit_id,
            'tenant_id': tenant_id,
            'waiting_id': waiting_id,
            'global_waiting_num': global_waiting_num,
            'message': 'Reservation created',
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ReservationNewWithSource_v6: {e}")
        return jsonify({'success': False, 'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error ReservationNewWithSource_v6: {e}")
        return jsonify({'success': False, 'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# GET /api/reservations/list — ReservationList_v3
# =============================================================================

@reservations_bp.route('/list')
@require_auth
@require_api_scope('reservations:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def reservation_list():
    """
    List reservations for a site via ReservationList_v3.

    Query parameters:
        site_code          — location code (required)
        waiting_id         — filter by specific WaitingID (default: 0 = all)
        global_waiting_num — filter by global waiting number (default: 0 = all)
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    waiting_id = request.args.get('waiting_id', '0').strip()
    global_waiting_num = request.args.get('global_waiting_num', '0').strip()

    try:
        waiting_id = int(waiting_id)
        global_waiting_num = int(global_waiting_num)
    except ValueError:
        return jsonify({'error': 'waiting_id and global_waiting_num must be integers'}), 400

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        results = soap_client.call(
            operation="ReservationList_v3",
            parameters={
                "sLocationCode": site_code,
                "iGlobalWaitingNum": str(global_waiting_num),
                "WaitingID": str(waiting_id),
            },
            soap_action=_cc_soap_action("ReservationList_v3"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'site_code': site_code,
            'reservations': results or [],
            'count': len(results) if results else 0,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ReservationList_v3: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error ReservationList_v3: {e}")
        return jsonify({'error': 'Failed to retrieve reservations'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# GET /api/reservations/<waiting_id> — single reservation detail
# =============================================================================

@reservations_bp.route('/<int:waiting_id>')
@require_auth
@require_api_scope('reservations:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def reservation_get(waiting_id):
    """
    Get a single reservation by WaitingID via ReservationList_v3.

    Path parameter:
        waiting_id — the reservation WaitingID

    Query parameters:
        site_code — location code (required)
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        results = soap_client.call(
            operation="ReservationList_v3",
            parameters={
                "sLocationCode": site_code,
                "iGlobalWaitingNum": "0",
                "WaitingID": str(waiting_id),
            },
            soap_action=_cc_soap_action("ReservationList_v3"),
            namespace=CC_NS,
            result_tag="Table",
        )

        if not results:
            return jsonify({'error': 'Reservation not found'}), 404

        return jsonify({
            'site_code': site_code,
            'reservation': results[0],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ReservationList_v3 (get): {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error fetching reservation {waiting_id}: {e}")
        return jsonify({'error': 'Failed to retrieve reservation'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# PUT /api/reservations/<waiting_id> — ReservationUpdate_v4
# =============================================================================

@reservations_bp.route('/<int:waiting_id>', methods=['PUT'])
@require_auth
@require_api_scope('reservations:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def reservation_update(waiting_id):
    """
    Update a reservation via ReservationUpdate_v4.

    Path parameter:
        waiting_id — the reservation WaitingID

    JSON body:
        site_code              — location code                     [required]
        tenant_id              — tenant ID                         [required]
        unit_id                — unit ID                           [required]
        needed_date            — needed date (YYYY-MM-DD)          [required]
        comment                — comment                           [default: ""]
        status                 — reservation status code           [default: 0]
        followup               — enable follow-up (bool)           [default: false]
        followup_date          — follow-up date (YYYY-MM-DD)       [default: today+3]
        followup_last_date     — last follow-up date               [default: today+3]
        inquiry_type           — inquiry type                      [default: 0]
        quoted_rate            — quoted rate                       [default: 0]
        expires_date           — expiry date (YYYY-MM-DD)          [default: today+14]
        rental_type_id         — rental type ID                    [default: 0]
        cancellation_type_id   — cancellation type ID              [default: 0]
        cancellation_reason    — cancellation reason               [default: ""]
        concession_id          — concession ID                     [default: 0]
    """
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    unit_id = data.get('unit_id')

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not unit_id:
        return jsonify({'error': 'unit_id is required'}), 400

    try:
        unit_id = int(unit_id)
    except (ValueError, TypeError):
        return jsonify({'error': 'unit_id must be an integer'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    needed_date, needed_err = _require_date(data.get('needed_date'))
    if needed_err:
        return jsonify({'error': f'needed_date: {needed_err}'}), 400

    # Date fields must never be empty — SMD crashes on empty date XML elements
    followup_date = _parse_date(data.get('followup_date'), 3)
    followup_last = _parse_date(data.get('followup_last_date'), 3)
    expires_date = _parse_date(data.get('expires_date'), 14)

    # Validate numeric fields
    status, status_err = _safe_int(data.get('status', 0), min_val=0, max_val=10)
    if status_err:
        return jsonify({'error': f'status: {status_err}'}), 400
    quoted_rate, rate_err = _safe_rate(data.get('quoted_rate', 0))
    if rate_err:
        return jsonify({'error': f'quoted_rate: {rate_err}'}), 400
    inquiry_type, _ = _safe_int(data.get('inquiry_type', 0), min_val=0)
    rental_type_id, _ = _safe_int(data.get('rental_type_id', 0), min_val=0)
    cancel_type_id, _ = _safe_int(data.get('cancellation_type_id', 0), min_val=0)
    concession_id, _ = _safe_int(data.get('concession_id', 0), min_val=0)

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        results = soap_client.call(
            operation="ReservationUpdate_v4",
            parameters={
                "sLocationCode": site_code,
                "WaitingID": str(waiting_id),
                "sTenantID": str(data.get('tenant_id', '0')),
                "sUnitID": str(unit_id),
                "dNeeded": needed_date,
                "sComment": _clamp(data.get('comment', ''), 500),
                "iStatus": str(status),
                "bFollowup": str(data.get('followup', False)).lower(),
                "dFollowup": followup_date,
                "dFollowupLast": followup_last,
                "iInquiryType": str(inquiry_type or 0),
                "dcQuotedRate": quoted_rate,
                "dExpires": expires_date,
                "QTRentalTypeID": str(rental_type_id or 0),
                "QTCancellationTypeID": str(cancel_type_id or 0),
                "sCancellationReason": _clamp(data.get('cancellation_reason', ''), 200),
                "ConcessionID": str(concession_id or 0),
            },
            soap_action=_cc_soap_action("ReservationUpdate_v4"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = None
        ret_msg = None
        if results:
            ret_code = results[0].get('Ret_Code')
            ret_msg = results[0].get('Ret_Msg')

        logger.info(
            f"ReservationUpdate_v4 waiting_id={waiting_id} site={site_code}: "
            f"code={ret_code}, msg={ret_msg}"
        )
        audit_log(
            'RESERVATION_UPDATED',
            f"site={site_code} waiting_id={waiting_id} status={status}"
        )

        return jsonify({
            'success': True,
            'waiting_id': waiting_id,
            'ret_code': ret_code,
            'message': ret_msg or 'Reservation updated',
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ReservationUpdate_v4: {e}")
        return jsonify({'success': False, 'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error ReservationUpdate_v4: {e}")
        return jsonify({'success': False, 'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# PUT /api/reservations/<waiting_id>/cancel — cancel via ReservationUpdate_v4
# =============================================================================

@reservations_bp.route('/<int:waiting_id>/cancel', methods=['PUT'])
@require_auth
@require_api_scope('reservations:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def reservation_cancel(waiting_id):
    """
    Cancel a reservation. Fetches current data then sets iStatus=2.

    Path parameter:
        waiting_id — the reservation WaitingID

    JSON body:
        site_code            — location code                [required]
        cancellation_reason  — reason for cancellation      [default: ""]
    """
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        # Fetch current reservation data
        current = soap_client.call(
            operation="ReservationList_v3",
            parameters={
                "sLocationCode": site_code,
                "iGlobalWaitingNum": "0",
                "WaitingID": str(waiting_id),
            },
            soap_action=_cc_soap_action("ReservationList_v3"),
            namespace=CC_NS,
            result_tag="Table",
        )

        if not current:
            return jsonify({'error': 'Reservation not found'}), 404

        res = current[0]

        # Date fields from existing reservation — never empty
        needed = _parse_date(res.get('dNeeded'), 1)
        followup = _parse_date(res.get('dFollowup'), 3)
        expires = _parse_date(res.get('dExpires'), 14)

        # Cancel: set iStatus=2
        results = soap_client.call(
            operation="ReservationUpdate_v4",
            parameters={
                "sLocationCode": site_code,
                "WaitingID": str(waiting_id),
                "sTenantID": str(res.get('TenantID', '0')),
                "sUnitID": str(res.get('UnitID', '0')),
                "dNeeded": needed,
                "sComment": res.get('sComment') or '',
                "iStatus": "2",
                "bFollowup": "false",
                "dFollowup": followup,
                "dFollowupLast": followup,
                "iInquiryType": str(res.get('iInquiryType', 0)),
                "dcQuotedRate": str(res.get('dcRate_Quoted', 0)),
                "dExpires": expires,
                "QTRentalTypeID": str(res.get('QTRentalTypeID', 0)),
                "QTCancellationTypeID": str(data.get('cancellation_type_id', 0)),
                "sCancellationReason": _clamp(data.get('cancellation_reason', ''), 200),
                "ConcessionID": str(res.get('ConcessionID', 0)),
            },
            soap_action=_cc_soap_action("ReservationUpdate_v4"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = None
        ret_msg = None
        if results:
            ret_code = results[0].get('Ret_Code')
            ret_msg = results[0].get('Ret_Msg')

        logger.info(f"Reservation cancelled: waiting_id={waiting_id} site={site_code}")
        audit_log(
            'RESERVATION_CANCELLED',
            f"site={site_code} waiting_id={waiting_id} "
            f"reason={_sanitize_log(data.get('cancellation_reason', ''))}"
        )

        # Sync tracking table
        _update_reservation_status(site_code, waiting_id, 'cancelled', 'cancelled_at')

        return jsonify({
            'success': True,
            'waiting_id': waiting_id,
            'ret_code': ret_code,
            'message': ret_msg or 'Reservation cancelled',
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault cancelling reservation {waiting_id}: {e}")
        return jsonify({'success': False, 'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error cancelling reservation {waiting_id}: {e}")
        return jsonify({'success': False, 'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# GET /api/reservations/<waiting_id>/notes — ReservationNotesRetrieve
# =============================================================================

@reservations_bp.route('/<int:waiting_id>/notes')
@require_auth
@require_api_scope('reservations:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def reservation_notes_get(waiting_id):
    """
    Retrieve notes for a reservation via ReservationNotesRetrieve.

    Path parameter:
        waiting_id — the reservation WaitingID

    Query parameters:
        site_code — location code (required)
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        results = soap_client.call(
            operation="ReservationNotesRetrieve",
            parameters={
                "sLocationCode": site_code,
                "WaitingID": str(waiting_id),
            },
            soap_action=_cc_soap_action("ReservationNotesRetrieve"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'waiting_id': waiting_id,
            'notes': results or [],
            'count': len(results) if results else 0,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ReservationNotesRetrieve: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error retrieving notes for {waiting_id}: {e}")
        return jsonify({'error': 'Failed to retrieve notes'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# POST /api/reservations/<waiting_id>/notes — ReservationNoteInsert
# =============================================================================

@reservations_bp.route('/<int:waiting_id>/notes', methods=['POST'])
@require_auth
@require_api_scope('reservations:write')
@rate_limit_api(max_requests=20, window_seconds=60)
def reservation_notes_add(waiting_id):
    """
    Add a note to a reservation via ReservationNoteInsert.

    Path parameter:
        waiting_id — the reservation WaitingID

    JSON body:
        site_code — location code   [required]
        note      — note text       [required]
    """
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    note = _clamp(data.get('note', '').strip(), 1000)

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not note:
        return jsonify({'error': 'note is required'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        results = soap_client.call(
            operation="ReservationNoteInsert",
            parameters={
                "sLocationCode": site_code,
                "WaitingID": str(waiting_id),
                "sNote": note,
            },
            soap_action=_cc_soap_action("ReservationNoteInsert"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = None
        ret_msg = None
        if results:
            ret_code = results[0].get('Ret_Code')
            ret_msg = results[0].get('Ret_Msg')

        logger.info(f"Note inserted for reservation {waiting_id} at {site_code}")

        return jsonify({
            'success': True,
            'waiting_id': waiting_id,
            'ret_code': ret_code,
            'message': ret_msg or 'Note added',
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ReservationNoteInsert: {e}")
        return jsonify({'success': False, 'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error inserting note for {waiting_id}: {e}")
        return jsonify({'success': False, 'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# GET /api/reservations/fees — ReservationFeeRetrieve
# =============================================================================

@reservations_bp.route('/fees')
@require_auth
@require_api_scope('reservations:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def reservation_fee_retrieve():
    """
    Retrieve reservation fee configuration for a site.

    Query parameters:
        site_code — location code (required)
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        results = soap_client.call(
            operation="ReservationFeeRetrieve",
            parameters={
                "sLocationCode": site_code,
            },
            soap_action=_cc_soap_action("ReservationFeeRetrieve"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'site_code': site_code,
            'fees': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ReservationFeeRetrieve: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error retrieving fees for {site_code}: {e}")
        return jsonify({'error': 'Failed to retrieve fee information'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# POST /api/reservations/fee/add — record paid reservation fee (dummy CC)
# =============================================================================

@reservations_bp.route('/fee/add', methods=['POST'])
@require_auth
@require_api_scope('reservations:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def add_reservation_fee():
    """
    ReservationFeeAddWithSource_v2 — charge reservation fee with dummy CC.

    SiteLink has no payment processor, so dummy CC values pass validation.
    iCreditCardType=5 is the only value that works on LSETUP (0-4 fail).

    Use this AFTER Stripe has confirmed the customer's payment externally.

    JSON body:
        site_code       — location code             [required]
        tenant_id       — tenant ID                 [required]
        waiting_id      — reservation WaitingID     [required]
        billing_name    — billing name              [optional]
        billing_address — billing address           [optional]
        billing_zip     — billing postal code       [optional]
        test_mode       — dry run                   [optional, default: false]
    """
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    tenant_id, err = _safe_int(data.get('tenant_id'), min_val=1)
    if err:
        return jsonify({'error': f'tenant_id: {err}'}), 400

    waiting_id, err = _safe_int(data.get('waiting_id'), min_val=1)
    if err:
        return jsonify({'error': f'waiting_id: {err}'}), 400

    billing_name = (data.get('billing_name') or 'ESA Booking').strip()[:100]
    billing_address = (data.get('billing_address') or '').strip()[:200]
    billing_zip = (data.get('billing_zip') or '000000').strip()[:20]
    test_mode = bool(data.get('test_mode', False))

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        results = soap_client.call(
            operation="ReservationFeeAddWithSource_v2",
            parameters={
                "sLocationCode": site_code,
                "iTenantID": str(tenant_id),
                "iWaitingListID": str(waiting_id),
                "iCreditCardType": "5",
                "sCreditCardNumber": "4111111111111111",
                "sCreditCardCVV": "123",
                "dExpirationDate": "2030-01-01T00:00:00",
                "sBillingName": billing_name,
                "sBillingAddress": billing_address,
                "sBillingZipCode": billing_zip,
                "bTestMode": "true" if test_mode else "false",
                "iSource": "0",
            },
            soap_action=_cc_soap_action("ReservationFeeAddWithSource_v2"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"ReservationFeeAddWithSource_v2 failed: "
                         f"ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Reservation fee failed',
                            'detail': ret_msg}), 502

        logger.info(f"Reservation fee added: site={site_code} "
                    f"tenant={tenant_id} waiting={waiting_id}")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'waiting_id': waiting_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ReservationFeeAddWithSource_v2: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error ReservationFeeAddWithSource_v2: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# GET /api/reservations/discount-plans — active discounts from DB
# =============================================================================

@reservations_bp.route('/discount-plans', methods=['GET'])
@require_auth
@require_api_scope('reservations:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def discount_plans_list():
    """
    Retrieve active discount plans for a site from ccws_discount.

    DiscountPlansRetrieve SOAP returns empty — read from synced DB instead.
    Booking engine uses ConcessionID to apply discounts at reservation/move-in.

    Query params:
        site_code — location code [required]
    """
    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code query parameter is required'}), 400
    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    session = get_pbi_session()
    try:
        rows = session.execute(text("""
            SELECT cd."ConcessionID", cd."sPlanName", cd."sDescription",
                   cd."dcPCDiscount", cd."dcFixedDiscount", cd."iAmtType",
                   cd."bForAllUnits", cd."dPlanStrt", cd."dPlanEnd",
                   cd."bNeverExpires", cd."iExpirMonths"
            FROM ccws_discount cd
            JOIN siteinfo si ON cd."SiteID" = si."SiteID"
            WHERE si."SiteCode" = :site_code
              AND cd."dDisabled" IS NULL
              AND cd."dDeleted" IS NULL
            ORDER BY cd."ConcessionID"
        """), {"site_code": site_code}).fetchall()

        plans = [dict(row._mapping) for row in rows]

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'count': len(plans),
            'data': plans,
        })

    except Exception as e:
        logger.error(f"Error fetching discount plans for {site_code}: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


# =============================================================================
# POST /api/reservations/track — external reservation tracking
# =============================================================================

_VALID_TRACK_EVENT_TYPES = {'reservation', 'move_in'}
_VALID_EVENT_TYPES = {'move_in', 'cancellation', 'expiry'}
_EVENT_STATUS_MAP = {
    'move_in': ('moved_in', 'moved_in_at'),
    'cancellation': ('cancelled', 'cancelled_at'),
    'expiry': ('expired', 'expired_at'),
}


@reservations_bp.route('/track', methods=['POST'])
@require_auth
@require_api_scope('reservations:track')
@rate_limit_api(max_requests=30, window_seconds=60)
def reservation_track():
    """
    Push an external reservation/move-in record into the tracking table.

    JSON body:
        site_code    — location code          [required]
        unit_id      — unit ID                [required]
        waiting_id   — SiteLink WaitingID     [required]
        event_type   — "reservation" or "move_in" [default: "reservation"]
        ... plus optional fields (see plan)
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = _clamp(data.get('site_code', '').strip(), 10)
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    unit_id = data.get('unit_id')
    if not unit_id:
        return jsonify({'error': 'unit_id is required'}), 400
    unit_id, uid_err = _safe_int(unit_id, min_val=1)
    if uid_err:
        return jsonify({'error': f'unit_id: {uid_err}'}), 400

    waiting_id = data.get('waiting_id')
    if not waiting_id:
        return jsonify({'error': 'waiting_id is required'}), 400
    waiting_id, wid_err = _safe_int(waiting_id, min_val=1)
    if wid_err:
        return jsonify({'error': f'waiting_id: {wid_err}'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    event_type = _clamp(data.get('event_type', 'reservation'), 20).lower()
    if event_type not in _VALID_TRACK_EVENT_TYPES:
        return jsonify({'error': f'event_type must be one of: {", ".join(sorted(_VALID_TRACK_EVENT_TYPES))}'}), 400

    # Optional fields
    global_waiting_num = None
    if data.get('global_waiting_num') is not None:
        global_waiting_num, gwn_err = _safe_int(data['global_waiting_num'], min_val=0)
        if gwn_err:
            return jsonify({'error': f'global_waiting_num: {gwn_err}'}), 400

    tenant_id = None
    if data.get('tenant_id') is not None:
        tenant_id, tid_err = _safe_int(data['tenant_id'], min_val=0)
        if tid_err:
            return jsonify({'error': f'tenant_id: {tid_err}'}), 400

    quoted_rate = '0.00'
    if data.get('quoted_rate') is not None:
        quoted_rate, rate_err = _safe_rate(data['quoted_rate'])
        if rate_err:
            return jsonify({'error': f'quoted_rate: {rate_err}'}), 400

    concession_id = 0
    if data.get('concession_id') is not None:
        concession_id, cid_err = _safe_int(data['concession_id'], min_val=0)
        if cid_err:
            return jsonify({'error': f'concession_id: {cid_err}'}), 400

    needed_date = None
    if data.get('needed_date'):
        needed_date, nd_err = _require_date(data['needed_date'])
        if nd_err:
            return jsonify({'error': f'needed_date: {nd_err}'}), 400

    # Determine status based on event_type
    status = 'created' if event_type == 'reservation' else 'moved_in'

    api_key_id, api_user = _get_caller_info()

    try:
        session = get_pbi_session()
        try:
            # Build the upsert — same SQL as _record_reservation but with moved_in_at handling
            moved_in_clause = "NOW()" if event_type == 'move_in' else "NULL"
            result = session.execute(text(f"""
                INSERT INTO api_reservations (
                    site_code, unit_id, first_name, last_name, email, phone,
                    mobile, quoted_rate, concession_id, needed_date, source_name,
                    comment, tenant_id, waiting_id, global_waiting_num,
                    source, gclid, gid, botid, api_key_id, api_user,
                    reserved_at, moved_in_at, status
                ) VALUES (
                    :site_code, :unit_id, :first_name, :last_name, :email, :phone,
                    :mobile, :quoted_rate, :concession_id, :needed_date, :source_name,
                    :comment, :tenant_id, :waiting_id, :global_waiting_num,
                    :source, :gclid, :gid, :botid, :api_key_id, :api_user,
                    NOW(), {moved_in_clause}, :status
                )
                ON CONFLICT (site_code, waiting_id) WHERE waiting_id IS NOT NULL
                DO UPDATE SET
                    unit_id = EXCLUDED.unit_id,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    mobile = EXCLUDED.mobile,
                    quoted_rate = EXCLUDED.quoted_rate,
                    concession_id = EXCLUDED.concession_id,
                    needed_date = EXCLUDED.needed_date,
                    source_name = EXCLUDED.source_name,
                    comment = EXCLUDED.comment,
                    tenant_id = COALESCE(EXCLUDED.tenant_id, api_reservations.tenant_id),
                    global_waiting_num = COALESCE(EXCLUDED.global_waiting_num, api_reservations.global_waiting_num),
                    source = EXCLUDED.source,
                    gclid = COALESCE(EXCLUDED.gclid, api_reservations.gclid),
                    gid = COALESCE(EXCLUDED.gid, api_reservations.gid),
                    botid = COALESCE(EXCLUDED.botid, api_reservations.botid),
                    status = EXCLUDED.status,
                    moved_in_at = CASE WHEN EXCLUDED.status = 'moved_in' THEN NOW() ELSE api_reservations.moved_in_at END,
                    updated_at = NOW()
                RETURNING id
            """), {
                'site_code': site_code,
                'unit_id': unit_id,
                'first_name': _clamp(data.get('first_name', ''), 100),
                'last_name': _clamp(data.get('last_name', ''), 100),
                'email': _clamp(data.get('email', ''), 100) or None,
                'phone': _clamp(data.get('phone', ''), 20) or None,
                'mobile': _clamp(data.get('mobile', ''), 20) or None,
                'quoted_rate': quoted_rate,
                'concession_id': concession_id,
                'needed_date': needed_date,
                'source_name': _clamp(data.get('source_name', 'ESA Backend'), 64),
                'comment': _clamp(data.get('comment', ''), 500) or None,
                'tenant_id': tenant_id,
                'waiting_id': waiting_id,
                'global_waiting_num': global_waiting_num,
                'source': _clamp(data.get('source', 'api'), 50),
                'gclid': _clamp(data.get('gclid', ''), 255) or None,
                'gid': _clamp(data.get('gid', ''), 255) or None,
                'botid': _clamp(data.get('botid', ''), 255) or None,
                'api_key_id': api_key_id,
                'api_user': api_user,
                'status': status,
            })
            row = result.fetchone()
            session.commit()

            record_id = row[0] if row else None
            logger.info(
                f"Reservation tracked: id={record_id} site={site_code} "
                f"waiting_id={waiting_id} event={event_type}"
            )
            audit_log(
                'RESERVATION_TRACKED',
                f"site={site_code} waiting_id={waiting_id} event={event_type} "
                f"source={_sanitize_log(data.get('source', 'api'))}"
            )

            return jsonify({
                'success': True,
                'id': record_id,
                'event_type': event_type,
            })

        finally:
            session.close()

    except Exception as e:
        logger.error(f"Error tracking reservation: {e}")
        return jsonify({'error': 'Failed to track reservation'}), 500


# =============================================================================
# PUT /api/reservations/track/event — lifecycle event on tracked reservation
# =============================================================================

@reservations_bp.route('/track/event', methods=['PUT'])
@require_auth
@require_api_scope('reservations:track')
@rate_limit_api(max_requests=30, window_seconds=60)
def reservation_track_event():
    """
    Push a lifecycle event (move-in, cancellation, expiry) against an
    existing tracked reservation.

    JSON body:
        site_code   — location code          [required]
        waiting_id  — reservation WaitingID   [required]
        event_type  — move_in / cancellation / expiry [required]
        comment     — event notes (appended)  [optional]
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = _clamp(data.get('site_code', '').strip(), 10)
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    waiting_id = data.get('waiting_id')
    if not waiting_id:
        return jsonify({'error': 'waiting_id is required'}), 400
    waiting_id, wid_err = _safe_int(waiting_id, min_val=1)
    if wid_err:
        return jsonify({'error': f'waiting_id: {wid_err}'}), 400

    event_type = _clamp(data.get('event_type', '').strip(), 20).lower()
    if event_type not in _VALID_EVENT_TYPES:
        return jsonify({'error': f'event_type must be one of: {", ".join(sorted(_VALID_EVENT_TYPES))}'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    status, date_column = _EVENT_STATUS_MAP[event_type]
    if date_column not in _LIFECYCLE_DATE_COLUMNS:
        return jsonify({'error': 'Invalid event type'}), 400
    comment = _clamp(data.get('comment', ''), 500) or None

    try:
        session = get_pbi_session()
        try:
            # Build update with optional comment append
            # safe: date_column validated against _LIFECYCLE_DATE_COLUMNS allowlist above
            if comment:
                result = session.execute(text(f"""
                    UPDATE api_reservations
                    SET status = :status,
                        {date_column} = NOW(),
                        comment = CASE
                            WHEN comment IS NULL OR comment = '' THEN :comment
                            ELSE comment || ' | ' || :comment
                        END,
                        updated_at = NOW()
                    WHERE site_code = :site_code AND waiting_id = :waiting_id
                    RETURNING waiting_id, status, {date_column}
                """), {
                    'status': status,
                    'comment': comment,
                    'site_code': site_code,
                    'waiting_id': waiting_id,
                })
            else:
                result = session.execute(text(f"""
                    UPDATE api_reservations
                    SET status = :status,
                        {date_column} = NOW(),
                        updated_at = NOW()
                    WHERE site_code = :site_code AND waiting_id = :waiting_id
                    RETURNING waiting_id, status, {date_column}
                """), {
                    'status': status,
                    'site_code': site_code,
                    'waiting_id': waiting_id,
                })

            row = result.fetchone()
            session.commit()

            if not row:
                return jsonify({'error': 'Reservation not found'}), 404

            logger.info(
                f"Reservation event: site={site_code} waiting_id={waiting_id} "
                f"event={event_type} status={status}"
            )
            audit_log(
                'RESERVATION_EVENT',
                f"site={site_code} waiting_id={waiting_id} event={event_type}"
            )

            return jsonify({
                'success': True,
                'waiting_id': row[0],
                'status': row[1],
                date_column: row[2].isoformat() if row[2] else None,
            })

        finally:
            session.close()

    except Exception as e:
        logger.error(f"Error processing reservation event: {e}")
        return jsonify({'error': 'Failed to process event'}), 500


# =============================================================================
# POST /api/reservations/track/batch — bulk import tracked reservations
# =============================================================================

@reservations_bp.route('/track/batch', methods=['POST'])
@require_auth
@require_api_scope('reservations:track')
@rate_limit_api(max_requests=5, window_seconds=60)
def reservation_track_batch():
    """
    Batch import external reservation records.

    JSON body:
        records — array of reservation objects (max 100), each following
                  the same schema as POST /api/reservations/track
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    records = data.get('records')
    if not records or not isinstance(records, list):
        return jsonify({'error': 'records array is required'}), 400

    if len(records) > 100:
        return jsonify({'error': 'Maximum 100 records per batch'}), 400

    api_key_id, api_user = _get_caller_info()

    # Cache site code validations
    valid_sites = {}

    inserted = 0
    updated = 0
    errors = []

    session = get_pbi_session()
    try:
        for idx, rec in enumerate(records):
            if not isinstance(rec, dict):
                errors.append({'index': idx, 'error': 'Record must be an object'})
                continue

            # Validate required fields
            sc = _clamp(str(rec.get('site_code', '')).strip(), 10)
            if not sc:
                errors.append({'index': idx, 'error': 'site_code is required'})
                continue

            uid = rec.get('unit_id')
            if not uid:
                errors.append({'index': idx, 'error': 'unit_id is required'})
                continue
            uid, uid_err = _safe_int(uid, min_val=1)
            if uid_err:
                errors.append({'index': idx, 'error': f'unit_id: {uid_err}'})
                continue

            wid = rec.get('waiting_id')
            if not wid:
                errors.append({'index': idx, 'error': 'waiting_id is required'})
                continue
            wid, wid_err = _safe_int(wid, min_val=1)
            if wid_err:
                errors.append({'index': idx, 'error': f'waiting_id: {wid_err}'})
                continue

            # Validate site_code (cached)
            if sc not in valid_sites:
                valid_sites[sc] = _validate_site_code(sc) is not None
            if not valid_sites[sc]:
                errors.append({'index': idx, 'error': 'Invalid site_code'})
                continue

            event_type = _clamp(str(rec.get('event_type', 'reservation')), 20).lower()
            if event_type not in _VALID_TRACK_EVENT_TYPES:
                errors.append({'index': idx, 'error': f'Invalid event_type: {event_type}'})
                continue

            # Optional fields
            gwn = None
            if rec.get('global_waiting_num') is not None:
                gwn, gwn_err = _safe_int(rec['global_waiting_num'], min_val=0)
                if gwn_err:
                    errors.append({'index': idx, 'error': f'global_waiting_num: {gwn_err}'})
                    continue

            tid = None
            if rec.get('tenant_id') is not None:
                tid, tid_err = _safe_int(rec['tenant_id'], min_val=0)
                if tid_err:
                    errors.append({'index': idx, 'error': f'tenant_id: {tid_err}'})
                    continue

            qr = '0.00'
            if rec.get('quoted_rate') is not None:
                qr, qr_err = _safe_rate(rec['quoted_rate'])
                if qr_err:
                    errors.append({'index': idx, 'error': f'quoted_rate: {qr_err}'})
                    continue

            cid = 0
            if rec.get('concession_id') is not None:
                cid, cid_err = _safe_int(rec['concession_id'], min_val=0)
                if cid_err:
                    errors.append({'index': idx, 'error': f'concession_id: {cid_err}'})
                    continue

            nd = None
            if rec.get('needed_date'):
                nd, nd_err = _require_date(rec['needed_date'])
                if nd_err:
                    errors.append({'index': idx, 'error': f'needed_date: {nd_err}'})
                    continue

            status = 'created' if event_type == 'reservation' else 'moved_in'
            moved_in_clause = "NOW()" if event_type == 'move_in' else "NULL"

            try:
                result = session.execute(text(f"""
                    INSERT INTO api_reservations (
                        site_code, unit_id, first_name, last_name, email, phone,
                        mobile, quoted_rate, concession_id, needed_date, source_name,
                        comment, tenant_id, waiting_id, global_waiting_num,
                        source, gclid, gid, botid, api_key_id, api_user,
                        reserved_at, moved_in_at, status
                    ) VALUES (
                        :site_code, :unit_id, :first_name, :last_name, :email, :phone,
                        :mobile, :quoted_rate, :concession_id, :needed_date, :source_name,
                        :comment, :tenant_id, :waiting_id, :global_waiting_num,
                        :source, :gclid, :gid, :botid, :api_key_id, :api_user,
                        NOW(), {moved_in_clause}, :status
                    )
                    ON CONFLICT (site_code, waiting_id) WHERE waiting_id IS NOT NULL
                    DO UPDATE SET
                        unit_id = EXCLUDED.unit_id,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        email = EXCLUDED.email,
                        phone = EXCLUDED.phone,
                        mobile = EXCLUDED.mobile,
                        quoted_rate = EXCLUDED.quoted_rate,
                        concession_id = EXCLUDED.concession_id,
                        needed_date = EXCLUDED.needed_date,
                        source_name = EXCLUDED.source_name,
                        comment = EXCLUDED.comment,
                        tenant_id = COALESCE(EXCLUDED.tenant_id, api_reservations.tenant_id),
                        global_waiting_num = COALESCE(EXCLUDED.global_waiting_num, api_reservations.global_waiting_num),
                        source = EXCLUDED.source,
                        gclid = COALESCE(EXCLUDED.gclid, api_reservations.gclid),
                        gid = COALESCE(EXCLUDED.gid, api_reservations.gid),
                        botid = COALESCE(EXCLUDED.botid, api_reservations.botid),
                        status = EXCLUDED.status,
                        moved_in_at = CASE WHEN EXCLUDED.status = 'moved_in' THEN NOW() ELSE api_reservations.moved_in_at END,
                        updated_at = NOW()
                    RETURNING (xmax = 0) AS is_insert
                """), {
                    'site_code': sc,
                    'unit_id': uid,
                    'first_name': _clamp(rec.get('first_name', ''), 100),
                    'last_name': _clamp(rec.get('last_name', ''), 100),
                    'email': _clamp(rec.get('email', ''), 100) or None,
                    'phone': _clamp(rec.get('phone', ''), 20) or None,
                    'mobile': _clamp(rec.get('mobile', ''), 20) or None,
                    'quoted_rate': qr,
                    'concession_id': cid,
                    'needed_date': nd,
                    'source_name': _clamp(rec.get('source_name', 'ESA Backend'), 64),
                    'comment': _clamp(rec.get('comment', ''), 500) or None,
                    'tenant_id': tid,
                    'waiting_id': wid,
                    'global_waiting_num': gwn,
                    'source': _clamp(rec.get('source', 'api'), 50),
                    'gclid': _clamp(rec.get('gclid', ''), 255) or None,
                    'gid': _clamp(rec.get('gid', ''), 255) or None,
                    'botid': _clamp(rec.get('botid', ''), 255) or None,
                    'api_key_id': api_key_id,
                    'api_user': api_user,
                    'status': status,
                })
                row = result.fetchone()
                if row and row[0]:
                    inserted += 1
                else:
                    updated += 1
            except Exception as e:
                logger.warning(f"Batch record {idx} failed: {e}")
                errors.append({'index': idx, 'error': 'Database error'})

        session.commit()

        logger.info(
            f"Batch track: {inserted} inserted, {updated} updated, "
            f"{len(errors)} errors"
        )
        audit_log(
            'RESERVATION_BATCH_TRACKED',
            f"inserted={inserted} updated={updated} errors={len(errors)}"
        )

        return jsonify({
            'success': True,
            'processed': inserted + updated,
            'inserted': inserted,
            'updated': updated,
            'errors': errors,
        })

    except Exception as e:
        logger.error(f"Error in batch track: {e}")
        return jsonify({'error': 'Failed to process batch'}), 500

    finally:
        session.close()


# =============================================================================
# GET /api/reservations/move-in/cost — MoveInCostRetrieve variants
# =============================================================================

_VALID_PAY_METHODS = {0, 1, 2, 3, 4}  # 0=none, 1=CC, 2=cash, 3=check, 4=ACH

_COST_VARIANTS = {
    'standard',         # MoveInCostRetrieveWithDiscount_v4 (default)
    '28day',            # MoveInCostRetrieveWithDiscount_28DayBilling_v3
    'reservation',      # MoveInCostRetrieveWithDiscount_Reservation_v4
    'push_rate',        # MoveInCostRetrieveWithPushRate_v2
}


@reservations_bp.route('/move-in/cost')
@require_auth
@require_api_scope('reservations:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def move_in_cost():
    """
    Get move-in cost breakdown via SOAP MoveInCostRetrieve variants.

    Returns multiple charge rows (rent, admin fee, deposit, etc.).
    Sum `total` across all rows for the exact payment amount required
    by MoveInReservation_v6 / MoveInWithDiscount_v7.

    Query parameters:
        site_code      — location code             [required]
        unit_id        — unit ID                    [required]
        move_in_date   — move-in date (YYYY-MM-DD)  [default: tomorrow]
        concession_id  — discount plan ID           [default: 0]
        insurance_id   — insurance coverage ID      [default: 0]
        variant        — cost calculation variant   [default: standard]
                         standard     — MoveInCostRetrieveWithDiscount_v4
                         28day        — MoveInCostRetrieveWithDiscount_28DayBilling_v3
                         reservation  — MoveInCostRetrieveWithDiscount_Reservation_v4
                                        (requires waiting_id)
                         push_rate    — MoveInCostRetrieveWithPushRate_v2
        waiting_id     — reservation WaitingID      [required for variant=reservation]
        promo_id       — promo global number        [default: 0, for variant=reservation]
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    unit_id = request.args.get('unit_id')

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not unit_id:
        return jsonify({'error': 'unit_id is required'}), 400

    unit_id, uid_err = _safe_int(unit_id, min_val=1)
    if uid_err:
        return jsonify({'error': f'unit_id: {uid_err}'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    move_in_date = _parse_date(request.args.get('move_in_date'), 1)

    concession_id, cid_err = _safe_int(request.args.get('concession_id', 0), min_val=0)
    if cid_err:
        return jsonify({'error': f'concession_id: {cid_err}'}), 400

    insurance_id, iid_err = _safe_int(request.args.get('insurance_id', 0), min_val=0)
    if iid_err:
        return jsonify({'error': f'insurance_id: {iid_err}'}), 400

    variant = request.args.get('variant', 'standard').strip().lower()
    if variant not in _COST_VARIANTS:
        return jsonify({'error': f'variant must be one of: {", ".join(sorted(_COST_VARIANTS))}'}), 400

    # Reservation variant requires waiting_id
    waiting_id = 0
    promo_id = 0
    if variant == 'reservation':
        wid_raw = request.args.get('waiting_id')
        if not wid_raw:
            return jsonify({'error': 'waiting_id is required for variant=reservation'}), 400
        waiting_id, wid_err = _safe_int(wid_raw, min_val=1)
        if wid_err:
            return jsonify({'error': f'waiting_id: {wid_err}'}), 400
        promo_id, _ = _safe_int(request.args.get('promo_id', 0), min_val=0)

    # Build SOAP operation + parameters based on variant
    if variant == '28day':
        operation = "MoveInCostRetrieveWithDiscount_28DayBilling_v3"
        params = {
            "sLocationCode": site_code,
            "iUnitID": str(unit_id),
            "dMoveInDate": move_in_date,
            "InsuranceCoverageID": str(insurance_id),
            "ConcessionPlanID": str(concession_id),
            "ChannelType": "0",
            "bApplyInsuranceCredit": "false",
        }
    elif variant == 'reservation':
        operation = "MoveInCostRetrieveWithDiscount_Reservation_v4"
        params = {
            "sLocationCode": site_code,
            "iUnitID": str(unit_id),
            "dMoveInDate": move_in_date,
            "InsuranceCoverageID": str(insurance_id),
            "ConcessionPlanID": str(concession_id),
            "WaitingID": str(waiting_id),
            "bApplyInsuranceCredit": "false",
            "iPromoGlobalNum": str(promo_id),
            "sCreditCardNum": "",
        }
    elif variant == 'push_rate':
        operation = "MoveInCostRetrieveWithPushRate_v2"
        params = {
            "sLocationCode": site_code,
            "iUnitID": str(unit_id),
            "dMoveInDate": move_in_date,
            "InsuranceCoverageID": str(insurance_id),
            "ConcessionPlanID": str(concession_id),
            "bApplyInsuranceCredit": "false",
        }
    else:  # standard
        operation = "MoveInCostRetrieveWithDiscount_v4"
        params = {
            "sLocationCode": site_code,
            "iUnitID": str(unit_id),
            "dMoveInDate": move_in_date,
            "InsuranceCoverageID": str(insurance_id),
            "ConcessionPlanID": str(concession_id),
            "ChannelType": "0",
            "bApplyInsuranceCredit": "false",
            "sCreditCardNum": "",
        }

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        results = soap_client.call(
            operation=operation,
            parameters=params,
            soap_action=_cc_soap_action(operation),
            namespace=CC_NS,
            result_tag="Table",
        )

        if not results:
            return jsonify({
                'site_code': site_code,
                'unit_id': unit_id,
                'variant': variant,
                'operation': operation,
                'charges': [],
                'total': 0,
                'move_in_date': move_in_date,
            })

        charges = []
        total = 0
        for row in results:
            charge_amount = float(row.get('ChargeAmount', 0))
            tax1 = float(row.get('TaxAmount', 0))
            tax2 = float(row.get('TaxAmount2', 0))
            line_total = float(row.get('dcTotal', 0))
            total += line_total

            charges.append({
                'description': row.get('ChargeDescription', ''),
                'amount': charge_amount,
                'tax': round(tax1 + tax2, 2),
                'total': line_total,
                'required': row.get('bMoveInRequired') == 'true',
                'start_date': row.get('StartDate'),
                'end_date': row.get('EndDate'),
            })

        return jsonify({
            'site_code': site_code,
            'unit_id': unit_id,
            'variant': variant,
            'operation': operation,
            'unit_name': results[0].get('UnitName', ''),
            'unit_type': results[0].get('TypeName', ''),
            'push_rate': float(results[0].get('dcPushRate', 0)),
            'tenant_rate': float(results[0].get('dcTenantRate', 0)),
            'web_rate': float(results[0].get('WebRate', 0)),
            'discount': float(results[0].get('dcDiscount', 0)),
            'concession_id': int(results[0].get('ConcessionID', -999)),
            'charges': charges,
            'total': round(total, 2),
            'move_in_date': move_in_date,
            'currency_decimals': int(results[0].get('iCurrencyDecimalPlaces', 2)),
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault MoveInCostRetrieve: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error MoveInCostRetrieve: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# POST /api/reservations/move-in — MoveInReservation_v6
# =============================================================================

@reservations_bp.route('/move-in', methods=['POST'])
@require_auth
@require_api_scope('reservations:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def move_in_reservation():
    """
    Move-in from an existing reservation via MoveInReservation_v6.

    Supports CC bypass via iPayMethod=2 (cash). Payment amount must
    match the exact total from GET /api/reservations/move-in/cost.

    JSON body:
        site_code       — location code                [required]
        waiting_id      — reservation WaitingID         [required]
        tenant_id       — tenant ID                     [required]
        unit_id         — unit ID                       [required]
        payment_amount  — exact total from cost API     [required]
        start_date      — lease start (YYYY-MM-DD)      [default: tomorrow]
        end_date        — lease end (YYYY-MM-DD)        [default: start+365]
        pay_method      — 1=CC, 2=cash, 3=check, 4=ACH [default: 2]
        concession_id   — discount plan ID              [default: 0]
        insurance_id    — insurance coverage ID          [default: 0]
        source_id       — source ID                     [default: 0]
        test_mode       — true for dry run              [default: false]
    """
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    # Required fields
    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    waiting_id = data.get('waiting_id')
    if not waiting_id:
        return jsonify({'error': 'waiting_id is required'}), 400
    waiting_id, wid_err = _safe_int(waiting_id, min_val=1)
    if wid_err:
        return jsonify({'error': f'waiting_id: {wid_err}'}), 400

    tenant_id = data.get('tenant_id')
    if not tenant_id:
        return jsonify({'error': 'tenant_id is required'}), 400
    tenant_id, tid_err = _safe_int(tenant_id, min_val=1)
    if tid_err:
        return jsonify({'error': f'tenant_id: {tid_err}'}), 400

    unit_id = data.get('unit_id')
    if not unit_id:
        return jsonify({'error': 'unit_id is required'}), 400
    unit_id, uid_err = _safe_int(unit_id, min_val=1)
    if uid_err:
        return jsonify({'error': f'unit_id: {uid_err}'}), 400

    payment_amount = data.get('payment_amount')
    if payment_amount is None:
        return jsonify({'error': 'payment_amount is required'}), 400
    try:
        payment_amount = float(payment_amount)
        if payment_amount <= 0:
            return jsonify({'error': 'payment_amount must be greater than zero'}), 400
    except (ValueError, TypeError):
        return jsonify({'error': 'payment_amount must be a number'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    # Optional fields
    start_date = _parse_date(data.get('start_date'), 1)
    end_default = (datetime.strptime(start_date, '%Y-%m-%d').date() + timedelta(days=365)).isoformat()
    end_date = _parse_date(data.get('end_date'), 365) if data.get('end_date') else end_default

    pay_method, pm_err = _safe_int(data.get('pay_method', 2), min_val=0, max_val=4)
    if pm_err:
        return jsonify({'error': f'pay_method: {pm_err}'}), 400
    if pay_method not in _VALID_PAY_METHODS:
        return jsonify({'error': 'pay_method must be 0-4'}), 400

    concession_id, cid_err = _safe_int(data.get('concession_id', 0), min_val=0)
    if cid_err:
        return jsonify({'error': f'concession_id: {cid_err}'}), 400

    insurance_id, iid_err = _safe_int(data.get('insurance_id', 0), min_val=0)
    if iid_err:
        return jsonify({'error': f'insurance_id: {iid_err}'}), 400

    source_id, sid_err = _safe_int(data.get('source_id', 0), min_val=0)
    if sid_err:
        return jsonify({'error': f'source_id: {sid_err}'}), 400

    test_mode = str(data.get('test_mode', False)).lower() in ('true', '1', 'yes')

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        results = soap_client.call(
            operation="MoveInReservation_v6",
            parameters={
                "sLocationCode": site_code,
                "WaitingID": str(waiting_id),
                "TenantID": str(tenant_id),
                "UnitID": str(unit_id),
                "dStartDate": start_date,
                "dEndDate": end_date,
                "dcPaymentAmount": f"{payment_amount:.2f}",
                "iCreditCardType": "0",
                "sCreditCardNumber": "",
                "sCreditCardCVV": "",
                "dExpirationDate": "2030-01-01T00:00:00",
                "sBillingName": "",
                "sBillingAddress": "",
                "sBillingZipCode": "",
                "InsuranceCoverageID": str(insurance_id),
                "ConcessionPlanID": str(concession_id),
                "iPayMethod": str(pay_method),
                "sABARoutingNum": "",
                "sAccountNum": "",
                "iAccountType": "0",
                "iSource": str(source_id),
                "bTestMode": str(test_mode).lower(),
                "bApplyInsuranceCredit": "false",
                "iPromoGlobalNum": "0",
            },
            soap_action=_cc_soap_action("MoveInReservation_v6"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = None
        lease_num = None
        ret_msg = None
        if results:
            ret_code = results[0].get('Ret_Code')
            lease_num = results[0].get('iLeaseNum')
            ret_msg = results[0].get('Ret_Msg')

        success = ret_code is not None and int(ret_code) > 0

        if success:
            logger.info(
                f"MoveInReservation_v6 site={site_code} unit={unit_id} "
                f"tenant={tenant_id} waiting_id={waiting_id}: "
                f"ledger_id={ret_code} lease_num={lease_num} test_mode={test_mode}"
            )
            audit_log(
                'MOVE_IN_COMPLETED',
                f"site={site_code} unit={unit_id} tenant={tenant_id} "
                f"waiting_id={waiting_id} ledger_id={ret_code} "
                f"pay_method={pay_method} test_mode={test_mode}"
            )

            # Update tracking table
            _update_reservation_status(site_code, waiting_id, 'moved_in', 'moved_in_at')
        else:
            logger.warning(
                f"MoveInReservation_v6 failed: site={site_code} unit={unit_id} "
                f"ret_code={ret_code} ret_msg={ret_msg}"
            )

        return jsonify({
            'success': success,
            'site_code': site_code,
            'unit_id': unit_id,
            'tenant_id': tenant_id,
            'waiting_id': waiting_id,
            'ledger_id': int(ret_code) if success else None,
            'lease_num': int(lease_num) if lease_num else None,
            'ret_code': ret_code,
            'message': ret_msg if not success else 'Move-in completed',
            'test_mode': test_mode,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault MoveInReservation_v6: {e}")
        return jsonify({'success': False, 'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error MoveInReservation_v6: {e}")
        return jsonify({'success': False, 'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# POST /api/reservations/move-in/direct — MoveInWithDiscount_v7
# =============================================================================

@reservations_bp.route('/move-in/direct', methods=['POST'])
@require_auth
@require_api_scope('reservations:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def move_in_direct():
    """
    Direct move-in via MoveInWithDiscount_v7 (no reservation required).

    Supports CC bypass via pay_method=2 (cash). Payment amount must
    match the exact total from GET /api/reservations/move-in/cost.

    JSON body:
        site_code           — location code                [required]
        tenant_id           — tenant ID                     [required]
        unit_id             — unit ID                       [required]
        payment_amount      — exact total from cost API     [required]
        billing_frequency   — billing frequency (site-specific) [required]
        start_date          — lease start (YYYY-MM-DD)      [default: tomorrow]
        end_date            — lease end (YYYY-MM-DD)        [default: start+365]
        pay_method          — 1=CC, 2=cash, 3=check, 4=ACH [default: 2]
        concession_id       — discount plan ID              [default: 0]
        insurance_id        — insurance coverage ID          [default: 0]
        source_id           — source ID                     [default: 0]
        source_name         — source label                  [default: "ESA Backend"]
        use_push_rate       — use push rate                 [default: false]
        waiting_id          — link to reservation           [default: 0]
        test_mode           — true for dry run              [default: false]
    """
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    # Required fields
    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    tenant_id = data.get('tenant_id')
    if not tenant_id:
        return jsonify({'error': 'tenant_id is required'}), 400
    tenant_id, tid_err = _safe_int(tenant_id, min_val=1)
    if tid_err:
        return jsonify({'error': f'tenant_id: {tid_err}'}), 400

    unit_id = data.get('unit_id')
    if not unit_id:
        return jsonify({'error': 'unit_id is required'}), 400
    unit_id, uid_err = _safe_int(unit_id, min_val=1)
    if uid_err:
        return jsonify({'error': f'unit_id: {uid_err}'}), 400

    payment_amount = data.get('payment_amount')
    if payment_amount is None:
        return jsonify({'error': 'payment_amount is required'}), 400
    try:
        payment_amount = float(payment_amount)
        if payment_amount <= 0:
            return jsonify({'error': 'payment_amount must be greater than zero'}), 400
    except (ValueError, TypeError):
        return jsonify({'error': 'payment_amount must be a number'}), 400

    billing_freq = data.get('billing_frequency')
    if billing_freq is None:
        return jsonify({'error': 'billing_frequency is required (site-specific value)'}), 400
    billing_freq, bf_err = _safe_int(billing_freq, min_val=0)
    if bf_err:
        return jsonify({'error': f'billing_frequency: {bf_err}'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    # Optional fields
    start_date = _parse_date(data.get('start_date'), 1)
    end_default = (datetime.strptime(start_date, '%Y-%m-%d').date() + timedelta(days=365)).isoformat()
    end_date = _parse_date(data.get('end_date'), 365) if data.get('end_date') else end_default

    pay_method, pm_err = _safe_int(data.get('pay_method', 2), min_val=0, max_val=4)
    if pm_err:
        return jsonify({'error': f'pay_method: {pm_err}'}), 400

    concession_id, cid_err = _safe_int(data.get('concession_id', 0), min_val=0)
    if cid_err:
        return jsonify({'error': f'concession_id: {cid_err}'}), 400

    insurance_id, iid_err = _safe_int(data.get('insurance_id', 0), min_val=0)
    if iid_err:
        return jsonify({'error': f'insurance_id: {iid_err}'}), 400

    source_id, sid_err = _safe_int(data.get('source_id', 0), min_val=0)
    if sid_err:
        return jsonify({'error': f'source_id: {sid_err}'}), 400

    waiting_id, wid_err = _safe_int(data.get('waiting_id', 0), min_val=0)
    if wid_err:
        return jsonify({'error': f'waiting_id: {wid_err}'}), 400

    source_name = _clamp(data.get('source_name', 'ESA Backend'), 64)
    use_push_rate = str(data.get('use_push_rate', False)).lower() in ('true', '1', 'yes')
    test_mode = str(data.get('test_mode', False)).lower() in ('true', '1', 'yes')

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()
        results = soap_client.call(
            operation="MoveInWithDiscount_v7",
            parameters={
                "sLocationCode": site_code,
                "TenantID": str(tenant_id),
                "sAccessCode": "",
                "UnitID": str(unit_id),
                "dStartDate": start_date,
                "dEndDate": end_date,
                "dcPaymentAmount": f"{payment_amount:.2f}",
                "iCreditCardType": "0",
                "sCreditCardNumber": "",
                "sCreditCardCVV": "",
                "sCCTrack2": "",
                "dExpirationDate": "2030-01-01T00:00:00",
                "sBillingName": "",
                "sBillingAddress": "",
                "sBillingZipCode": "",
                "InsuranceCoverageID": str(insurance_id),
                "ConcessionPlanID": str(concession_id),
                "iSource": str(source_id),
                "sSource": source_name,
                "bUsePushRate": str(use_push_rate).lower(),
                "iPayMethod": str(pay_method),
                "sABARoutingNum": "",
                "sAccountNum": "",
                "iAccountType": "0",
                "iKeypadZoneID": "0",
                "iTimeZoneID": "0",
                "iBillingFrequency": str(billing_freq),
                "WaitingID": str(waiting_id),
                "ChannelType": "0",
                "bTestMode": str(test_mode).lower(),
                "bApplyInsuranceCredit": "false",
            },
            soap_action=_cc_soap_action("MoveInWithDiscount_v7"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = None
        ret_msg = None
        if results:
            ret_code = results[0].get('Ret_Code')
            ret_msg = results[0].get('Ret_Msg')

        success = ret_code is not None and int(ret_code) > 0

        if success:
            logger.info(
                f"MoveInWithDiscount_v7 site={site_code} unit={unit_id} "
                f"tenant={tenant_id}: ledger_id={ret_code} test_mode={test_mode}"
            )
            audit_log(
                'MOVE_IN_COMPLETED',
                f"site={site_code} unit={unit_id} tenant={tenant_id} "
                f"ledger_id={ret_code} pay_method={pay_method} "
                f"waiting_id={waiting_id} test_mode={test_mode}"
            )

            # Update tracking table if linked to a reservation
            if waiting_id:
                _update_reservation_status(site_code, waiting_id, 'moved_in', 'moved_in_at')
        else:
            logger.warning(
                f"MoveInWithDiscount_v7 failed: site={site_code} unit={unit_id} "
                f"ret_code={ret_code} ret_msg={ret_msg}"
            )

        return jsonify({
            'success': success,
            'site_code': site_code,
            'unit_id': unit_id,
            'tenant_id': tenant_id,
            'ledger_id': int(ret_code) if success else None,
            'waiting_id': waiting_id if waiting_id else None,
            'ret_code': ret_code,
            'message': ret_msg if not success else 'Move-in completed',
            'test_mode': test_mode,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault MoveInWithDiscount_v7: {e}")
        return jsonify({'success': False, 'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error MoveInWithDiscount_v7: {e}")
        return jsonify({'success': False, 'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# GET /api/reservations/insurance-coverage — list insurance coverage options
# =============================================================================

@reservations_bp.route('/insurance-coverage', methods=['GET'])
@require_auth
@require_api_scope('reservations:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def insurance_coverage_retrieve():
    """
    Retrieve available insurance coverage options for a site.

    Query params:
        site_code — location code [required]
        unit_id   — filter by unit ID [optional]
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code query parameter is required'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    unit_id = request.args.get('unit_id', '0').strip()
    unit_id_val, uid_err = _safe_int(unit_id, default=0, min_val=0)
    if uid_err:
        return jsonify({'error': f'unit_id: {uid_err}'}), 400

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()

        results = soap_client.call(
            operation="InsuranceCoverageRetrieve_V2",
            parameters={
                "sLocationCode": site_code,
            },
            soap_action=_cc_soap_action("InsuranceCoverageRetrieve_V2"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'count': len(results),
            'data': results,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault InsuranceCoverageRetrieve_V2: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error InsuranceCoverageRetrieve_V2: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()


# =============================================================================
# GET /api/reservations/insurance-minimums — insurance coverage minimums
# =============================================================================

@reservations_bp.route('/insurance-minimums', methods=['GET'])
@require_auth
@require_api_scope('reservations:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def insurance_coverage_minimums():
    """
    Retrieve insurance coverage minimum requirements for a site.

    Query params:
        site_code — location code [required]
    """
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code query parameter is required'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = None
    try:
        soap_client = _get_cc_soap_client()

        results = soap_client.call(
            operation="InsuranceCoverageMinimumsRetrieve",
            parameters={
                "sLocationCode": site_code,
            },
            soap_action=_cc_soap_action("InsuranceCoverageMinimumsRetrieve"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'count': len(results),
            'data': results,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault InsuranceCoverageMinimumsRetrieve: {e}")
        return jsonify({'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error InsuranceCoverageMinimumsRetrieve: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500

    finally:
        if soap_client:
            soap_client.close()
