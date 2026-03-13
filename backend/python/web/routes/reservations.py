"""
Reservations API routes.

Dedicated blueprint for SOAP CallCenterWs reservation operations:
- Create reservation (ReservationNewWithSource_v6)
- List reservations (ReservationList_v3)
- Update reservation (ReservationUpdate_v4)
- Retrieve/insert notes
- Send confirmation email
- Fee retrieve
"""

import logging
from datetime import datetime, timedelta

from flask import Blueprint, jsonify, request, current_app
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api
from web.utils.audit import audit_log, AuditEvent

logger = logging.getLogger(__name__)

reservations_bp = Blueprint('reservations', __name__, url_prefix='/api/reservations')


# =============================================================================
# PBI Database Session
# =============================================================================

_pbi_engine = None
_pbi_session_factory = None


def get_pbi_session():
    """Get PBI database session."""
    global _pbi_engine, _pbi_session_factory
    if _pbi_engine is None:
        from common.config_loader import get_database_url
        from sqlalchemy import create_engine
        pbi_url = get_database_url('pbi')
        _pbi_engine = create_engine(pbi_url)
        _pbi_session_factory = sessionmaker(bind=_pbi_engine)
    return _pbi_session_factory()


# =============================================================================
# SOAP Client Helper
# =============================================================================

def _get_cc_soap_client():
    """Create a CallCenterWs SOAP client."""
    from common.config import DataLayerConfig
    from common.soap_client import SOAPClient

    config = DataLayerConfig.from_env()
    if not config.soap:
        raise RuntimeError("SOAP configuration not available")

    cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    return SOAPClient(
        base_url=cc_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries,
    )


CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"


# =============================================================================
# POST /api/reservations/quick — MakeReservation (simple walk-in)
# =============================================================================

@reservations_bp.route('/quick', methods=['POST'])
@require_auth
@require_api_scope('inventory:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def reservation_quick():
    """
    Quick reservation via MakeReservation SOAP endpoint.
    Simpler than ReservationNewWithSource_v6 — accepts name/phone directly.
    Used by the unit availability tool page for walk-in reservations.

    JSON body:
        site_code    — location code (e.g. "LSETUP")  [required]
        unit_id      — unit ID to reserve              [required]
        tenant_name  — first name                      [required]
        tenant_last  — last name                       [required]
        phone        — contact phone                   [required]
        email        — contact email                   [optional]
        notes        — reservation notes               [optional]
    """
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    unit_id = data.get('unit_id')
    tenant_name = data.get('tenant_name', '').strip()
    tenant_last = data.get('tenant_last', '').strip()
    phone = data.get('phone', '').strip()
    email = data.get('email', '').strip()
    notes = data.get('notes', '').strip()

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not unit_id:
        return jsonify({'error': 'unit_id is required'}), 400
    if not tenant_name:
        return jsonify({'error': 'tenant_name is required'}), 400
    if not tenant_last:
        return jsonify({'error': 'tenant_last is required'}), 400
    if not phone:
        return jsonify({'error': 'phone is required'}), 400

    try:
        unit_id = int(unit_id)
    except (ValueError, TypeError):
        return jsonify({'error': 'unit_id must be an integer'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = _get_cc_soap_client()
    try:
        results = soap_client.call(
            operation="MakeReservation",
            parameters={
                "sLocationCode": site_code,
                "iUnitID": str(unit_id),
                "sFirstName": tenant_name,
                "sLastName": tenant_last,
                "sPhone": phone,
                "sEmail": email or "",
                "sNote": notes or "",
            },
            soap_action=_cc_soap_action("MakeReservation"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = None
        ret_msg = None
        if results:
            ret_code = results[0].get('Ret_Code')
            ret_msg = results[0].get('Ret_Msg')

        logger.info(
            f"MakeReservation unit={unit_id} site={site_code}: "
            f"code={ret_code}, msg={ret_msg}"
        )
        audit_log(
            'RESERVATION_CREATED',
            f"site={site_code} unit={unit_id} name={tenant_name} {tenant_last}"
        )

        return jsonify({
            'success': True,
            'site_code': site_code,
            'unit_id': unit_id,
            'ret_code': ret_code,
            'message': ret_msg or 'Reservation submitted',
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault MakeReservation: {e}")
        return jsonify({'success': False, 'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error MakeReservation: {e}")
        return jsonify({'success': False, 'error': 'An internal error occurred'}), 500

    finally:
        soap_client.close()


def _cc_soap_action(operation):
    return f"{CC_NS}/{operation}"


def _validate_site_code(site_code):
    """Validate site_code exists in SiteInfo. Returns site or None."""
    from common.models import SiteInfo
    pbi_session = get_pbi_session()
    try:
        return pbi_session.query(SiteInfo).filter_by(SiteCode=site_code).first()
    finally:
        pbi_session.close()


# =============================================================================
# POST /api/reservations/create — ReservationNewWithSource_v6
# =============================================================================

@reservations_bp.route('/create', methods=['POST'])
@require_auth
@require_api_scope('inventory:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def reservation_create():
    """
    Create a new reservation via ReservationNewWithSource_v6.

    JSON body:
        site_code      — location code (e.g. "LSETUP")  [required]
        unit_id        — unit ID to reserve              [required]
        tenant_id      — tenant ID (0 for walk-in)       [default: "0"]
        needed_date    — move-in date (YYYY-MM-DD)       [default: today+1]
        expires_date   — expiry date (YYYY-MM-DD)        [default: today+14]
        followup_date  — follow-up date (YYYY-MM-DD)     [default: today+3]
        quoted_rate    — quoted rate                      [default: 0]
        comment        — reservation comment              [default: ""]
        source_id      — source ID                        [default: 0]
        source_name    — source name                      [default: "ESA Backend"]
        rental_type_id — rental type ID (QTRentalTypeID)  [default: 0]
        inquiry_type   — inquiry type                     [default: 0]
        tracking_code  — tracking code                    [default: ""]
        caller_id      — caller ID                        [default: ""]
        concession_id  — concession/discount plan ID      [default: 0]
        promo_global   — promo global number              [default: 0]
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

    # Defaults
    today = datetime.utcnow().date()
    needed = data.get('needed_date') or (today + timedelta(days=1)).isoformat()
    expires = data.get('expires_date') or (today + timedelta(days=14)).isoformat()
    followup = data.get('followup_date') or (today + timedelta(days=3)).isoformat()

    soap_client = _get_cc_soap_client()
    try:
        results = soap_client.call(
            operation="ReservationNewWithSource_v6",
            parameters={
                "sLocationCode": site_code,
                "sTenantID": str(data.get('tenant_id', '0')),
                "sUnitID": str(unit_id),
                "dNeeded": needed,
                "sComment": data.get('comment', ''),
                "iSource": str(data.get('source_id', 0)),
                "sSource": data.get('source_name', 'ESA Backend'),
                "QTRentalTypeID": str(data.get('rental_type_id', 0)),
                "iInquiryType": str(data.get('inquiry_type', 0)),
                "dcQuotedRate": str(data.get('quoted_rate', 0)),
                "dExpires": expires,
                "dFollowUp": followup,
                "sTrackingCode": data.get('tracking_code', ''),
                "sCallerID": data.get('caller_id', ''),
                "ConcessionID": str(data.get('concession_id', 0)),
                "PromoGlobalNum": str(data.get('promo_global', 0)),
            },
            soap_action=_cc_soap_action("ReservationNewWithSource_v6"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = None
        ret_msg = None
        waiting_id = None
        if results:
            ret_code = results[0].get('Ret_Code')
            ret_msg = results[0].get('Ret_Msg')
            waiting_id = results[0].get('WaitingID') or results[0].get('iWaitingID')

        logger.info(
            f"ReservationNewWithSource_v6 unit={unit_id} site={site_code}: "
            f"code={ret_code}, msg={ret_msg}, waiting_id={waiting_id}"
        )
        audit_log(
            'RESERVATION_CREATED',
            f"site={site_code} unit={unit_id} waiting_id={waiting_id}"
        )

        return jsonify({
            'success': True,
            'site_code': site_code,
            'unit_id': unit_id,
            'waiting_id': waiting_id,
            'ret_code': ret_code,
            'message': ret_msg or 'Reservation created',
            'raw': results[0] if results else None,
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
        soap_client.close()


# =============================================================================
# GET /api/reservations/list — ReservationList_v3
# =============================================================================

@reservations_bp.route('/list')
@require_auth
@require_api_scope('inventory:read')
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

    soap_client = _get_cc_soap_client()
    try:
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
        soap_client.close()


# =============================================================================
# GET /api/reservations/<waiting_id> — single reservation detail
# =============================================================================

@reservations_bp.route('/<int:waiting_id>')
@require_auth
@require_api_scope('inventory:read')
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

    soap_client = _get_cc_soap_client()
    try:
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
        soap_client.close()


# =============================================================================
# PUT /api/reservations/<waiting_id> — ReservationUpdate_v4
# =============================================================================

@reservations_bp.route('/<int:waiting_id>', methods=['PUT'])
@require_auth
@require_api_scope('inventory:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def reservation_update(waiting_id):
    """
    Update a reservation via ReservationUpdate_v4.

    Path parameter:
        waiting_id — the reservation WaitingID

    JSON body:
        site_code              — location code                     [required]
        tenant_id              — tenant ID                         [default: "0"]
        unit_id                — unit ID                           [required]
        needed_date            — needed date (YYYY-MM-DD)          [required]
        comment                — comment                           [default: ""]
        status                 — reservation status code           [default: 0]
        followup               — enable follow-up (bool)           [default: false]
        followup_date          — follow-up date (YYYY-MM-DD)       [default: ""]
        followup_last_date     — last follow-up date               [default: ""]
        inquiry_type           — inquiry type                      [default: 0]
        quoted_rate            — quoted rate                       [default: 0]
        expires_date           — expiry date (YYYY-MM-DD)          [default: ""]
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

    needed_date = data.get('needed_date', '')
    if not needed_date:
        return jsonify({'error': 'needed_date is required'}), 400

    soap_client = _get_cc_soap_client()
    try:
        results = soap_client.call(
            operation="ReservationUpdate_v4",
            parameters={
                "sLocationCode": site_code,
                "WaitingID": str(waiting_id),
                "sTenantID": str(data.get('tenant_id', '0')),
                "sUnitID": str(unit_id),
                "dNeeded": needed_date,
                "sComment": data.get('comment', ''),
                "iStatus": str(data.get('status', 0)),
                "bFollowup": str(data.get('followup', False)).lower(),
                "dFollowup": data.get('followup_date', ''),
                "dFollowupLast": data.get('followup_last_date', ''),
                "iInquiryType": str(data.get('inquiry_type', 0)),
                "dcQuotedRate": str(data.get('quoted_rate', 0)),
                "dExpires": data.get('expires_date', ''),
                "QTRentalTypeID": str(data.get('rental_type_id', 0)),
                "QTCancellationTypeID": str(data.get('cancellation_type_id', 0)),
                "sCancellationReason": data.get('cancellation_reason', ''),
                "ConcessionID": str(data.get('concession_id', 0)),
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
            f"site={site_code} waiting_id={waiting_id} status={data.get('status', 0)}"
        )

        return jsonify({
            'success': True,
            'waiting_id': waiting_id,
            'ret_code': ret_code,
            'message': ret_msg or 'Reservation updated',
            'raw': results[0] if results else None,
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
        soap_client.close()


# =============================================================================
# PUT /api/reservations/<waiting_id>/cancel — cancel via ReservationUpdate_v4
# =============================================================================

@reservations_bp.route('/<int:waiting_id>/cancel', methods=['PUT'])
@require_auth
@require_api_scope('inventory:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def reservation_cancel(waiting_id):
    """
    Cancel a reservation (convenience wrapper around ReservationUpdate_v4).

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

    # First fetch the reservation to get current values
    soap_client = _get_cc_soap_client()
    try:
        # Get current reservation data
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

        # Update with cancel status (iStatus=2 is typically cancelled in SMD)
        results = soap_client.call(
            operation="ReservationUpdate_v4",
            parameters={
                "sLocationCode": site_code,
                "WaitingID": str(waiting_id),
                "sTenantID": str(res.get('TenantID', '0')),
                "sUnitID": str(res.get('UnitID', '0')),
                "dNeeded": res.get('dNeeded', ''),
                "sComment": res.get('sComment', ''),
                "iStatus": "2",
                "bFollowup": "false",
                "dFollowup": "",
                "dFollowupLast": "",
                "iInquiryType": str(res.get('iInquiryType', 0)),
                "dcQuotedRate": str(res.get('dcQuotedRate', 0)),
                "dExpires": res.get('dExpires', ''),
                "QTRentalTypeID": str(res.get('QTRentalTypeID', 0)),
                "QTCancellationTypeID": str(data.get('cancellation_type_id', 0)),
                "sCancellationReason": data.get('cancellation_reason', ''),
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
            f"reason={data.get('cancellation_reason', '')}"
        )

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
        soap_client.close()


# =============================================================================
# GET /api/reservations/<waiting_id>/notes — ReservationNotesRetrieve
# =============================================================================

@reservations_bp.route('/<int:waiting_id>/notes')
@require_auth
@require_api_scope('inventory:read')
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

    soap_client = _get_cc_soap_client()
    try:
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
        soap_client.close()


# =============================================================================
# POST /api/reservations/<waiting_id>/notes — ReservationNoteInsert
# =============================================================================

@reservations_bp.route('/<int:waiting_id>/notes', methods=['POST'])
@require_auth
@require_api_scope('inventory:write')
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
    note = data.get('note', '').strip()

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not note:
        return jsonify({'error': 'note is required'}), 400

    if not _validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = _get_cc_soap_client()
    try:
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
        soap_client.close()


# =============================================================================
# POST /api/reservations/<waiting_id>/send-confirmation
#   — SendReservationConfirmationEmail
# =============================================================================

@reservations_bp.route('/<int:waiting_id>/send-confirmation', methods=['POST'])
@require_auth
@require_api_scope('inventory:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def reservation_send_confirmation(waiting_id):
    """
    Send confirmation email for a reservation.

    Path parameter:
        waiting_id — the reservation WaitingID

    JSON body:
        site_code    — location code          [required]
        move_in_link — move-in link URL       [default: ""]
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

    move_in_link = data.get('move_in_link', '')

    soap_client = _get_cc_soap_client()
    try:
        results = soap_client.call(
            operation="SendReservationConfirmationEmail",
            parameters={
                "sLocationCode": site_code,
                "waitingId": str(waiting_id),
                "moveInLink": move_in_link,
            },
            soap_action=_cc_soap_action("SendReservationConfirmationEmail"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = None
        ret_msg = None
        if results:
            ret_code = results[0].get('Ret_Code')
            ret_msg = results[0].get('Ret_Msg')

        logger.info(f"Confirmation email sent for reservation {waiting_id} at {site_code}")

        return jsonify({
            'success': True,
            'waiting_id': waiting_id,
            'ret_code': ret_code,
            'message': ret_msg or 'Confirmation email sent',
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault SendReservationConfirmationEmail: {e}")
        return jsonify({'success': False, 'error': 'SOAP API error'}), 502

    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    except Exception as e:
        logger.error(f"Unexpected error sending confirmation for {waiting_id}: {e}")
        return jsonify({'success': False, 'error': 'An internal error occurred'}), 500

    finally:
        soap_client.close()


# =============================================================================
# GET /api/reservations/fees — ReservationFeeRetrieve
# =============================================================================

@reservations_bp.route('/fees')
@require_auth
@require_api_scope('inventory:read')
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

    soap_client = _get_cc_soap_client()
    try:
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
        soap_client.close()
