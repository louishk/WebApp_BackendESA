"""
Units API routes.

SOAP CallCenterWs unit information endpoints for the booking engine.
"""

import logging

from flask import Blueprint, jsonify, request

from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api
from web.utils.soap_helpers import (
    CC_NS, cc_soap_action, get_cc_soap_client, validate_site_code,
)

logger = logging.getLogger(__name__)

units_bp = Blueprint('units', __name__, url_prefix='/api/units')


@units_bp.route('/available', methods=['GET'])
@require_auth
@require_api_scope('reservations:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def available_units():
    """UnitsInformationAvailableUnitsOnly_v2 — list available units for a site."""
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code query parameter is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="UnitsInformationAvailableUnitsOnly_v2",
            parameters={"sLocationCode": site_code},
            soap_action=cc_soap_action("UnitsInformationAvailableUnitsOnly_v2"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'count': len(results) if results else 0,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault UnitsInformationAvailableUnitsOnly_v2: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error UnitsInformationAvailableUnitsOnly_v2: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@units_bp.route('/price-list', methods=['GET'])
@require_auth
@require_api_scope('reservations:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def unit_price_list():
    """UnitTypePriceList_v2 — retrieve unit type pricing for a site."""
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code query parameter is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="UnitTypePriceList_v2",
            parameters={"sLocationCode": site_code},
            soap_action=cc_soap_action("UnitTypePriceList_v2"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'count': len(results) if results else 0,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault UnitTypePriceList_v2: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error UnitTypePriceList_v2: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()
