"""
Billing API routes.

Dedicated blueprint for SOAP CallCenterWs billing operations:
- Charge management (add, recurring, future, update price, credits)
- Ledger & balance queries
- Payments (cash, check, bank transfer, card, multi)
- Refunds (cash, check, card)
- Invoices & move-out scheduling
- Insurance coverage
"""

import logging

from flask import Blueprint, jsonify, request

from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api
from web.utils.audit import audit_log, AuditEvent
from web.utils.soap_helpers import (
    CC_NS, cc_soap_action, get_cc_soap_client, validate_site_code,
    safe_int, safe_rate, sanitize_log, clamp, require_date,
)

logger = logging.getLogger(__name__)

billing_bp = Blueprint('billing', __name__, url_prefix='/api/billing')


# ---------------------------------------------------------------------------
# Shared SOAP helper — structurally prevents audit-before-success
# ---------------------------------------------------------------------------

def _billing_soap_call(site_code, operation, params, result_tag="RT",
                       audit_event=None, audit_detail=""):
    """
    Call a billing SOAP operation with standard error handling.

    Returns (results, None) on success or (None, error_response) on failure.
    Fires audit_log ONLY on success (Ret_Code > 0).

    For Table-type results (read operations), returns the raw rows.
    For RT-type results (writes), parses Ret_Code and returns 502 on failure.
    """
    from common.soap_client import SOAPFaultError

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation=operation,
            parameters={"sLocationCode": site_code, **params},
            soap_action=cc_soap_action(operation),
            namespace=CC_NS,
            result_tag=result_tag,
        )

        if result_tag == "Table":
            return results or [], None

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"{operation} failed: ret_code={ret_code} msg={ret_msg}")
            return None, (jsonify({'error': f'{operation} rejected by SMD',
                                   'detail': ret_msg}), 502)

        if audit_event:
            audit_log(audit_event, audit_detail)

        return results, None

    except SOAPFaultError as e:
        logger.error(f"SOAP fault {operation}: {e}")
        return None, (jsonify({'error': 'SOAP API error'}), 502)
    except RuntimeError as e:
        logger.error(f"Config error {operation}: {e}")
        return None, (jsonify({'error': 'SOAP configuration not available'}), 500)
    except Exception as e:
        logger.error(f"Unexpected error {operation}: {e}")
        return None, (jsonify({'error': 'An internal error occurred'}), 500)
    finally:
        if soap_client:
            soap_client.close()


# ============================================================================
# SECTION 1: Charge Management
# ============================================================================

@billing_bp.route('/charge-types', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def charge_types():
    """ChargeDescriptionsRetrieve — list available charge types for a site."""
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
            operation="ChargeDescriptionsRetrieve",
            parameters={"sLocationCode": site_code},
            soap_action=cc_soap_action("ChargeDescriptionsRetrieve"),
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
        logger.error(f"SOAP fault ChargeDescriptionsRetrieve: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error ChargeDescriptionsRetrieve: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/tax-rates', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def tax_rates():
    """RentTaxRatesRetrieve — get site-level rent tax configuration."""
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
            operation="RentTaxRatesRetrieve",
            parameters={"sLocationCode": site_code},
            soap_action=cc_soap_action("RentTaxRatesRetrieve"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'data': results[0] if results else {},
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault RentTaxRatesRetrieve: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error RentTaxRatesRetrieve: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/charges', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def add_charge():
    """ChargeAddToLedger — add a one-time charge to a ledger."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    ledger_id, err = safe_int(data.get('ledger_id'), min_val=1)
    if err:
        return jsonify({'error': f'ledger_id: {err}'}), 400

    charge_desc_id, err = safe_int(data.get('charge_description_id'), min_val=1)
    if err:
        return jsonify({'error': f'charge_description_id: {err}'}), 400

    amount, err = safe_rate(data.get('amount'))
    if err:
        return jsonify({'error': f'amount: {err}'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="ChargeAddToLedger",
            parameters={
                "sLocationCode": site_code,
                "LedgerID": str(ledger_id),
                "ChargeDescID": str(charge_desc_id),
                "dcAmtPreTax": amount,
            },
            soap_action=cc_soap_action("ChargeAddToLedger"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"ChargeAddToLedger failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Charge add failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.CHARGE_ADDED,
                  f"site={sanitize_log(site_code)} ledger={ledger_id} "
                  f"charge_desc={charge_desc_id} amount={amount}")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ChargeAddToLedger: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error ChargeAddToLedger: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/charges/recurring', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def add_recurring_charge():
    """RecurringChargeAddToLedger_v1 — add a recurring charge to a ledger."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    ledger_id, err = safe_int(data.get('ledger_id'), min_val=1)
    if err:
        return jsonify({'error': f'ledger_id: {err}'}), 400

    charge_desc_id, err = safe_int(data.get('charge_description_id'), min_val=1)
    if err:
        return jsonify({'error': f'charge_description_id: {err}'}), 400

    amount, err = safe_rate(data.get('amount'))
    if err:
        return jsonify({'error': f'amount: {err}'}), 400

    recurring_amount, err = safe_rate(
        data.get('recurring_rate_amount') or data.get('amount'))
    if err:
        return jsonify({'error': f'recurring_rate_amount: {err}'}), 400

    start_date, err = require_date(data.get('start_date'))
    if err:
        return jsonify({'error': f'start_date: {err}'}), 400

    qty, err = safe_int(data.get('qty', 1) or data.get('frequency', 1), min_val=1)
    if err:
        return jsonify({'error': f'qty: {err}'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="RecurringChargeAddToLedger_v1",
            parameters={
                "sLocationCode": site_code,
                "LedgerID": str(ledger_id),
                "ChargeDescID": str(charge_desc_id),
                "dcAmtPreTax": amount,
                "dcRecurringRateAmt": recurring_amount,
                "iQty": str(qty),
            },
            soap_action=cc_soap_action("RecurringChargeAddToLedger_v1"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"RecurringChargeAddToLedger_v1 failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Recurring charge failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.CHARGE_ADDED,
                  f"site={sanitize_log(site_code)} ledger={ledger_id} "
                  f"charge_desc={charge_desc_id} amount={amount} "
                  f"recurring_amt={recurring_amount} qty={qty}")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault RecurringChargeAddToLedger_v1: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error RecurringChargeAddToLedger_v1: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/charges/future', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def make_future_charges():
    """CustomerAccountsMakeFutureCharges — generate future charges through a date."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    tenant_id, err = safe_int(data.get('tenant_id'), min_val=1)
    if err:
        return jsonify({'error': f'tenant_id: {err}'}), 400

    n_periods, err = safe_int(data.get('number_of_future_periods'), min_val=1)
    if err:
        return jsonify({'error': f'number_of_future_periods: {err}'}), 400

    future_due_date, err = require_date(
        data.get('future_due_date') or data.get('charge_through_date'))
    if err:
        return jsonify({'error': f'future_due_date: {err}'}), 400

    future_due_iso = (f"{future_due_date}T00:00:00"
                      if "T" not in str(future_due_date) else str(future_due_date))

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="CustomerAccountsMakeFutureCharges",
            parameters={
                "sLocationCode": site_code,
                "iTenantID": str(tenant_id),
                "iNumberOfFuturePeriods": str(n_periods),
                "dFutureDueDate": future_due_iso,
            },
            soap_action=cc_soap_action("CustomerAccountsMakeFutureCharges"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"CustomerAccountsMakeFutureCharges failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Future charges failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.CHARGE_ADDED,
                  f"site={sanitize_log(site_code)} tenant={tenant_id} "
                  f"future_charges periods={n_periods} due={future_due_date}")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault CustomerAccountsMakeFutureCharges: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error CustomerAccountsMakeFutureCharges: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/charges/<int:ledger_id>', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def charges_by_ledger(ledger_id):
    """ChargesAllByLedgerID — list all charges for a ledger."""
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
            operation="ChargesAllByLedgerID",
            parameters={
                "sLocationCode": site_code,
                "ledgerId": str(ledger_id),
            },
            soap_action=cc_soap_action("ChargesAllByLedgerID"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'count': len(results) if results else 0,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ChargesAllByLedgerID: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error ChargesAllByLedgerID: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/charges/price', methods=['PUT'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def update_charge_price():
    """ChargePriceUpdate — update the price of an existing charge."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    ledger_id, err = safe_int(data.get('ledger_id'), min_val=1)
    if err:
        return jsonify({'error': f'ledger_id: {err}'}), 400

    charge_id, err = safe_int(data.get('charge_id'), min_val=1)
    if err:
        return jsonify({'error': f'charge_id: {err}'}), 400

    new_price, err = safe_rate(data.get('new_price'))
    if err:
        return jsonify({'error': f'new_price: {err}'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="ChargePriceUpdate",
            parameters={
                "sLocationCode": site_code,
                "ledgerId": str(ledger_id),
                "chargeId": str(charge_id),
                "amount": new_price,
            },
            soap_action=cc_soap_action("ChargePriceUpdate"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"ChargePriceUpdate failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Charge price update failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.CHARGE_PRICE_UPDATED,
                  f"site={sanitize_log(site_code)} charge={charge_id} "
                  f"new_price={new_price}")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'charge_id': charge_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ChargePriceUpdate: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error ChargePriceUpdate: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/credits', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def apply_credit():
    """ApplyCredit — apply a credit to a ledger."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    ledger_id, err = safe_int(data.get('ledger_id'), min_val=1)
    if err:
        return jsonify({'error': f'ledger_id: {err}'}), 400

    charge_id, err = safe_int(data.get('charge_id'), min_val=1)
    if err:
        return jsonify({'error': f'charge_id: {err}'}), 400

    amount, err = safe_rate(data.get('amount'))
    if err:
        return jsonify({'error': f'amount: {err}'}), 400

    credit_reason = clamp(data.get('credit_reason') or data.get('comment', ''), 500)

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="ApplyCredit",
            parameters={
                "sLocationCode": site_code,
                "ledgerId": str(ledger_id),
                "chargeId": str(charge_id),
                "amount": amount,
                "creditReason": credit_reason,
            },
            soap_action=cc_soap_action("ApplyCredit"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"ApplyCredit failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Apply credit failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.CREDIT_APPLIED,
                  f"site={sanitize_log(site_code)} ledger={ledger_id} "
                  f"amount={amount}")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ApplyCredit: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error ApplyCredit: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


# ============================================================================
# SECTION 2: Ledger & Balance
# ============================================================================

@billing_bp.route('/ledgers/<int:tenant_id>', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def ledgers_by_tenant(tenant_id):
    """LedgersByTenantID_v3 — list ledgers for a tenant."""
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
            operation="LedgersByTenantID_v3",
            parameters={
                "sLocationCode": site_code,
                "sTenantID": str(tenant_id),
            },
            soap_action=cc_soap_action("LedgersByTenantID_v3"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'count': len(results) if results else 0,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault LedgersByTenantID_v3: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error LedgersByTenantID_v3: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/balance/<int:tenant_id>', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def balance_details(tenant_id):
    """CustomerAccountsBalanceDetails_v2 — balance details for a tenant."""
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
            operation="CustomerAccountsBalanceDetails_v2",
            parameters={
                "sLocationCode": site_code,
                "iTenantID": str(tenant_id),
            },
            soap_action=cc_soap_action("CustomerAccountsBalanceDetails_v2"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'count': len(results) if results else 0,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault CustomerAccountsBalanceDetails_v2: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error CustomerAccountsBalanceDetails_v2: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/balance-with-discount/<int:tenant_id>', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def balance_with_discount(tenant_id):
    """CustomerAccountsBalanceDetailsWithDiscount — balance with discount info."""
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code query parameter is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    unit_id, err = safe_int(request.args.get('unit_id'), min_val=1)
    if err:
        return jsonify({'error': f'unit_id: {err}'}), 400

    concession_plan_id, err = safe_int(request.args.get('concession_plan_id'), min_val=0)
    if err:
        return jsonify({'error': f'concession_plan_id: {err}'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="CustomerAccountsBalanceDetailsWithDiscount",
            parameters={
                "sLocationCode": site_code,
                "iTenantID": str(tenant_id),
                "iUnitID": str(unit_id),
                "ConcessionPlanID": str(concession_plan_id),
            },
            soap_action=cc_soap_action("CustomerAccountsBalanceDetailsWithDiscount"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'unit_id': unit_id,
            'count': len(results) if results else 0,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault CustomerAccountsBalanceDetailsWithDiscount: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error CustomerAccountsBalanceDetailsWithDiscount: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/paid-through/<int:ledger_id>', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def paid_through_date(ledger_id):
    """PaidThroughDateByLedgerID — get paid-through date for a ledger."""
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
            operation="PaidThroughDateByLedgerID",
            parameters={
                "sLocationCode": site_code,
                "iLedgerID": str(ledger_id),
            },
            soap_action=cc_soap_action("PaidThroughDateByLedgerID"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'ret_code': ret_code,
            'ret_msg': ret_msg,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault PaidThroughDateByLedgerID: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error PaidThroughDateByLedgerID: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/transactions/<int:ledger_id>', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def transactions_by_ledger(ledger_id):
    """ChargesAndPaymentsByLedgerID — all charges and payments for a ledger."""
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
            operation="ChargesAndPaymentsByLedgerID",
            parameters={
                "sLocationCode": site_code,
                "sLedgerID": str(ledger_id),
            },
            soap_action=cc_soap_action("ChargesAndPaymentsByLedgerID"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'count': len(results) if results else 0,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ChargesAndPaymentsByLedgerID: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error ChargesAndPaymentsByLedgerID: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/statement/<int:ledger_id>', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def ledger_statement(ledger_id):
    """LedgerStatementByLedgerID — full statement for a ledger."""
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
            operation="LedgerStatementByLedgerID",
            parameters={
                "sLocationCode": site_code,
                "sLedgerID": str(ledger_id),
            },
            soap_action=cc_soap_action("LedgerStatementByLedgerID"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'count': len(results) if results else 0,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault LedgerStatementByLedgerID: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error LedgerStatementByLedgerID: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/ledger-transfer', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def ledger_transfer():
    """LedgerTransferToNewTenant — transfer a ledger to a different tenant."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    ledger_id, err = safe_int(data.get('ledger_id'), min_val=1)
    if err:
        return jsonify({'error': f'ledger_id: {err}'}), 400

    new_tenant_id, err = safe_int(data.get('new_tenant_id'), min_val=1)
    if err:
        return jsonify({'error': f'new_tenant_id: {err}'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="LedgerTransferToNewTenant",
            parameters={
                "sLocationCode": site_code,
                "LedgerID": str(ledger_id),
                "TenantID": str(new_tenant_id),
            },
            soap_action=cc_soap_action("LedgerTransferToNewTenant"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"LedgerTransferToNewTenant failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Ledger transfer failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.LEDGER_TRANSFERRED,
                  f"site={sanitize_log(site_code)} ledger={ledger_id} "
                  f"new_tenant={new_tenant_id}")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'new_tenant_id': new_tenant_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault LedgerTransferToNewTenant: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error LedgerTransferToNewTenant: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/discount/remove', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def remove_discount():
    """RemoveDiscountFromLedger — remove a discount plan from a ledger."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    ledger_id, err = safe_int(data.get('ledger_id'), min_val=1)
    if err:
        return jsonify({'error': f'ledger_id: {err}'}), 400

    discount_plan_id, err = safe_int(data.get('discount_plan_id'), min_val=1)
    if err:
        return jsonify({'error': f'discount_plan_id: {err}'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="RemoveDiscountFromLedger",
            parameters={
                "sLocationCode": site_code,
                "LedgerID": str(ledger_id),
                "ConcessionID": str(discount_plan_id),
            },
            soap_action=cc_soap_action("RemoveDiscountFromLedger"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"RemoveDiscountFromLedger failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Remove discount failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.DISCOUNT_REMOVED,
                  f"site={sanitize_log(site_code)} ledger={ledger_id} "
                  f"discount_plan={discount_plan_id}")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'discount_plan_id': discount_plan_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault RemoveDiscountFromLedger: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error RemoveDiscountFromLedger: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


# ============================================================================
# SECTION 3: Payments
# ============================================================================

@billing_bp.route('/payments/cash', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def payment_cash():
    """PaymentSimpleCashWithSource — record a cash payment."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    tenant_id, err = safe_int(data.get('tenant_id'), min_val=1)
    if err:
        return jsonify({'error': f'tenant_id: {err}'}), 400

    unit_id, err = safe_int(data.get('unit_id'), min_val=1)
    if err:
        return jsonify({'error': f'unit_id: {err}'}), 400

    amount, err = safe_rate(data.get('amount'))
    if err:
        return jsonify({'error': f'amount: {err}'}), 400

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="PaymentSimpleCashWithSource",
            parameters={
                "sLocationCode": site_code,
                "iTenantID": str(tenant_id),
                "iUnitID": str(unit_id),
                "dcPaymentAmount": amount,
                "iSource": "0",
            },
            soap_action=cc_soap_action("PaymentSimpleCashWithSource"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"PaymentSimpleCashWithSource failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Cash payment failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.PAYMENT_RECORDED,
                  f"site={sanitize_log(site_code)} tenant={tenant_id} unit={unit_id} "
                  f"amount={amount} type=cash")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'unit_id': unit_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault PaymentSimpleCashWithSource: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error PaymentSimpleCashWithSource: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/payments/check', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def payment_check():
    """PaymentSimpleCheckWithSource — record a check payment."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    tenant_id, err = safe_int(data.get('tenant_id'), min_val=1)
    if err:
        return jsonify({'error': f'tenant_id: {err}'}), 400

    unit_id, err = safe_int(data.get('unit_id'), min_val=1)
    if err:
        return jsonify({'error': f'unit_id: {err}'}), 400

    amount, err = safe_rate(data.get('amount'))
    if err:
        return jsonify({'error': f'amount: {err}'}), 400

    check_number = data.get('check_number', '').strip()
    if not check_number:
        return jsonify({'error': 'check_number is required'}), 400
    check_number = clamp(check_number, 50)

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="PaymentSimpleCheckWithSource",
            parameters={
                "sLocationCode": site_code,
                "iTenantID": str(tenant_id),
                "iUnitID": str(unit_id),
                "dcPaymentAmount": amount,
                "sCheckNumber": check_number,
                "iSource": "0",
            },
            soap_action=cc_soap_action("PaymentSimpleCheckWithSource"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"PaymentSimpleCheckWithSource failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Check payment failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.PAYMENT_RECORDED,
                  f"site={sanitize_log(site_code)} tenant={tenant_id} unit={unit_id} "
                  f"amount={amount} check={check_number} type=check")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'unit_id': unit_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault PaymentSimpleCheckWithSource: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error PaymentSimpleCheckWithSource: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/payments/bank-transfer', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def payment_bank_transfer():
    """PaymentSimpleBankTransferWithSource — record a bank transfer payment."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    tenant_id, err = safe_int(data.get('tenant_id'), min_val=1)
    if err:
        return jsonify({'error': f'tenant_id: {err}'}), 400

    unit_id, err = safe_int(data.get('unit_id'), min_val=1)
    if err:
        return jsonify({'error': f'unit_id: {err}'}), 400

    amount, err = safe_rate(data.get('amount'))
    if err:
        return jsonify({'error': f'amount: {err}'}), 400

    transfer_number = (data.get('transfer_number') or data.get('reference', '')).strip()
    if not transfer_number:
        return jsonify({'error': 'transfer_number is required'}), 400
    transfer_number = clamp(transfer_number, 100)

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="PaymentSimpleBankTransferWithSource",
            parameters={
                "sLocationCode": site_code,
                "iTenantID": str(tenant_id),
                "iUnitID": str(unit_id),
                "dcPaymentAmount": amount,
                "sTransferNumber": transfer_number,
                "iSource": "0",
            },
            soap_action=cc_soap_action("PaymentSimpleBankTransferWithSource"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"PaymentSimpleBankTransferWithSource failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Bank transfer payment failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.PAYMENT_RECORDED,
                  f"site={sanitize_log(site_code)} tenant={tenant_id} unit={unit_id} "
                  f"amount={amount} ref={transfer_number} type=bank_transfer")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'unit_id': unit_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault PaymentSimpleBankTransferWithSource: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error PaymentSimpleBankTransferWithSource: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/payments/multi', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def payment_multi():
    """PaymentMultipleWithSource_v3 — record multiple payments in one call."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    # NOTE: PaymentMultipleWithSource_v3 WSDL params are NOT confirmed.
    # The original sLedgerIDs/sAmounts/sPaymentTypeIDs comma-delimited fields
    # do not appear in the WSDL. This route requires WSDL verification on
    # LSETUP before use. Inputs match the entity model (tenant_id+unit_id).
    payments = data.get('payments')
    if not payments or not isinstance(payments, list):
        return jsonify({'error': 'payments must be a non-empty array'}), 400
    if len(payments) > 50:
        return jsonify({'error': 'Maximum 50 payments per request'}), 400

    for i, p in enumerate(payments):
        if not isinstance(p, dict):
            return jsonify({'error': f'payments[{i}] must be an object'}), 400

        tid, err = safe_int(p.get('tenant_id'), min_val=1)
        if err:
            return jsonify({'error': f'payments[{i}].tenant_id: {err}'}), 400
        p['_tenant_id'] = tid

        uid, err = safe_int(p.get('unit_id'), min_val=1)
        if err:
            return jsonify({'error': f'payments[{i}].unit_id: {err}'}), 400
        p['_unit_id'] = uid

        amt, err = safe_rate(p.get('amount'))
        if err:
            return jsonify({'error': f'payments[{i}].amount: {err}'}), 400
        p['_amount'] = amt

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="PaymentMultipleWithSource_v3",
            parameters={
                "sLocationCode": site_code,
                "sTenantIDs": ",".join(str(p['_tenant_id']) for p in payments),
                "sUnitIDs": ",".join(str(p['_unit_id']) for p in payments),
                "sAmounts": ",".join(p['_amount'] for p in payments),
                "iSource": "0",
            },
            soap_action=cc_soap_action("PaymentMultipleWithSource_v3"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"PaymentMultipleWithSource_v3 failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Multi payment failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.PAYMENT_RECORDED,
                  f"site={sanitize_log(site_code)} "
                  f"tenants={','.join(str(p['_tenant_id']) for p in payments)} "
                  f"type=multi count={len(payments)}")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'payment_count': len(payments),
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault PaymentMultipleWithSource_v3: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error PaymentMultipleWithSource_v3: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/payments/<int:ledger_id>', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def payments_by_ledger(ledger_id):
    """PaymentsByLedgerID — list all payments for a ledger."""
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
            operation="PaymentsByLedgerID",
            parameters={
                "sLocationCode": site_code,
                "sLedgerID": str(ledger_id),
            },
            soap_action=cc_soap_action("PaymentsByLedgerID"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'count': len(results) if results else 0,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault PaymentsByLedgerID: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error PaymentsByLedgerID: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/payment-types', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def payment_types():
    """PaymentTypesRetrieve — list available payment types for a site."""
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
            operation="PaymentTypesRetrieve",
            parameters={"sLocationCode": site_code},
            soap_action=cc_soap_action("PaymentTypesRetrieve"),
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
        logger.error(f"SOAP fault PaymentTypesRetrieve: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error PaymentTypesRetrieve: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


# ============================================================================
# SECTION 4: Refunds
# ============================================================================

@billing_bp.route('/refunds/cash', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def refund_cash():
    """RefundPaymentCash — process a cash refund."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    tenant_id, err = safe_int(data.get('tenant_id'), min_val=1)
    if err:
        return jsonify({'error': f'tenant_id: {err}'}), 400

    unit_id, err = safe_int(data.get('unit_id'), min_val=1)
    if err:
        return jsonify({'error': f'unit_id: {err}'}), 400

    payment_id, err = safe_int(data.get('payment_id'), min_val=1)
    if err:
        return jsonify({'error': f'payment_id: {err}'}), 400

    reason = clamp(data.get('reason') or data.get('comment', ''), 500)

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="RefundPaymentCash",
            parameters={
                "sLocationCode": site_code,
                "iTenantID": str(tenant_id),
                "iUnitID": str(unit_id),
                "iPaymentID": str(payment_id),
                "sReason": reason,
            },
            soap_action=cc_soap_action("RefundPaymentCash"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"RefundPaymentCash failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Cash refund failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.REFUND_PROCESSED,
                  f"site={sanitize_log(site_code)} tenant={tenant_id} unit={unit_id} "
                  f"payment={payment_id} type=cash")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'unit_id': unit_id,
            'payment_id': payment_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault RefundPaymentCash: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error RefundPaymentCash: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/refunds/check', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def refund_check():
    """RefundPaymentCheck — process a check refund."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    tenant_id, err = safe_int(data.get('tenant_id'), min_val=1)
    if err:
        return jsonify({'error': f'tenant_id: {err}'}), 400

    unit_id, err = safe_int(data.get('unit_id'), min_val=1)
    if err:
        return jsonify({'error': f'unit_id: {err}'}), 400

    payment_id, err = safe_int(data.get('payment_id'), min_val=1)
    if err:
        return jsonify({'error': f'payment_id: {err}'}), 400

    check_number = data.get('check_number', '').strip()
    if not check_number:
        return jsonify({'error': 'check_number is required'}), 400
    check_number = clamp(check_number, 50)

    reason = clamp(data.get('reason') or data.get('comment', ''), 500)

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="RefundPaymentCheck",
            parameters={
                "sLocationCode": site_code,
                "iTenantID": str(tenant_id),
                "iUnitID": str(unit_id),
                "iPaymentID": str(payment_id),
                "sCheckNumber": check_number,
                "sReason": reason,
            },
            soap_action=cc_soap_action("RefundPaymentCheck"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"RefundPaymentCheck failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Check refund failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.REFUND_PROCESSED,
                  f"site={sanitize_log(site_code)} tenant={tenant_id} unit={unit_id} "
                  f"payment={payment_id} check={check_number} type=check")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'unit_id': unit_id,
            'payment_id': payment_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault RefundPaymentCheck: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error RefundPaymentCheck: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


# ============================================================================
# SECTION 5: Invoices & Move-Out
# ============================================================================

@billing_bp.route('/invoices/<int:tenant_id>', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def invoices_by_tenant(tenant_id):
    """TenantInvoicesByTenantID — list invoices for a tenant."""
    from common.soap_client import SOAPFaultError

    site_code = request.args.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code query parameter is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    from datetime import datetime, timedelta
    date_end = request.args.get(
        'date_end', datetime.now().strftime('%Y-%m-%dT00:00:00'))
    date_start = request.args.get(
        'date_start',
        (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%dT00:00:00'))

    if "T" not in date_end:
        date_end = f"{date_end}T00:00:00"
    if "T" not in date_start:
        date_start = f"{date_start}T00:00:00"

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="TenantInvoicesByTenantID",
            parameters={
                "sLocationCode": site_code,
                "sTenantIDsCommaDelimited": str(tenant_id),
                "dDateStart": date_start,
                "dDateEnd": date_end,
            },
            soap_action=cc_soap_action("TenantInvoicesByTenantID"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'date_start': date_start,
            'date_end': date_end,
            'count': len(results) if results else 0,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault TenantInvoicesByTenantID: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error TenantInvoicesByTenantID: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/move-out/schedule', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def schedule_move_out():
    """ScheduleMoveOut — schedule a move-out date for a ledger."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    ledger_id, err = safe_int(data.get('ledger_id'), min_val=1)
    if err:
        return jsonify({'error': f'ledger_id: {err}'}), 400

    scheduled_date, err = require_date(data.get('scheduled_date'))
    if err:
        return jsonify({'error': f'scheduled_date: {err}'}), 400

    scheduled_date_iso = (f"{scheduled_date}T00:00:00"
                          if "T" not in str(scheduled_date) else str(scheduled_date))

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="ScheduleMoveOut",
            parameters={
                "sLocationCode": site_code,
                "iLedgerID": str(ledger_id),
                "dScheduledOut": scheduled_date_iso,
            },
            soap_action=cc_soap_action("ScheduleMoveOut"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"ScheduleMoveOut failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Move-out scheduling failed',
                            'detail': ret_msg}), 502

        audit_log(AuditEvent.MOVE_OUT_SCHEDULED,
                  f"site={sanitize_log(site_code)} ledger={ledger_id} "
                  f"date={scheduled_date}")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'scheduled_date': scheduled_date,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault ScheduleMoveOut: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error ScheduleMoveOut: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


# ============================================================================
# SECTION 6: Insurance
# ============================================================================

@billing_bp.route('/insurance', methods=['POST'])
@require_auth
@require_api_scope('billing:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def add_insurance():
    """InsuranceCoverageAddToLedger — add insurance coverage to a ledger."""
    from common.soap_client import SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code', '').strip()
    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not validate_site_code(site_code):
        return jsonify({'error': 'Invalid site_code'}), 400

    tenant_id, err = safe_int(data.get('tenant_id'), min_val=1)
    if err:
        return jsonify({'error': f'tenant_id: {err}'}), 400

    unit_id, err = safe_int(data.get('unit_id'), min_val=1)
    if err:
        return jsonify({'error': f'unit_id: {err}'}), 400

    insurance_coverage_id, err = safe_int(data.get('insurance_coverage_id'), min_val=1)
    if err:
        return jsonify({'error': f'insurance_coverage_id: {err}'}), 400

    policy_number = clamp((data.get('policy_number') or '').strip(), 100)
    if not policy_number:
        return jsonify({'error': 'policy_number is required'}), 400

    start_date, err = require_date(data.get('start_date'))
    if err:
        return jsonify({'error': f'start_date: {err}'}), 400

    start_date_iso = (f"{start_date}T00:00:00"
                      if "T" not in str(start_date) else str(start_date))

    soap_client = None
    try:
        soap_client = get_cc_soap_client()
        results = soap_client.call(
            operation="InsuranceCoverageAddToLedger",
            parameters={
                "sLocationCode": site_code,
                "TenantID": str(tenant_id),
                "UnitID": str(unit_id),
                "InsuranceCoverageID": str(insurance_coverage_id),
                "sPolicyNumber": policy_number,
                "dStartDate": start_date_iso,
            },
            soap_action=cc_soap_action("InsuranceCoverageAddToLedger"),
            namespace=CC_NS,
            result_tag="RT",
        )

        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"InsuranceCoverageAddToLedger failed: ret_code={ret_code} msg={ret_msg}")
            return jsonify({'error': 'Insurance add failed', 'detail': ret_msg}), 502

        audit_log(AuditEvent.INSURANCE_ADDED,
                  f"site={sanitize_log(site_code)} tenant={tenant_id} unit={unit_id} "
                  f"coverage={insurance_coverage_id} policy={policy_number}")

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'tenant_id': tenant_id,
            'unit_id': unit_id,
            'insurance_coverage_id': insurance_coverage_id,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault InsuranceCoverageAddToLedger: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error InsuranceCoverageAddToLedger: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()


@billing_bp.route('/insurance/<int:ledger_id>', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def insurance_status(ledger_id):
    """InsuranceLedgerStatusByLedgerID — get insurance status for a ledger."""
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
            operation="InsuranceLedgerStatusByLedgerID",
            parameters={
                "sLocationCode": site_code,
                "iLedgerID": str(ledger_id),
            },
            soap_action=cc_soap_action("InsuranceLedgerStatusByLedgerID"),
            namespace=CC_NS,
            result_tag="Table",
        )

        return jsonify({
            'status': 'success',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'count': len(results) if results else 0,
            'data': results or [],
        })

    except SOAPFaultError as e:
        logger.error(f"SOAP fault InsuranceLedgerStatusByLedgerID: {e}")
        return jsonify({'error': 'SOAP API error'}), 502
    except RuntimeError as e:
        logger.error(f"Config error: {e}")
        return jsonify({'error': 'SOAP configuration not available'}), 500
    except Exception as e:
        logger.error(f"Unexpected error InsuranceLedgerStatusByLedgerID: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        if soap_client:
            soap_client.close()
