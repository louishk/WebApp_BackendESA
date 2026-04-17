# SOAP Middleware Remediation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 25 broken billing.py SOAP call sites, wire missing booking engine endpoints, add data sync pipelines for cost calculator, and build the internal MoveInCost calculator.

**Architecture:** Three independent phases on the current monolith structure. All SOAP logic stays in dedicated files (`billing.py`, `reservations.py`, `common/`) for future middleware split. Shared `_billing_soap_call` helper prevents audit-before-success bugs structurally. New pipelines follow existing datalayer patterns.

**Tech Stack:** Python 3, Flask, SQLAlchemy, SOAP (CallCenterWs via `common/soap_client.py`), PostgreSQL (esa_pbi)

**Reference docs:**
- Discovery report: `docs/tools/smd_soap_discovery_report.md`
- Original audit: `docs/tools/smd_soap_audit_fix_plan.md`
- Design spec: `docs/superpowers/specs/2026-04-16-soap-audit-fix-and-booking-engine-design.md`

---

## File Map

### Phase 1 — billing.py remediation
- Modify: `backend/python/web/routes/billing.py` (shared helper + param fixes + delete CC routes)

### Phase 2 — Booking engine SOAP routes
- Modify: `backend/python/web/routes/reservations.py` (insurance V3→V2 downgrade + ReservationFeeAdd route)
- Modify: `backend/python/web/routes/billing.py` (add RentTaxRatesRetrieve route)
- Create: `backend/python/web/routes/units.py` (new blueprint: available units + price list)

### Phase 3 — Data pipelines + cost calculator
- Create: `backend/python/datalayer/cc_charge_descriptions_to_sql.py`
- Create: `backend/python/datalayer/cc_insurance_coverage_to_sql.py`
- Create: `backend/python/common/movein_cost_calculator.py`
- Modify: `backend/python/common/models.py` (new models: CcwsChargeDescription, CcwsInsuranceCoverage)
- Create: `sql/028_charge_descriptions_and_insurance.sql` (migration)

---

## Phase 1: billing.py Remediation (P0)

### Task 1: Add shared `_billing_soap_call` helper

**Files:**
- Modify: `backend/python/web/routes/billing.py:1-27`

- [ ] **Step 1: Add the shared helper function after the imports**

Insert after line 27 (`billing_bp = Blueprint(...)`) in `billing.py`:

```python
# ---------------------------------------------------------------------------
# Shared SOAP helper — structurally prevents audit-before-success
# ---------------------------------------------------------------------------

def _billing_soap_call(site_code, operation, params, result_tag="RT",
                       audit_event=None, audit_detail=""):
    """
    Call a billing SOAP operation with standard error handling.

    Returns (results, None) on success or (None, error_response) on failure.
    Fires audit_log ONLY on success (Ret_Code > 0).
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

        # For Table-type results (read operations), return directly
        if result_tag == "Table":
            return results or [], None

        # For RT-type results, check Ret_Code
        ret_code = results[0].get('Ret_Code') if results else None
        ret_msg = results[0].get('Ret_Msg', '') if results else ''

        if ret_code is None or int(ret_code) <= 0:
            logger.error(f"{operation} failed: ret_code={ret_code} msg={ret_msg}")
            return None, (jsonify({'error': f'{operation} rejected by SMD',
                                   'detail': ret_msg}), 502)

        # Success — audit only now
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
```

- [ ] **Step 2: Verify billing.py still imports cleanly**

Run: `cd backend/python && python -c "from web.routes.billing import billing_bp; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add backend/python/web/routes/billing.py
git commit -m "feat(billing): add shared _billing_soap_call helper

Structurally prevents audit-before-success bug by only firing
audit_log after Ret_Code > 0 is confirmed. Returns HTTP 502 on
SMD failure instead of HTTP 200 with status:error."
```

---

### Task 2: Fix Category F — 5 read-only param renames

**Files:**
- Modify: `backend/python/web/routes/billing.py`

These are one-line fixes. Each changes a WSDL param name.

- [ ] **Step 1: Fix ChargesAllByLedgerID (line ~338)**

Change: `"iLedgerID": str(ledger_id)` → `"ledgerId": str(ledger_id)`

- [ ] **Step 2: Fix LedgersByTenantID_v3 (line ~537)**

Change: `"iTenantID": str(tenant_id)` → `"sTenantID": str(tenant_id)`

- [ ] **Step 3: Fix ChargesAndPaymentsByLedgerID (line ~741)**

Change: `"iLedgerID": str(ledger_id)` → `"sLedgerID": str(ledger_id)`

- [ ] **Step 4: Fix LedgerStatementByLedgerID (line ~791)**

Change: `"iLedgerID": str(ledger_id)` → `"sLedgerID": str(ledger_id)`

- [ ] **Step 5: Fix PaymentsByLedgerID (line ~1406)**

Change: `"iLedgerID": str(ledger_id)` → `"sLedgerID": str(ledger_id)`

- [ ] **Step 6: Verify import**

Run: `cd backend/python && python -c "from web.routes.billing import billing_bp; print('OK')"`

- [ ] **Step 7: Commit**

```bash
git add backend/python/web/routes/billing.py
git commit -m "fix(billing): correct WSDL param names for 5 read-only operations

ChargesAllByLedgerID: iLedgerID → ledgerId
LedgersByTenantID_v3: iTenantID → sTenantID
ChargesAndPaymentsByLedgerID: iLedgerID → sLedgerID
LedgerStatementByLedgerID: iLedgerID → sLedgerID
PaymentsByLedgerID: iLedgerID → sLedgerID"
```

---

### Task 3: Fix ScheduleMoveOut date format

**Files:**
- Modify: `backend/python/web/routes/billing.py` (around line 1802)

- [ ] **Step 1: Read the current schedule_move_out function to find the date param**

Find where `dScheduledOut` is set. The `require_date()` helper returns bare `YYYY-MM-DD`. Append `T00:00:00`.

- [ ] **Step 2: Fix the date format**

In the parameters dict, change from:
```python
"dScheduledOut": scheduled_date,
```
to:
```python
"dScheduledOut": f"{scheduled_date}T00:00:00" if "T" not in str(scheduled_date) else str(scheduled_date),
```

- [ ] **Step 3: Move audit_log to success-only path**

Find the `audit_log(AuditEvent.MOVE_OUT_SCHEDULED, ...)` call. Move it inside the `if success:` block, or refactor to use `_billing_soap_call`.

- [ ] **Step 4: Add HTTP 502 on failure**

If the route doesn't already return 502 on `Ret_Code <= 0`, add:
```python
if ret_code is None or int(ret_code) <= 0:
    logger.error(f"ScheduleMoveOut failed: ret_code={ret_code} msg={ret_msg}")
    return jsonify({'error': 'Move-out scheduling failed'}), 502
```

- [ ] **Step 5: Commit**

```bash
git add backend/python/web/routes/billing.py
git commit -m "fix(billing): ScheduleMoveOut date format + audit-on-success

DateTime must be YYYY-MM-DDT00:00:00, not bare date.
audit_log now fires only after Ret_Code > 0."
```

---

### Task 4: Fix audit-before-success on all remaining write routes

**Files:**
- Modify: `backend/python/web/routes/billing.py`

Every write route that calls `audit_log` before checking `success` must be fixed. The pattern is identical for each:

1. Find `audit_log(AuditEvent.XXX, ...)` 
2. Move it after the `success` check
3. Add HTTP 502 return on failure

- [ ] **Step 1: Identify all affected routes**

Search for `audit_log(` in billing.py. Each occurrence before a `success` check is a bug.

Affected routes (write operations with audit_log):
- `add_charge` (ChargeAddToLedger)
- `add_recurring_charge` (RecurringChargeAddToLedger_v1)
- `make_future_charges` (CustomerAccountsMakeFutureCharges)
- `update_charge_price` (ChargePriceUpdate)
- `apply_credit` (ApplyCredit)
- `ledger_transfer` (LedgerTransferToNewTenant)
- `remove_discount` (RemoveDiscountFromLedger)
- `payment_cash` (PaymentSimpleCashWithSource)
- `payment_check` (PaymentSimpleCheckWithSource)
- `payment_bank_transfer` (PaymentSimpleBankTransferWithSource)
- `payment_card` (PaymentSimpleWithSource_v3) — will be deleted in Task 5
- `payment_multi` (PaymentMultipleWithSource_v3)
- `refund_cash` (RefundPaymentCash)
- `refund_check` (RefundPaymentCheck)
- `refund_card` (RefundPaymentCreditCard_v2) — will be deleted in Task 5
- `add_insurance` (InsuranceCoverageAddToLedger)

- [ ] **Step 2: Apply the fix pattern to each route**

For each route, replace:
```python
# BEFORE (broken):
ret_code = results[0].get('Ret_Code') if results else None
ret_msg = results[0].get('Ret_Msg', '') if results else ''
success = ret_code is not None and int(ret_code) > 0

audit_log(AuditEvent.XXX, f"site={sanitize_log(site_code)} ...")

return jsonify({
    'status': 'success' if success else 'error',
    ...
})
```

With:
```python
# AFTER (fixed):
ret_code = results[0].get('Ret_Code') if results else None
ret_msg = results[0].get('Ret_Msg', '') if results else ''

if ret_code is None or int(ret_code) <= 0:
    logger.error(f"OPERATION_NAME failed: ret_code={ret_code} msg={ret_msg}")
    return jsonify({'error': 'Operation rejected by SMD', 'detail': ret_msg}), 502

audit_log(AuditEvent.XXX, f"site={sanitize_log(site_code)} ...")

return jsonify({
    'status': 'success',
    ...
})
```

- [ ] **Step 3: Verify import**

Run: `cd backend/python && python -c "from web.routes.billing import billing_bp; print('OK')"`

- [ ] **Step 4: Commit**

```bash
git add backend/python/web/routes/billing.py
git commit -m "fix(billing): move audit_log to success-only path on all write routes

Prevents phantom audit records when SMD returns Ret_Code=-1.
All write routes now return HTTP 502 on SMD failure."
```

---

### Task 5: Delete 2 CC payment/refund routes

**Files:**
- Modify: `backend/python/web/routes/billing.py`

- [ ] **Step 1: Delete `payment_card` function**

Remove the entire `payment_card()` function (route `/payments/card`, operation `PaymentSimpleWithSource_v3`) — approximately lines 1208–1286. SiteLink has no CC processor.

- [ ] **Step 2: Delete `refund_card` function**

Remove the entire `refund_card()` function (route `/refunds/card`, operation `RefundPaymentCreditCard_v2`) — approximately lines 1639–1720.

- [ ] **Step 3: Verify import**

Run: `cd backend/python && python -c "from web.routes.billing import billing_bp; print('OK')"`

- [ ] **Step 4: Commit**

```bash
git add backend/python/web/routes/billing.py
git commit -m "fix(billing): remove CC payment and refund routes

PaymentSimpleWithSource_v3 and RefundPaymentCreditCard_v2 deleted.
SiteLink has no payment processor — CC routes can never work."
```

---

### Task 6: Fix Category B/D entity model + param renames

**Files:**
- Modify: `backend/python/web/routes/billing.py`

These routes need param renames to match WSDL. Entity model changes where WSDL uses `tenant_id`/`unit_id` instead of `ledger_id`.

- [ ] **Step 1: Fix ChargeAddToLedger params**

```python
# BEFORE:
"iLedgerID": str(ledger_id),
"iChargeDescriptionID": str(charge_desc_id),
"dcAmount": amount,
"sComment": comment,

# AFTER:
"LedgerID": str(ledger_id),
"ChargeDescID": str(charge_desc_id),
"dcAmtPreTax": amount,
```
Remove `sComment` (not in WSDL).

- [ ] **Step 2: Fix LedgerTransferToNewTenant params**

```python
# BEFORE:
"iLedgerID": str(ledger_id),
"iNewTenantID": str(new_tenant_id),

# AFTER:
"LedgerID": str(ledger_id),
"TenantID": str(new_tenant_id),
```

- [ ] **Step 3: Fix ApplyCredit params**

```python
# BEFORE:
"iLedgerID": str(ledger_id),
"dcAmount": amount,
"sComment": comment,

# AFTER:
"ledgerId": str(ledger_id),
"amount": amount,
"creditReason": comment,
```
Add `chargeId` as a new required body field:
```python
charge_id, err = safe_int(data.get('charge_id'), min_val=1)
if err:
    return jsonify({'error': f'charge_id: {err}'}), 400
```
Add `"chargeId": str(charge_id)` to the SOAP params.

- [ ] **Step 4: Fix RemoveDiscountFromLedger params**

```python
# BEFORE:
"iLedgerID": str(ledger_id),
"iDiscountPlanID": str(discount_plan_id),

# AFTER:
"LedgerID": str(ledger_id),
"ConcessionID": str(discount_plan_id),
```

- [ ] **Step 5: Verify import**

Run: `cd backend/python && python -c "from web.routes.billing import billing_bp; print('OK')"`

- [ ] **Step 6: Commit**

```bash
git add backend/python/web/routes/billing.py
git commit -m "fix(billing): correct WSDL params for charge/credit/discount/transfer routes

ChargeAddToLedger: iLedgerID→LedgerID, iChargeDescriptionID→ChargeDescID, dcAmount→dcAmtPreTax, remove phantom sComment
ApplyCredit: add required chargeId, rename params
RemoveDiscountFromLedger: iDiscountPlanID→ConcessionID
LedgerTransferToNewTenant: iNewTenantID→TenantID"
```

---

### Task 7: Fix Category A — payment/refund entity model

**Files:**
- Modify: `backend/python/web/routes/billing.py`

WSDL for payment operations uses `iTenantID` + `iUnitID` instead of `iLedgerID`. Route inputs need to change from `ledger_id` to `tenant_id` + `unit_id`.

- [ ] **Step 1: Fix PaymentSimpleCashWithSource**

Replace input validation:
```python
# BEFORE:
ledger_id, err = safe_int(data.get('ledger_id'), min_val=1)

# AFTER:
tenant_id, err = safe_int(data.get('tenant_id'), min_val=1)
if err:
    return jsonify({'error': f'tenant_id: {err}'}), 400
unit_id, err = safe_int(data.get('unit_id'), min_val=1)
if err:
    return jsonify({'error': f'unit_id: {err}'}), 400
```

Replace SOAP params:
```python
# BEFORE:
"iLedgerID": str(ledger_id),
"dcPayment": amount,
"sSource": source,
"bTestMode": "true" if test_mode else "false",

# AFTER:
"iTenantID": str(tenant_id),
"iUnitID": str(unit_id),
"dcPaymentAmount": amount,
"iSource": "0",
```

Update audit_log and response to reference `tenant_id`/`unit_id` instead of `ledger_id`.

- [ ] **Step 2: Fix PaymentSimpleCheckWithSource**

Same entity swap as Step 1, plus:
```python
check_number = clamp(data.get('check_number', ''), 64)
```
Add to SOAP params:
```python
"sCheckNumber": check_number,
```

- [ ] **Step 3: Fix PaymentSimpleBankTransferWithSource**

Same entity swap, plus:
```python
transfer_number = clamp(data.get('transfer_number', ''), 64)
```
SOAP params:
```python
"sTransferNumber": transfer_number,
```

- [ ] **Step 4: Fix PaymentMultipleWithSource_v3**

Read the WSDL carefully for this one — it may need a different approach since it handles multiple payments. Fix the entity model and param names.

- [ ] **Step 5: Fix RefundPaymentCash**

Entity swap from `iLedgerID` to `iTenantID` + `iUnitID`. Remove phantom `dcRefund` param (SMD derives refund from payment record).

- [ ] **Step 6: Fix RefundPaymentCheck**

Same entity swap as RefundPaymentCash.

- [ ] **Step 7: Verify import**

Run: `cd backend/python && python -c "from web.routes.billing import billing_bp; print('OK')"`

- [ ] **Step 8: Commit**

```bash
git add backend/python/web/routes/billing.py
git commit -m "fix(billing): payment/refund entity model rewrite

All payment and refund routes now use iTenantID+iUnitID (WSDL)
instead of iLedgerID. PaymentSimpleCheckWithSource gets sCheckNumber
field for Stripe transaction ID recording."
```

---

### Task 8: Fix Category C — CustomerAccounts entity remodel + Category E

**Files:**
- Modify: `backend/python/web/routes/billing.py`

These routes take `ledger_id` but WSDL needs `iTenantID`. Since no consumers exist, change the route input.

- [ ] **Step 1: Fix CustomerAccountsMakeFutureCharges**

Change route input from `ledger_id` to `tenant_id`. Add `number_of_future_periods` body field. Fix params:
```python
"iTenantID": str(tenant_id),
"iNumberOfFuturePeriods": str(n_periods),
"dFutureDueDate": f"{future_due_date}T00:00:00" if "T" not in str(future_due_date) else str(future_due_date),
```

- [ ] **Step 2: Fix CustomerAccountsBalanceDetails_v2**

Change route from `/<int:ledger_id>` to `/<int:tenant_id>`. Fix param:
```python
"iTenantID": str(tenant_id),
```

- [ ] **Step 3: Fix CustomerAccountsBalanceDetailsWithDiscount**

Change to `tenant_id`, add `unit_id` and `concession_plan_id` query params:
```python
"iTenantID": str(tenant_id),
"iUnitID": str(unit_id),
"ConcessionPlanID": str(concession_plan_id),
```

- [ ] **Step 4: Fix TenantInvoicesByTenantID**

Rename param `iTenantID` → `sTenantIDsCommaDelimited`. Add `date_start`/`date_end` query params with 90-day default:
```python
from datetime import datetime, timedelta
date_end = request.args.get('date_end', datetime.now().strftime('%Y-%m-%dT00:00:00'))
date_start = request.args.get('date_start', (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%dT00:00:00'))
```

- [ ] **Step 5: Fix InsuranceCoverageAddToLedger**

Entity swap from `ledger_id` to `tenant_id` + `unit_id`. Add `policy_number` and `start_date` body fields:
```python
"TenantID": str(tenant_id),
"UnitID": str(unit_id),
"InsuranceCoverageID": str(insurance_coverage_id),
"sPolicyNumber": policy_number,
"dStartDate": f"{start_date}T00:00:00",
```

- [ ] **Step 6: Verify import**

Run: `cd backend/python && python -c "from web.routes.billing import billing_bp; print('OK')"`

- [ ] **Step 7: Commit**

```bash
git add backend/python/web/routes/billing.py
git commit -m "fix(billing): entity model rewrite for CustomerAccounts + invoices + insurance

Routes now use tenant_id/unit_id matching WSDL entity model.
TenantInvoicesByTenantID gets date range params.
InsuranceCoverageAddToLedger uses tenant+unit instead of ledger."
```

---

## Phase 2: Booking Engine SOAP Routes (P1)

### Task 9: Downgrade InsuranceCoverageRetrieve from V3 to V2

**Files:**
- Modify: `backend/python/web/routes/reservations.py:2292-2319`

- [ ] **Step 1: Change the operation name**

At line ~2293, change:
```python
operation="InsuranceCoverageRetrieve_V3",
```
to:
```python
operation="InsuranceCoverageRetrieve_V2",
```

At line ~2298, change the soap_action:
```python
soap_action=_cc_soap_action("InsuranceCoverageRetrieve_V2"),
```

Update the 3 logger.error strings from `InsuranceCoverageRetrieve_V3` to `InsuranceCoverageRetrieve_V2`.

- [ ] **Step 2: Verify**

Run: `cd backend/python && python -c "from web.routes.reservations import reservations_bp; print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add backend/python/web/routes/reservations.py
git commit -m "fix(reservations): downgrade InsuranceCoverageRetrieve V3 → V2

V3 returns 0 results on LSETUP. V2 returns all 11 plans correctly.
Confirmed via LSETUP testing 2026-04-16."
```

---

### Task 10: Add ReservationFeeAddWithSource_v2 route

**Files:**
- Modify: `backend/python/web/routes/reservations.py`

- [ ] **Step 1: Add the route after the existing ReservationFeeRetrieve route**

Find the `insurance_coverage` route section in reservations.py. Add the new route before it:

```python
# =============================================================================
# POST /api/reservations/fee/add — add reservation fee (dummy CC)
# =============================================================================

@reservations_bp.route('/fee/add', methods=['POST'])
@require_auth
@require_api_scope('reservations:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def add_reservation_fee():
    """
    ReservationFeeAddWithSource_v2 — charge reservation fee with dummy CC.

    SiteLink has no payment processor, so dummy CC values pass validation.
    iCreditCardType=5 is the only value that works (0-4 fail).

    JSON body:
        site_code       — location code             [required]
        tenant_id       — tenant ID                 [required]
        waiting_id      — reservation WaitingID     [required]
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
                "sBillingName": "ESA Booking",
                "sBillingAddress": "",
                "sBillingZipCode": "000000",
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
```

- [ ] **Step 2: Verify**

Run: `cd backend/python && python -c "from web.routes.reservations import reservations_bp; print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add backend/python/web/routes/reservations.py
git commit -m "feat(reservations): add ReservationFeeAddWithSource_v2 route

Uses dummy CC pattern (iCreditCardType=5) since SiteLink has no
payment processor. Confirmed working on LSETUP with bTestMode=true."
```

---

### Task 11: Create units blueprint (available units + price list)

**Files:**
- Create: `backend/python/web/routes/units.py`
- Modify: `backend/python/web/app.py` (register blueprint)

- [ ] **Step 1: Create `units.py` blueprint**

```python
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
```

- [ ] **Step 2: Register the blueprint in app.py**

Find where other blueprints are registered (e.g. `app.register_blueprint(billing_bp)`) and add:
```python
from web.routes.units import units_bp
app.register_blueprint(units_bp)
```

- [ ] **Step 3: Verify**

Run: `cd backend/python && python -c "from web.routes.units import units_bp; print('OK')"`

- [ ] **Step 4: Commit**

```bash
git add backend/python/web/routes/units.py backend/python/web/app.py
git commit -m "feat(units): add units blueprint with available units and price list

UnitsInformationAvailableUnitsOnly_v2 at GET /api/units/available
UnitTypePriceList_v2 at GET /api/units/price-list
Both scoped under reservations:read for booking engine use."
```

---

## Phase 3: Data Pipelines + Cost Calculator (P1)

### Task 12: Create ccws_charge_descriptions pipeline

**Files:**
- Create: `sql/028_charge_descriptions_and_insurance.sql`
- Modify: `backend/python/common/models.py` (add CcwsChargeDescription model)
- Create: `backend/python/datalayer/cc_charge_descriptions_to_sql.py`

- [ ] **Step 1: Create the migration SQL**

```sql
-- 028_charge_descriptions_and_insurance.sql
-- Charge type configuration per site (tax rates, default prices)

CREATE TABLE IF NOT EXISTS ccws_charge_descriptions (
    id SERIAL PRIMARY KEY,
    "ChargeDescID" INTEGER NOT NULL,
    "SiteID" INTEGER NOT NULL,
    "SiteCode" VARCHAR(20),
    "sChgDesc" VARCHAR(255),
    "sChgCategory" VARCHAR(100),
    "dcPrice" NUMERIC(14,4) DEFAULT 0,
    "dcTax1Rate" NUMERIC(14,6) DEFAULT 0,
    "dcTax2Rate" NUMERIC(14,6) DEFAULT 0,
    "bApplyAtMoveIn" BOOLEAN DEFAULT FALSE,
    "bProrateAtMoveIn" BOOLEAN DEFAULT FALSE,
    "bPermanent" BOOLEAN DEFAULT FALSE,
    "dDisabled" TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE ("ChargeDescID", "SiteID")
);

CREATE INDEX IF NOT EXISTS idx_ccws_charge_desc_site ON ccws_charge_descriptions ("SiteID");
CREATE INDEX IF NOT EXISTS idx_ccws_charge_desc_category ON ccws_charge_descriptions ("sChgCategory");

-- Insurance coverage plans per site
CREATE TABLE IF NOT EXISTS ccws_insurance_coverage (
    id SERIAL PRIMARY KEY,
    "InsurCoverageID" INTEGER NOT NULL,
    "SiteID" INTEGER NOT NULL,
    "SiteCode" VARCHAR(20),
    "dcCoverage" NUMERIC(14,4) DEFAULT 0,
    "dcPremium" NUMERIC(14,4) DEFAULT 0,
    "dcPCTheft" NUMERIC(14,4) DEFAULT 0,
    "sCoverageDesc" VARCHAR(255),
    "sProvidor" VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE ("InsurCoverageID", "SiteID")
);

CREATE INDEX IF NOT EXISTS idx_ccws_insurance_site ON ccws_insurance_coverage ("SiteID");
```

- [ ] **Step 2: Add SQLAlchemy models to `common/models.py`**

Add at the end of the file, before any `Base.metadata.create_all` calls:

```python
class CcwsChargeDescription(Base):
    __tablename__ = 'ccws_charge_descriptions'
    id = Column(Integer, primary_key=True)
    ChargeDescID = Column(Integer, nullable=False)
    SiteID = Column(Integer, nullable=False)
    SiteCode = Column(String(20))
    sChgDesc = Column(String(255))
    sChgCategory = Column(String(100))
    dcPrice = Column(Numeric(14, 4), default=0)
    dcTax1Rate = Column(Numeric(14, 6), default=0)
    dcTax2Rate = Column(Numeric(14, 6), default=0)
    bApplyAtMoveIn = Column(Boolean, default=False)
    bProrateAtMoveIn = Column(Boolean, default=False)
    bPermanent = Column(Boolean, default=False)
    dDisabled = Column(DateTime)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class CcwsInsuranceCoverage(Base):
    __tablename__ = 'ccws_insurance_coverage'
    id = Column(Integer, primary_key=True)
    InsurCoverageID = Column(Integer, nullable=False)
    SiteID = Column(Integer, nullable=False)
    SiteCode = Column(String(20))
    dcCoverage = Column(Numeric(14, 4), default=0)
    dcPremium = Column(Numeric(14, 4), default=0)
    dcPCTheft = Column(Numeric(14, 4), default=0)
    sCoverageDesc = Column(String(255))
    sProvidor = Column(String(255))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
```

- [ ] **Step 3: Create the datalayer pipeline**

Create `backend/python/datalayer/cc_charge_descriptions_to_sql.py` following the existing pipeline pattern from `cc_discount_plans_to_sql.py`. The pipeline should:
1. Iterate all sites from `siteinfo`
2. Call `ChargeDescriptionsRetrieve` per site
3. Upsert into `ccws_charge_descriptions` on `(ChargeDescID, SiteID)`
4. Filter out disabled charge types (`dDisabled IS NOT NULL`)

- [ ] **Step 4: Run the migration**

Run: `PGPASSWORD=<PBI_PW> psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d esa_pbi -f sql/028_charge_descriptions_and_insurance.sql`

- [ ] **Step 5: Commit**

```bash
git add sql/028_charge_descriptions_and_insurance.sql backend/python/common/models.py backend/python/datalayer/cc_charge_descriptions_to_sql.py
git commit -m "feat(datalayer): add charge descriptions + insurance coverage sync pipelines

New tables: ccws_charge_descriptions, ccws_insurance_coverage
Syncs ChargeDescriptionsRetrieve per site — provides per-charge-type
tax rates (Rent=9%, Insurance=8%, POS=7%) and admin fee amounts."
```

---

### Task 13: Create insurance coverage pipeline

**Files:**
- Create: `backend/python/datalayer/cc_insurance_coverage_to_sql.py`

- [ ] **Step 1: Create the pipeline**

Follow the same pattern as the charge descriptions pipeline. Key differences:
- SOAP operation: `InsuranceCoverageRetrieve_V2` (NOT V3)
- Parameters: `sLocationCode` only (no `iUnitID` needed for full list)
- Result tag: `Table`
- Upsert on `(InsurCoverageID, SiteID)`

- [ ] **Step 2: Commit**

```bash
git add backend/python/datalayer/cc_insurance_coverage_to_sql.py
git commit -m "feat(datalayer): add insurance coverage sync pipeline

Syncs InsuranceCoverageRetrieve_V2 per site into ccws_insurance_coverage.
Uses V2 (not V3) — V3 returns 0 results on LSETUP."
```

---

### Task 14: Build MoveInCost calculator

**Files:**
- Create: `backend/python/common/movein_cost_calculator.py`

- [ ] **Step 1: Create the calculator module**

```python
"""
Internal MoveInCost calculator.

Replicates SiteLink's MoveInCostRetrieveWithDiscount calculation for
display-only cost estimates. NOT for binding amounts — use SOAP for
the exact dcPaymentAmount at checkout.

Supports two billing modes:
- 1st-of-month (bAnnivDateLeasing=false): prorate partial first month
- Anniversary (bAnnivDateLeasing=true): full month from move-in date
"""

from calendar import monthrange
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass


@dataclass
class ChargeTypeTax:
    """Tax rates for a specific charge type (from ccws_charge_descriptions)."""
    category: str
    tax1_rate: Decimal  # percentage, e.g. 9.0 for 9%
    tax2_rate: Decimal
    default_price: Decimal


@dataclass
class CostLine:
    """One line in the cost breakdown."""
    description: str
    charge_amount: Decimal
    discount: Decimal
    tax1: Decimal
    tax2: Decimal
    total: Decimal


def _round2(value):
    """Round to 2 decimal places."""
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _prorate(monthly_amount, move_in_day, days_in_month):
    """Prorate a monthly amount for a partial month (inclusive of move-in day)."""
    remaining = days_in_month - move_in_day + 1
    return _round2(Decimal(str(monthly_amount)) * Decimal(remaining) / Decimal(days_in_month))


def _tax(amount, rate_pct):
    """Calculate tax from a percentage rate (e.g. 9.0 = 9%)."""
    return _round2(Decimal(str(amount)) * Decimal(str(rate_pct)) / Decimal("100"))


def calculate_movein_cost(
    std_rate,
    security_deposit,
    admin_fee,
    move_in_date,
    rent_tax,
    admin_tax=None,
    deposit_tax=None,
    insurance_tax=None,
    pc_discount=0,
    insurance_premium=0,
    anniversary_billing=False,
    day_start_prorate_plus_next=17,
):
    """
    Calculate estimated move-in cost.

    Args:
        std_rate: Monthly rent rate
        security_deposit: Flat deposit amount
        admin_fee: Flat admin fee amount (from ChargeDescriptionsRetrieve)
        move_in_date: datetime.date or datetime.datetime
        rent_tax: ChargeTypeTax for Rent category
        admin_tax: ChargeTypeTax for AdminFee (defaults to rent_tax)
        deposit_tax: ChargeTypeTax for SecDep (defaults to 0%)
        insurance_tax: ChargeTypeTax for Insurance (defaults to rent_tax)
        pc_discount: Percentage discount (0-100)
        insurance_premium: Monthly insurance premium (0 if no insurance)
        anniversary_billing: True for anniversary mode (no proration)
        day_start_prorate_plus_next: Threshold for second month charge

    Returns:
        List[CostLine]
    """
    admin_tax = admin_tax or rent_tax
    deposit_tax = deposit_tax or ChargeTypeTax("SecDep", Decimal("0"), Decimal("0"), Decimal("0"))
    insurance_tax = insurance_tax or rent_tax

    day = move_in_date.day
    _, days_in_month = monthrange(move_in_date.year, move_in_date.month)

    charges = []

    # --- First Monthly Rent ---
    if anniversary_billing:
        rent_base = _round2(Decimal(str(std_rate)))
    else:
        rent_base = _prorate(std_rate, day, days_in_month)

    discount_amt = Decimal("0")
    if pc_discount > 0:
        discount_amt = _round2(rent_base * Decimal(str(pc_discount)) / Decimal("100"))

    rent_after_disc = rent_base - discount_amt
    rent_t1 = _tax(rent_after_disc, rent_tax.tax1_rate)
    rent_t2 = _tax(rent_after_disc, rent_tax.tax2_rate)

    charges.append(CostLine(
        description="First Monthly Rent Fee",
        charge_amount=rent_base,
        discount=discount_amt,
        tax1=rent_t1,
        tax2=rent_t2,
        total=rent_after_disc + rent_t1 + rent_t2,
    ))

    # --- Admin Fee ---
    admin = _round2(Decimal(str(admin_fee)))
    admin_t1 = _tax(admin, admin_tax.tax1_rate)
    admin_t2 = _tax(admin, admin_tax.tax2_rate)
    charges.append(CostLine(
        description="Administrative Fee",
        charge_amount=admin,
        discount=Decimal("0"),
        tax1=admin_t1,
        tax2=admin_t2,
        total=admin + admin_t1 + admin_t2,
    ))

    # --- Security Deposit ---
    dep = _round2(Decimal(str(security_deposit)))
    dep_t1 = _tax(dep, deposit_tax.tax1_rate)
    dep_t2 = _tax(dep, deposit_tax.tax2_rate)
    charges.append(CostLine(
        description="Security Deposit",
        charge_amount=dep,
        discount=Decimal("0"),
        tax1=dep_t1,
        tax2=dep_t2,
        total=dep + dep_t1 + dep_t2,
    ))

    # --- Second Month (1st-of-month billing, late move-in) ---
    if not anniversary_billing and day >= day_start_prorate_plus_next:
        full_rent = _round2(Decimal(str(std_rate)))
        disc2 = Decimal("0")
        if pc_discount > 0:
            disc2 = _round2(full_rent * Decimal(str(pc_discount)) / Decimal("100"))
        rent2_after = full_rent - disc2
        rent2_t1 = _tax(rent2_after, rent_tax.tax1_rate)
        rent2_t2 = _tax(rent2_after, rent_tax.tax2_rate)
        charges.append(CostLine(
            description="Second Monthly Rent Fee",
            charge_amount=full_rent,
            discount=disc2,
            tax1=rent2_t1,
            tax2=rent2_t2,
            total=rent2_after + rent2_t1 + rent2_t2,
        ))

    # --- Insurance ---
    if insurance_premium > 0:
        if anniversary_billing:
            ins_base = _round2(Decimal(str(insurance_premium)))
        else:
            ins_base = _prorate(insurance_premium, day, days_in_month)

        ins_t1 = _tax(ins_base, insurance_tax.tax1_rate)
        charges.append(CostLine(
            description="First Month Insurance",
            charge_amount=ins_base,
            discount=Decimal("0"),
            tax1=ins_t1,
            tax2=Decimal("0"),
            total=ins_base + ins_t1,
        ))

        # Second month insurance (if second month rent was added)
        if not anniversary_billing and day >= day_start_prorate_plus_next:
            ins2_base = _round2(Decimal(str(insurance_premium)))
            ins2_t1 = _tax(ins2_base, insurance_tax.tax1_rate)
            charges.append(CostLine(
                description="Second Month Insurance",
                charge_amount=ins2_base,
                discount=Decimal("0"),
                tax1=ins2_t1,
                tax2=Decimal("0"),
                total=ins2_base + ins2_t1,
            ))

    return charges


def estimate_total(charges):
    """Sum all charge line totals."""
    return sum(c.total for c in charges)
```

- [ ] **Step 2: Verify module imports**

Run: `cd backend/python && python -c "from common.movein_cost_calculator import calculate_movein_cost, ChargeTypeTax; print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add backend/python/common/movein_cost_calculator.py
git commit -m "feat(common): add MoveInCost internal calculator

Supports 1st-of-month (proration) and anniversary (full month) billing.
Uses per-charge-type tax rates from ChargeDescriptionsRetrieve.
For display estimates only — SOAP is authoritative for checkout amounts."
```

---

### Task 15: Add RentTaxRatesRetrieve route to billing.py

**Files:**
- Modify: `backend/python/web/routes/billing.py`

- [ ] **Step 1: Add the route**

Add near the existing `charge_types` route (they're both site config reads):

```python
@billing_bp.route('/tax-rates', methods=['GET'])
@require_auth
@require_api_scope('billing:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def tax_rates():
    """RentTaxRatesRetrieve — get site-level tax configuration."""
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
```

- [ ] **Step 2: Commit**

```bash
git add backend/python/web/routes/billing.py
git commit -m "feat(billing): add RentTaxRatesRetrieve route

GET /api/billing/tax-rates?site_code=X — returns site-level
dcTax1Rate and dcTax2Rate from SiteLink."
```

---

### Task 16: Add discount plans REST endpoint

**Files:**
- Modify: `backend/python/web/routes/reservations.py`

- [ ] **Step 1: Add a DB-backed discount plans route**

This reads from the `ccws_discount` table (already synced), not from SOAP:

```python
# =============================================================================
# GET /api/reservations/discount-plans — available discounts from DB
# =============================================================================

@reservations_bp.route('/discount-plans', methods=['GET'])
@require_auth
@require_api_scope('reservations:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def discount_plans():
    """
    Retrieve active discount plans for a site from the ccws_discount DB table.

    DiscountPlansRetrieve SOAP returns empty — use synced DB data instead.

    Query params:
        site_code — location code [required]
    """
    from sqlalchemy import text
    from web.utils.soap_helpers import get_pbi_session

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
```

- [ ] **Step 2: Verify**

Run: `cd backend/python && python -c "from web.routes.reservations import reservations_bp; print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add backend/python/web/routes/reservations.py
git commit -m "feat(reservations): add discount plans REST endpoint from DB

GET /api/reservations/discount-plans?site_code=X
Reads from ccws_discount table (synced via pipeline).
SOAP DiscountPlansRetrieve returns empty — DB-first approach."
```

---

## Cleanup

### Task 17: Clean up test files

**Files:**
- Delete: `backend/python/test_lsetup_booking_flow.py`
- Delete: `backend/python/test_lsetup_fee_retry.py`
- Delete: `backend/python/test_lsetup_round2.py`
- Delete: `backend/python/test_lsetup_round3.py`
- Delete: `backend/python/test_lsetup_cost_validation.py`

- [ ] **Step 1: Move test files to a test artifacts directory**

```bash
mkdir -p backend/python/tests/lsetup_validation
mv backend/python/test_lsetup_*.py backend/python/tests/lsetup_validation/
```

- [ ] **Step 2: Commit**

```bash
git add backend/python/tests/lsetup_validation/ backend/python/test_lsetup_*.py
git commit -m "chore: move LSETUP validation scripts to tests/lsetup_validation/"
```

---

## Verification Checklist

After all tasks complete:

- [ ] `cd backend/python && python -c "from web.routes.billing import billing_bp; print('OK')"` — billing imports
- [ ] `cd backend/python && python -c "from web.routes.reservations import reservations_bp; print('OK')"` — reservations imports
- [ ] `cd backend/python && python -c "from web.routes.units import units_bp; print('OK')"` — units imports
- [ ] `cd backend/python && python -c "from common.movein_cost_calculator import calculate_movein_cost; print('OK')"` — calculator imports
- [ ] Grep billing.py for `audit_log(` — every occurrence must be inside a success guard, never before `Ret_Code` check
- [ ] Grep billing.py for `"iLedgerID"` — should return 0 matches (all renamed)
- [ ] Grep billing.py for `PaymentSimpleWithSource_v3` — should return 0 matches (deleted)
- [ ] Grep billing.py for `RefundPaymentCreditCard_v2` — should return 0 matches (deleted)
