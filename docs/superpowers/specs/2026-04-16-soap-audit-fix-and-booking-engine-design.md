# SOAP Audit Fix & Booking Engine SOAP Integration — Design Spec

**Date:** 2026-04-16
**Status:** Draft
**Scope:** Two workstreams — (1) billing.py remediation, (2) booking engine SOAP integration gaps

---

## Context

A full audit of `backend/python/web/routes/` against the live SMD WSDLs revealed 25 broken SOAP call sites in `billing.py`. Separately, mapping the booking engine SOAP flow against existing routes uncovered 4+ missing endpoints and critical integration findings from LSETUP testing.

**billing.py** is intentional scaffolding for future tenant portal and admin operations (post-move-in lifecycle). It was pre-built to cover all necessary CallCenterWs operations but has WSDL param name drift and missing safety guards. Zero consumers exist today — no `billing:read`/`billing:write` scopes are registered, no frontend or MCP references found.

**Booking engine** needs two flows supported by SOAP:
- Flow 1 (direct move-in): select unit + discount → move-in
- Flow 2 (paid reservation): select unit + discount → reserve → Stripe payment → move-in

---

## Part 1 — billing.py Remediation

### 1.1 Root cause

SMD's CallCenterWs silently accepts SOAP requests with wrong param names, returns `Ret_Code=-1` in a 200 OK response. Routes without `Ret_Code` guards report success while SMD did nothing.

### 1.2 Cross-cutting bugs (all write routes)

1. **`audit_log` fires unconditionally** — before the success check. Creates phantom audit records for operations that never posted to SiteLink. This is the primary financial integrity risk.
2. **Routes return HTTP 200 `status:error`** on SMD failure — should return HTTP 502.
3. **Param names don't match WSDL** — case-sensitive mismatches (`iLedgerID` vs `ledgerId`).

### 1.3 Architecture: shared SOAP billing helper (Approach B)

Extract repeated boilerplate into one function:

```python
def _billing_soap_call(site_code, operation, params, audit_event=None, audit_detail=""):
    """
    1. Get SOAP client
    2. Call operation with site auth + params
    3. Parse result, extract Ret_Code
    4. If Ret_Code <= 0: log error, return (None, error_response_502)
    5. If success AND audit_event: audit_log(...)
    6. Return (results, None)
    """
```

Each route handler becomes ~15 lines: validate inputs → call helper → format response. The helper **structurally prevents** audit-before-success.

### 1.4 Entity model: match WSDL

Routes accept the same IDs the SOAP operation expects. `site_code` is always required (IDs are site-scoped — `tenant_id`, `ledger_id`, `charge_id` are NOT globally unique).

ID hierarchy:
- Payment path: `site_code → tenant_id → ledger_id → charge_id`
- Inventory path: `site_code → unit_id → tenant_id`

No server-side ID resolution. Callers pass the full chain.

### 1.5 Classification

| Category | Operations | Action |
|----------|-----------|--------|
| **TENANT_PORTAL (view)** | LedgersByTenantID_v3, ChargesAllByLedgerID, ChargesAndPaymentsByLedgerID, LedgerStatementByLedgerID, PaymentsByLedgerID, CustomerAccountsBalanceDetails_v2, CustomerAccountsBalanceDetailsWithDiscount, TenantInvoicesByTenantID, InsuranceLedgerStatusByLedgerID, ChargeDescriptionsRetrieve | Fix params |
| **TENANT_PORTAL (action)** | PaymentSimpleCheckWithSource (Stripe→SiteLink bridge), PaymentSimpleCashWithSource, PaymentSimpleBankTransferWithSource, ScheduleMoveOut, InsuranceCoverageAddToLedger | Fix params + entity model |
| **ADMIN** | ChargeAddToLedger, RecurringChargeAddToLedger_v1, CustomerAccountsMakeFutureCharges, ChargePriceUpdate, ApplyCredit, LedgerTransferToNewTenant, RemoveDiscountFromLedger, PaymentMultipleWithSource_v3, RefundPaymentCash, RefundPaymentCheck | Fix params + entity model |
| **DELETE** | PaymentSimpleWithSource_v3 (CC), RefundPaymentCreditCard_v2 (CC) | Remove — SiteLink CC processing not used |

### 1.6 PR plan

| PR | Contents | Risk |
|----|----------|------|
| **PR1** | Shared helper + fix audit-before-success on ALL write routes + Cat F (5 renames) + ScheduleMoveOut date fix + delete 2 CC routes | Low |
| **PR2** | Cat B/D/E non-contract fixes (ChargeAddToLedger, LedgerTransferToNewTenant) + contract-change routes (entity model corrections) | Medium |
| **PR3** | Cat A payment/refund entity rewrite (6 routes) + Cat C CustomerAccounts entity remodel (3 routes) | Medium |

### 1.7 Acceptance criteria (per route)

- SOAP op + param names match WSDL exactly (case-sensitive)
- DateTime params use `%Y-%m-%dT%H:%M:%S` format
- All `minOccurs="1"` params present
- `Ret_Code <= 0` → log error + HTTP 502 with generic message
- `audit_log` on success path only
- No `str(e)` leaked to response body
- Non-destructive verification on reads (L025), LSETUP for writes

### 1.8 Stripe→SiteLink payment recording

For tenant portal payments processed via Stripe, record in SiteLink as a check payment:
- `PaymentSimpleCheckWithSource` with `sCheckNumber` = Stripe payment intent ID (`pi_xxx`)
- WSDL entity: `iTenantID` + `iUnitID` (not `iLedgerID`)
- `sSource` = payment channel identifier

Move-in operations do NOT have a check number / memo field. Stripe ref for move-in payments goes in tenant notes (`TenantNoteInsert_v2`).

---

## Part 2 — Booking Engine SOAP Integration

### 2.1 Booking flows (confirmed on LSETUP)

**Flow 1 — Direct move-in:**
1. Browse available units → `UnitsInformationAvailableUnitsOnly_v2`
2. Show pricing → `UnitTypePriceList_v2`
3. Fetch discounts → read from `ccws_discount` DB table (SOAP `DiscountPlansRetrieve` returns empty — see 2.5)
4. Fetch insurance → `InsuranceCoverageRetrieve` v1 or V2 (V3 returns 0 on LSETUP — see 2.5)
5. Get exact cost → `MoveInCostRetrieveWithDiscount_v4`
6. Create tenant → `TenantNewDetailed_v3`
7. Move-in → `MoveInWithDiscount_v7` with `iPayMethod=2` (cash bypass)

**Flow 2 — Paid reservation:**
Steps 1–4 same, then:
5. Create tenant → `TenantNewDetailed_v3`
6. Reserve unit → `ReservationNewWithSource_v6` (carries `ConcessionID`)
7. Stripe checkout (external)
8. Record reservation fee → `ReservationFeeAddWithSource_v2` with `iCreditCardType=5` + dummy CC
9. Get exact cost → `MoveInCostRetrieveWithDiscount_Reservation_v4`
10. Move-in → `MoveInReservation_v6` with `iPayMethod=2` (cash bypass)

### 2.2 LSETUP test results

| Test | Result | Finding |
|------|--------|---------|
| Reservation without MoveInCostRetrieve | OK (WaitingID returned) | Reservation doesn't need cost upfront |
| ReservationFeeRetrieve | dcPrice=$100, bReqReservationFee=true | Fee is site-configured |
| ReservationFeeAdd iCreditCardType=0–4 | All fail: "Error getting Credit Card Type ID" | Types 0–4 don't map to configured CC types |
| **ReservationFeeAdd iCreditCardType=5** | **Ret_Code=1 (SUCCESS, bTestMode=true)** | CC type 5 works — SiteLink has no processor so dummy CC passes |
| MoveIn wrong amount ($99.99) | Ret_Code=-11 "does not match required payment of $139.81" | **Amount match is strict to the cent** |
| MoveIn correct amount ($139.81) | Ret_Code=592852 (LedgerID) | Cash bypass works |
| DiscountPlansRetrieve | 0 plans via SOAP, 15 in DB | SOAP returns empty — use DB table |
| InsuranceCoverageRetrieve v1/V2 | 11 plans (AON Singapore, $3–$40/mo) | v1/V2 work, V3 returns 0 |
| InsuranceCoverageRetrieve_V3 | 0 plans | V3 broken on LSETUP — use V2 |
| MoveInCost + 5% discount | $143.94 (saved $1.91) | Discount applies to rent only |
| MoveInCost + insurance ($3/mo) | $147.36 (added $1.51 prorated) | Insurance prorated to partial month |
| MoveInCost + discount + insurance | $145.45 | Combinations work correctly |

### 2.3 Missing endpoints (must wire as API routes)

| Operation | Purpose | Where to wire |
|-----------|---------|---------------|
| `UnitsInformationAvailableUnitsOnly_v2` | Available units for browsing | New blueprint or api.py |
| `UnitTypePriceList_v2` | Unit type pricing | Same |
| `MoveInCostRetrieveWithDiscount_v4` | Exact cost breakdown (already wired in reservations.py) | Expose via API if not already |
| `MoveInCostRetrieveWithDiscount_Reservation_v4` | Cost for reservation-based move-in (already wired) | Same |
| `ReservationFeeAddWithSource_v2` | Record paid reservation fee | reservations.py |

**Already wired and confirmed OK:** TenantNewDetailed_v3, ReservationNewWithSource_v6, ReservationUpdate_v4, MoveInWithDiscount_v7, MoveInReservation_v6, InsuranceCoverageRetrieve_V3 (needs version downgrade), InsuranceCoverageMinimumsRetrieve, ReservationFeeRetrieve.

### 2.4 Discount plans — DB-first approach

`DiscountPlansRetrieve` SOAP returns empty despite plans existing in SiteLink. The `ccws_discount` table (synced by `cc_discount_plans_to_sql.py` pipeline) has all plans with full metadata.

Booking engine reads discounts from `ccws_discount` WHERE `dDisabled IS NULL` AND site matches. The `ConcessionID` is passed to reservation/move-in operations.

No new SOAP endpoint needed — just a REST API route over the DB table.

### 2.5 Insurance — use V2, not V3

`InsuranceCoverageRetrieve_V3` returns 0 results on LSETUP despite 11 plans existing. V1 and V2 both return all 11 plans correctly. The existing route in reservations.py uses V3 — **downgrade to V2**.

Field mapping: `InsurCoverageID` (use as `InsuranceCoverageID` in move-in), `dcCoverage` (coverage amount), `dcPremium` (monthly premium), `sProvidor` (provider name).

### 2.6 Reservation fee — dummy CC pattern

`ReservationFeeAddWithSource_v2` works with:
```python
iCreditCardType = 5       # only value that works (0-4 all fail)
sCreditCardNumber = "4111111111111111"  # standard test Visa
sCreditCardCVV = "123"
dExpirationDate = "2030-01-01T00:00:00"  # placeholder (never empty!)
sBillingName = "<tenant name>"
sBillingAddress = "<tenant address>"
sBillingZipCode = "<tenant postal>"
```

**Needs real-mode test** (`bTestMode=false`) on LSETUP before production use.

### 2.7 MoveInCostRetrieve — internal replication assessment

**Status:** Under investigation (reverse-engineering agents dispatched).

Preliminary analysis from LSETUP test data:
- **Proration**: `rate * remaining_days / days_in_month` — matches exactly for LSETUP
- **Tax**: 9% GST on rent and admin fee, 0% on security deposit — matches
- **Discount**: applied to rent charge before tax, not to admin/deposit — matches
- **Insurance**: prorated same as rent, taxed at a potentially different rate

**Open question:** can we replicate this reliably across all sites with varying tax rates, proration rules (`iDayStrtProrating`, `b2ndMonthProrate`, `bAnnivDateLeasing`), and rounding? Agent findings pending.

### Validation results (60 scenarios on LSETUP)

**29/60 passed (48.3%)** — but failures are exactly two fixable patterns:

**What matches 100%:**
- Rent proration — exact to the cent across 6 dates, 2 units, all rates
- Tax calculation — 9% GST applied correctly
- Discount application — 5%, 10% exact (discount before tax)
- Admin fee — $30 flat + 9% tax
- Security deposit — flat, no tax
- 0% tax units — handled correctly

**Two failure patterns:**

1. **Insurance tax rate**: insurance uses 8% tax (not 9% GST). Fix: use coverage-specific tax rate, not unit tax rate.
2. **Second Monthly Rent Fee**: when move-in is late in month (day >= `iDayStrtProratePlusNext`), SiteLink charges prorated first month + full second month. Fix: detect threshold and add second charge line.

### Two billing modes (must support both)

**1st-of-month billing** (LSETUP):
- `bAnnivDateLeasing=false`, `iDayStrtProrating=1`
- Prorate: `rate * remaining_days / days_in_month`
- Late move-in threshold: if `day >= iDayStrtProratePlusNext`, add full second month
- All validation tests confirm this formula

**Anniversary billing** (all other ESA sites currently):
- `bAnnivDateLeasing=true`
- No proration — full month from move-in date
- No second month charge
- Simpler but requires a separate code path
- **Not yet validated on LSETUP** — need to test against a real anniversary site

### Data gaps for internal replication

| Data needed | Available? | Source | Gap |
|-------------|-----------|--------|-----|
| Unit rate (`dcStdRate`) | Yes | `units_info` | — |
| Security deposit (`dcStdSecDep`) | Yes | `units_info` | — |
| Tax rate (`dcTax1Rate`) | Yes | `units_info` | Rate is in % (9.0), not decimal |
| Discount % (`dcPCDiscount`) | Yes | `ccws_discount` | — |
| Admin fee | Partial | `cc_ledgers` (historical) | No site-level config table; varies per site ($0–$300) |
| Insurance premium | No | SOAP only | No `ccws_coverage` table synced |
| Insurance tax rate | No | SOAP only | Different from unit tax rate (8% vs 9% on LSETUP) |
| Proration config (`iDayStrtProrating`) | No | SOAP only | Not synced to any DB table |
| Billing mode (`bAnnivDateLeasing`) | No | SOAP only | Not synced |
| Second month threshold (`iDayStrtProratePlusNext`) | No | SOAP only | Not synced |

### Recommended approach: hybrid (cache SOAP + internal fallback)

1. **Primary**: call SOAP at quote time, cache result (keyed on `site+unit+date+concession+insurance`, 10–15 min TTL)
2. **Internal calculator as fallback**: for display-only estimates during browsing (before exact quote)
3. **Shadow mode**: run both paths in parallel during validation period, log mismatches
4. **New sync pipeline**: add `site_billing_config` table to capture per-site proration/billing mode settings via one-time SOAP call per site (MoveInCostRetrieve on a known unit, extract the config fields)
5. **Insurance coverage table**: new `ccws_insurance_coverage` pipeline to sync coverage plans with premiums and tax rates

---

## Part 3 — Scoped Out (Future Work)

- **Booking engine frontend** — Jinja2 templates for the booking flow UI
- **Stripe integration** — webhook handler, payment intent creation, refund flow
- **DiscountPlansRetrieve SOAP debugging** — why does it return empty? Low priority since DB-first works
- **InsuranceCoverageRetrieve_V3 investigation** — why 0 results? Low priority since V2 works
- **CancelInsurancePolicy, InsuranceRateUpdate** — tenant portal insurance management
- **billing:read / billing:write scope registration** — add to api_key.py when first consumer arrives
- **SendPaymentConfirmationEmail, SendReservationConfirmationEmail** — email notifications

---

## Design Decisions Log

| Decision | Rationale |
|----------|-----------|
| Match WSDL entity model (no server-side ID resolution) | Booking engine/portal will know tenant context; avoids extra DB lookups and ambiguity |
| Always require `site_code` | IDs below site level are NOT globally unique |
| Delete CC payment/refund routes | SiteLink has no payment processor; Stripe handles payments externally |
| Use `PaymentSimpleCheckWithSource` for Stripe reconciliation | `sCheckNumber` field carries Stripe payment intent ID |
| Read discounts from DB, not SOAP | `DiscountPlansRetrieve` returns empty; `ccws_discount` pipeline is reliable |
| Downgrade insurance endpoint from V3 to V2 | V3 returns 0 on LSETUP; V2 returns all plans |
| `iCreditCardType=5` for reservation fee | Only value that passes validation without a payment processor |
| Shared `_billing_soap_call` helper | Structurally prevents audit-before-success bug class |
| MoveInCostRetrieve is mandatory for final amount | Amount match is strict to the cent; internal calc for estimates only until fully validated |
| Support both billing modes | LSETUP=1st-of-month (proration); other sites=anniversary (no proration). Both paths needed |

---

## Test Artifacts

- `backend/python/test_lsetup_booking_flow.py` — full flow test (tenant → reserve → fee → cost → move-in)
- `backend/python/test_lsetup_fee_retry.py` — CC type sweep for ReservationFeeAdd
- `backend/python/test_lsetup_round2.py` — insurance/discount debug + fee retry
- `backend/python/test_lsetup_round3.py` — cost comparison with discount/insurance combos
- `backend/python/test_lsetup_cost_validation.py` — 60-scenario internal calc vs SOAP validation (48.3% pass, 2 fixable patterns)
