# SMD SOAP Middleware — Discovery Report

**Date:** 2026-04-16
**Scope:** Full audit of SOAP integration layer, booking engine flow validation, and internal cost calculator reverse-engineering
**Test environment:** LSETUP (bTestMode=true unless noted)

---

## 1. billing.py SOAP Audit & Remediation

### Root cause

SMD's CallCenterWs silently accepts SOAP requests with wrong or misspelled parameter names and returns `Ret_Code=-1` inside a normal HTTP 200 OK response. Routes that lack a `Ret_Code` guard treat this as success, so the caller sees "OK" while SMD performed no operation. This is how the ECRI bug (`ScheduleRentIncrease` — a non-existent operation) survived in production undetected.

### Cross-cutting bugs (all write routes)

| Bug | Impact |
|-----|--------|
| `audit_log` fires unconditionally before success check | Phantom audit records for operations that never posted to SiteLink — primary financial integrity risk |
| Routes return HTTP 200 with `status:error` on SMD failure | Should return HTTP 502; callers cannot distinguish success from failure by status code |
| Param name drift from WSDL | Case-sensitive mismatches (e.g. `iLedgerID` vs `ledgerId`) cause silent failures |

### Classification of 25 broken call sites

| Category | Count | Examples | Action |
|----------|-------|----------|--------|
| Tenant Portal (view) | 10 | LedgersByTenantID_v3, ChargesAllByLedgerID, PaymentsByLedgerID | Fix param names |
| Tenant Portal (action) | 5 | PaymentSimpleCheckWithSource, ScheduleMoveOut, InsuranceCoverageAddToLedger | Fix params + entity model |
| Admin | 10 | ChargeAddToLedger, ApplyCredit, RefundPaymentCash, LedgerTransferToNewTenant | Fix params + entity model |
| Delete (CC routes) | 2 | PaymentSimpleWithSource_v3, RefundPaymentCreditCard_v2 | Remove — SiteLink has no CC processor |

Only 1 of 26 total call sites (`InsuranceLedgerStatusByLedgerID`) matched the WSDL correctly.

### Entity model decision

Match the WSDL exactly. `site_code` is always required because IDs below site level are not globally unique. No server-side ID resolution — callers pass the full chain.

- **Payment path:** site_code → tenant_id → ledger_id → charge_id
- **Inventory path:** site_code → unit_id → tenant_id

### Architecture decision

Extract a shared `_billing_soap_call(site_code, operation, params, audit_event, audit_detail)` helper that: (1) calls the SOAP operation, (2) parses `Ret_Code`, (3) returns HTTP 502 on failure, (4) fires `audit_log` only on success. This **structurally prevents** the audit-before-success bug class. Each route handler reduces to ~15 lines.

### PR plan

| PR | Contents | Risk |
|----|----------|------|
| PR1 | Shared helper + audit-before-success fix on all write routes + 5 Category F renames + ScheduleMoveOut date fix + delete 2 CC routes | Low |
| PR2 | Category B/D non-contract fixes (ChargeAddToLedger, LedgerTransferToNewTenant) + contract-change routes (entity model corrections) | Medium |
| PR3 | Category A payment/refund entity rewrite (6 routes) + Category C CustomerAccounts entity remodel (3 routes) | Medium |

### Acceptance criteria (per route)

- SOAP operation and param names match WSDL `<s:element>` / `<s:sequence>` exactly (case-sensitive)
- DateTime params use `%Y-%m-%dT%H:%M:%S` format, not bare date
- All `minOccurs="1"` params present
- `Ret_Code <= 0` → log `Ret_Msg` to `logger.error`, return HTTP 502 with generic message
- `audit_log` called on success path only (after `Ret_Code > 0`)
- No `str(e)` leaked to response body
- Non-destructive SOAP verification on reads (L025), LSETUP for writes

### What's already fixed

| File | Deploy date | Status |
|------|-------------|--------|
| `ecri.py` (batch push) | 2026-04-15 | Deployed, verified on Ledger 565680 |
| `tenants.py` (4 bugs) | 2026-04-15 | Deployed, post-deploy verification pending |
| `reservations.py` | N/A | All 11 call sites confirmed OK against WSDL |
| `soap_reports.py` | N/A | Confirmed OK — datetime format and params already correct |

---

## 2. Booking Engine SOAP Integration

All findings validated against LSETUP test site using `bTestMode=true` unless noted.

### 2.1 Confirmed Booking Flows

**Flow 1 — Direct move-in:**
Select unit + discount → `MoveInCostRetrieveWithDiscount_v4` → `TenantNewDetailed_v3` → `MoveInWithDiscount_v7` (`iPayMethod=2` cash bypass)

**Flow 2 — Paid reservation:**
Select unit + discount → `TenantNewDetailed_v3` → `ReservationNewWithSource_v6` (carries `ConcessionID`) → Stripe checkout → `ReservationFeeAddWithSource_v2` (`iCreditCardType=5`, dummy CC) → `MoveInCostRetrieveWithDiscount_Reservation_v4` → `MoveInReservation_v6` (`iPayMethod=2` cash bypass)

### 2.2 Key Test Results

| Test | Result | Finding |
|------|--------|---------|
| Reservation without prior MoveInCostRetrieve | OK (WaitingID returned) | Reservation does not require cost upfront |
| ReservationFeeRetrieve | dcPrice=$100, bReqReservationFee=true | Fee amount is site-configured |
| ReservationFeeAdd iCreditCardType 0–4 | All fail: "Error getting Credit Card Type ID" | Types 0–4 not mapped on LSETUP |
| **ReservationFeeAdd iCreditCardType=5** | **Ret_Code=1 (success, bTestMode=true)** | Only CC type that works; SiteLink has no processor so dummy CC passes |
| MoveIn wrong amount ($99.99) | Ret_Code=-11 "does not match required payment of $139.81" | Amount match is strict to the cent |
| MoveIn correct amount ($139.81) | Ret_Code=592852 (LedgerID) | Cash bypass (iPayMethod=2) works |
| MoveInCost + 5% discount (CID 4661) | $143.94 (saved $1.91) | Discount applies to rent only, not admin/deposit |
| MoveInCost + insurance (CoverageID 9649, $3/mo) | $147.36 (added $1.51 prorated) | Insurance prorated to partial month |
| MoveInCost + discount + insurance | $145.45 | Combinations work correctly |

### 2.3 Insurance Endpoint

`InsuranceCoverageRetrieve_V3` returns 0 results on LSETUP. V1 and V2 both return 11 plans (AON Singapore, $3–$40/mo). The existing route in `reservations.py` uses V3 and must be downgraded to V2. Field mapping: SOAP returns `InsurCoverageID`, which maps to `InsuranceCoverageID` in move-in and cost-retrieve operations.

### 2.4 Discount Plans

`DiscountPlansRetrieve` SOAP returns empty (0 plans) despite 15 active plans existing in SiteLink. Tested with multiple result tags (`Table`, `RT`, `NewDataSet`) and `DiscountPlansRetrieveIncludingDisabled` — all empty. The `ccws_discount` DB table (synced via the `cc_discount_plans_to_sql.py` pipeline) has full plan metadata. Booking engine reads discounts from this table; `ConcessionID` is passed to reservation and move-in operations. No new SOAP endpoint needed.

### 2.5 Reservation Fee

`ReservationFeeAddWithSource_v2` works only with `iCreditCardType=5`. Types 0–4 (and 6–10) all fail with "Error getting Credit Card Type ID". The dummy CC pattern that passes:

```
sCreditCardNumber:  4111111111111111  (standard test Visa)
sCreditCardCVV:     123
dExpirationDate:    2030-01-01T00:00:00  (must not be empty — empty dates crash SMD)
sBillingName/Address/Zip: tenant details
```

Fee amount is site-configured ($100 on LSETUP). Still needs `bTestMode=false` validation before production use.

### 2.6 Missing API Routes

These SOAP operations are required by the booking flows but not yet wired as REST API routes:

| Operation | Purpose |
|-----------|---------|
| `UnitsInformationAvailableUnitsOnly_v2` | Browse available units |
| `UnitTypePriceList_v2` | Unit type pricing |
| `ReservationFeeAddWithSource_v2` | Record paid reservation fee |

`MoveInCostRetrieveWithDiscount_v4` and `MoveInCostRetrieveWithDiscount_Reservation_v4` are already wired in `reservations.py`.

### 2.7 Stripe Payment Recording at Move-In

Move-in operations (`MoveInWithDiscount_v7`, `MoveInReservation_v6`) have no memo or reference field for a Stripe payment intent ID. `PaymentSimpleCheckWithSource` has `sCheckNumber` for Stripe PI IDs, but it is a post-move-in billing operation, not usable during move-in. For move-in transactions, the Stripe reference must be recorded via `TenantNoteInsert_v2` on the tenant record after the move-in completes.

---

## 3. MoveInCost Internal Calculator

### 3.1 Reverse-engineered formula (confirmed on LSETUP)

All calculations use `Decimal` with `ROUND_HALF_UP` rounding.

| Component | Formula |
|-----------|---------|
| **Proration** | `rate * remaining_days / days_in_month` where `remaining_days = days_in_month - move_in_day + 1` (inclusive of move-in day) |
| **Tax** | `round(charge_amount * tax_rate, 2)` — tax rate is per charge type, not per site |
| **Discount** | Applied to prorated rent before tax: `discount = prorated_rent * pcDiscount / 100`, tax computed on `prorated_rent - discount` |
| **Admin fee** | Flat amount (from `ChargeDescriptionsRetrieve`), taxed at its own charge-type rate (9% on LSETUP) |
| **Security deposit** | Flat amount, 0% tax |
| **Insurance** | Prorated identically to rent, taxed at 8% (charge-type-specific, distinct from the unit's 9% GST) |

### 3.2 Validation results

60 scenarios tested on LSETUP (2 units x 6 dates x 5 configurations each). **29 passed (48.3%).**

| Pattern | Cause | Affected scenarios | Fix |
|---------|-------|--------------------|-----|
| Insurance tax rate mismatch | Calculator used unit's 9% GST; actual charge-type rate is 8% | All scenarios with insurance (24/60) | Use per-charge-type tax rate from `ChargeDescriptionsRetrieve` |
| Missing second month charge | Late move-in (day >= `iDayStrtProratePlusNext`) triggers prorated first month + full second month | Late-month dates (7/60) | Detect threshold, emit additional "Second Monthly Rent Fee" line |

All non-insurance, non-late-month scenarios matched SOAP to the cent: rent proration, 5%/10% discounts, admin fee, security deposit, and 0%-tax units.

### 3.3 ChargeDescriptionsRetrieve — the missing puzzle piece

This single SOAP call returns all charge types per site with individual tax rates and default prices, resolving the insurance tax gap and admin fee gap simultaneously.

| Charge type | Category | Tax1 | Tax2 | Default price | Notes |
|-------------|----------|------|------|---------------|-------|
| Rent | Rent | 9% | 0% | per unit | — |
| Administrative Fee | AdminFee | 9% | 0% | $30 | Varies by site ($0–$300) |
| Insurance | Insurance | 8% | 0% | $3/mo | Charge-type rate, not site GST |
| Security Deposit | SecDep | 0% | 0% | per unit | — |
| POS/Merchandise | POS | 7% | 0% | varies | Goods at different rate |
| Reservation Fee | ReservFee | 0% | 0% | $100 | — |

Must be synced to a new `ccws_charge_descriptions` table.

### 3.4 Billing modes (both must be supported)

| Mode | Flag | Proration | Second month | Current sites |
|------|------|-----------|--------------|---------------|
| 1st-of-month | `bAnnivDateLeasing=false` | `rate * remaining / days_in_month` | Yes, if `day >= iDayStrtProratePlusNext` | LSETUP only |
| Anniversary | `bAnnivDateLeasing=true` | None (full month from move-in) | No | All other ESA sites |

### 3.5 Site config variance (28-site audit)

| Parameter | SG (9%) | KR (10%) | MY (6%) | HK (0%) |
|-----------|---------|----------|---------|---------|
| Tax rate | 9% | 10% | 6% | 0% |
| Tax consistency per site | 1 rate | 1 rate | 1 rate | 1 rate |
| Admin fee range | $30 | $30–$50 | $20–$50 | $0–$300 |
| Billing mode | Anniversary | Anniversary | Anniversary | Anniversary |

Tax rates are consistent within each site (verified across all 28 sites). Only LSETUP has mixed rates (test artifact).

### 3.6 Data gaps and recommended new pipelines

| Pipeline | SOAP source | Solves |
|----------|-------------|--------|
| `ccws_charge_descriptions` | `ChargeDescriptionsRetrieve` per site | Admin fee amount, insurance premium, per-charge-type tax rates |
| `site_billing_config` | One-time `MoveInCostRetrieve` per site (extract config fields) | Proration flags, billing mode, second-month threshold |
| `ccws_insurance_coverage` | `InsuranceCoverageRetrieve` (V2) per site | Coverage amounts, premiums, provider info |

### 3.7 Recommended approach

Use SOAP `MoveInCostRetrieveWithDiscount_v4` for the exact amount at checkout (amount match is strict to the cent — a $0.01 mismatch causes move-in rejection). Use the internal calculator for display-only estimates during unit browsing. Run both paths in parallel during a shadow validation period, logging mismatches, before relying on the internal calculator for any binding amounts.

---

## 4. Middleware Action Items

### A. New API Routes Needed

| Route | Purpose | Priority | Effort |
|-------|---------|----------|--------|
| `UnitsInformationAvailableUnitsOnly_v2` | Unit browsing for booking engine | P1 | S |
| `UnitTypePriceList_v2` | Pricing display | P1 | S |
| `DiscountPlans` REST endpoint | Serve discounts from `ccws_discount` DB table | P1 | S |
| `ReservationFeeAddWithSource_v2` | Record paid reservation fee (Stripe flow) | P1 | M |
| `ChargeDescriptionsRetrieve` | Charge type config incl. tax rates | P1 | S |
| `RentTaxRatesRetrieve` | Site-level tax config | P1 | S |

### B. New Data Pipelines Needed

| Pipeline | Purpose | Priority | Effort |
|----------|---------|----------|--------|
| `ccws_charge_descriptions` | Sync ChargeDescriptionsRetrieve per site | P1 | M |
| `site_billing_config` | Sync proration/billing mode flags | P1 | M |
| `ccws_insurance_coverage` | Sync insurance plans with premiums | P1 | M |

### C. Existing Route Fixes — billing.py Remediation

| PR | Scope | Priority | Effort |
|----|-------|----------|--------|
| **PR1** | Shared helper + Cat F renames + audit fix + delete 2 CC routes | P0 | M |
| **PR2** | Entity model corrections (tenant_id instead of ledger_id) | P1 | M |
| **PR3** | Payment/refund entity rewrite (6 routes) | P2 | L |

### D. Existing Route Fixes — Other

| Fix | File | Priority | Effort |
|-----|------|----------|--------|
| Downgrade InsuranceCoverageRetrieve from V3 to V2 | `reservations.py` | P0 | S |
| Register `billing:read` / `billing:write` scopes | `api_key.py` | P2 | S |

### E. LSETUP Validation Still Needed

All P0 (blockers before production launch):

- ReservationFeeAddWithSource_v2 with `bTestMode=false` — confirm fee posts to ledger
- Full move-in flow end-to-end (reserve → fee → cost → move-in) with `bTestMode=false`
- MoveInCost calculator validation against a real anniversary-billing site
- Insurance tax rate validation on a real production site

### F. Future / Deferred (P2)

- Stripe webhook handler and payment intent creation
- Booking engine frontend (Jinja2 templates)
- `CancelInsurancePolicy`, `InsuranceRateUpdate` endpoints
- `SendPaymentConfirmationEmail`, `SendReservationConfirmationEmail`
- `DiscountPlansRetrieve` SOAP debugging (returns empty; DB-first approach works, low urgency)

---

## Test Artifacts

| File | Purpose |
|------|---------|
| `backend/python/test_lsetup_booking_flow.py` | Full flow test (tenant → reserve → fee → cost → move-in) |
| `backend/python/test_lsetup_fee_retry.py` | CC type sweep for ReservationFeeAdd |
| `backend/python/test_lsetup_round2.py` | Insurance/discount debug + fee retry |
| `backend/python/test_lsetup_round3.py` | Cost comparison with discount/insurance combos |
| `backend/python/test_lsetup_cost_validation.py` | 60-scenario internal calc vs SOAP validation |

---

## Reference: SOAP Endpoints by Status

### Confirmed OK (no changes needed)
`reservations.py`: TenantNewDetailed_v3, ReservationNewWithSource_v6, ReservationUpdate_v4, ReservationList_v3, ReservationNotesRetrieve, ReservationNoteInsert, ReservationFeeRetrieve, MoveInReservation_v6, MoveInWithDiscount_v7, InsuranceCoverageRetrieve_V3 (works but V2 preferred), InsuranceCoverageMinimumsRetrieve

`soap_reports.py`: All ReportingWs operations confirmed OK.

`tenants.py`: TenantInfoByTenantID, TenantIDByUnitNameOrAccessCode, TenantListDetailed_v3, TenantUpdate_v3 confirmed OK. 4 bugs fixed and deployed 2026-04-15.

### Broken (25 sites in billing.py)
See Section 1 classification table.

### Not Yet Wired
UnitsInformationAvailableUnitsOnly_v2, UnitTypePriceList_v2, ReservationFeeAddWithSource_v2, ChargeDescriptionsRetrieve (as API route), RentTaxRatesRetrieve (as API route), DiscountPlansRetrieve (use DB instead), CancelInsurancePolicy, InsuranceRateUpdate, SendPaymentConfirmationEmail, SendReservationConfirmationEmail
