# SOAP Middleware & Booking Engine — Session Handoff

**Generated:** 2026-04-17
**Purpose:** Complete handoff document for architecture-design session continuation
**Repository:** `/home/louis/PycharmProjects/WebApp_BackendESA`
**Latest deployed commit:** `f06c215` (manifest `f06c2159e0`)

---

## 1. Mission & Outcome

### Original problem
A full audit of `backend/python/web/routes/` against the live SMD WSDLs revealed:
- **25 of 26** SOAP call sites in `billing.py` were broken (param name drift, entity-model mismatches)
- **Audit-before-success bug**: every write route called `audit_log(...)` before checking `Ret_Code`, creating phantom audit records when SMD silently rejected the call (`Ret_Code=-1` in HTTP 200)
- **Booking engine gaps**: no API routes for unit browsing, discount selection, or paid reservations

### What was delivered (5 deployed commits this session)
1. `b4e38eb` — billing.py SOAP remediation (25 routes fixed) + 3 new booking-engine API routes + 2 new sync pipelines + MoveInCost calculator
2. `4bae1e4` — Pipeline configs registered in `pipelines.yaml`
3. `10f4825` — Dropped `/api/reservations/fee/add` (dead path; SOAP requires real payment processor) + 78-scenario validation matrix
4. `9b6b9cd` — Per-site billing config table + admin UI + calculator rounding fixes (Feb-15 mystery solved)
5. `f06c215` — Renamed `cc_*` → `ccws_*` for SOAP-sourced tables/files

---

## 2. Critical SOAP Behavior Discoveries (LSETUP-validated)

### 2.1 Silent failures
SMD's CallCenterWs accepts unknown param names without complaint. Returns `Ret_Code=-1` inside an HTTP 200 OK envelope. Without an explicit guard, code thinks it succeeded.

**Fix pattern:** check `Ret_Code > 0` (or `WaitingID > 0` for reservation creates) BEFORE calling `audit_log`. Failure path logs the error and returns an error response without writing an audit record. Helpers: `_billing_soap_call` in `billing.py` and `_reservation_soap_call` in `reservations.py` — all write routes route through these.

### 2.2 Reservation fee endpoint is a dead path
- `ReservationFeeAddWithSource_v2` works in `bTestMode=true` with dummy CC (`iCreditCardType=5`)
- In `bTestMode=false` (real mode): `Ret_Code=-100 "No credit card processor found"`
- LSETUP has no payment processor configured. Production sites likely don't either.
- **For Stripe-based booking**: skip this entirely. Flow is reserve (free) → Stripe (external) → MoveInReservation_v6 with `iPayMethod=2` (cash bypass).

### 2.3 Insurance endpoint version
- `InsuranceCoverageRetrieve_V3` returns 0 results on LSETUP
- `InsuranceCoverageRetrieve_V2` returns 11 plans (AON Singapore, $3-$40/mo)
- **Use V2** — already corrected in `reservations.py`

### 2.4 DiscountPlansRetrieve returns empty via SOAP
- `DiscountPlansRetrieve` and `DiscountPlansRetrieveIncludingDisabled` both return zero rows
- DB sync pipeline (`ccws_discount_plans_to_sql.py`) DOES return all plans correctly
- **Booking engine reads discounts from `ccws_discount` table**, not from real-time SOAP

### 2.5 Move-in operations have no payment reference field
- `MoveInWithDiscount_v7` and `MoveInReservation_v6` accept `iPayMethod=2` (cash bypass) but have **no memo/check-number/reference field**
- For Stripe payment ID tracking: use `TenantNoteInsert_v2` after move-in completes, OR record a separate `PaymentSimpleCheckWithSource` with `sCheckNumber=stripe_pi_xxx`

### 2.6 Amount-match is strict to the cent
- Move-in `dcPaymentAmount` must exactly equal `MoveInCostRetrieve` total
- $0.01 mismatch → `Ret_Code=-11 "Payment amount does not match required payment of $X.XX"`
- Calculator can be used for **display estimates only**; final amount must come from SOAP at checkout

### 2.7 Two billing modes (per-site)
- **1st-of-month** (`bAnnivDateLeasing=false`) — only LSETUP currently
  - Day 1: full month rent
  - Day 2 to X: prorated current month
  - Day X+1 to EOM: prorated current + full second month
  - X = `iDayStrtProratePlusNext` (17 on LSETUP)
- **Anniversary** (`bAnnivDateLeasing=true`) — all 27 production sites
  - Full month from move-in date, no proration
  - X value returned (24) but unused
- Both modes are now supported by the calculator

### 2.8 SOAP tax rounding behavior
- Tax = `truncate(amount * rate)` — `ROUND_DOWN`
- `35.50 * 0.09 = 3.195` → SOAP returns **3.19** (NOT 3.20 HALF_UP)
- When discount is present: `tax = trunc(full*rate) - trunc(discount*rate)`, NOT `trunc(net*rate)`
  - SOAP reports `ChargeAmount=full`, `dcDiscount=disc_amount`, `TaxAmount=net_tax_after_subtraction`
- Some edge cases still produce $0.01 differences (SiteLink uses different tax paths for different scenarios — unable to fully reverse-engineer)

### 2.9 Per-charge-type tax rates (the missing puzzle piece)
`ChargeDescriptionsRetrieve` returns ALL site charge types with **individual tax rates**:
| Charge Category | LSETUP Tax Rate | Default Price | Notes |
|---|---|---|---|
| Rent | 9% (GST) | per unit | — |
| AdminFee | 9% | $30 | varies by site ($0–$300) |
| Insurance | **8%** (NOT 9%) | $3/mo | distinct from rent tax |
| SecDep | 0% | per unit | no tax |
| POS | 7% | varies | merchandise |
| ReservFee | 0% | $100 | — |

This is why insurance lines fail with the unit's tax rate — must use the charge-type-specific rate.

**Per-site scoping:** `ccws_charge_descriptions` stores one row per `(ChargeDescID, SiteID)` pair — rates shown above are LSETUP. Korean sites (KR GST 10%), Malaysia (SST 6%), HK (0%) have their own per-site rows. Always query by `SiteCode` / `SiteID`.

### 2.10 Site tax rates (one rate per site)
| Country | Tax Rate | Sites |
|---|---|---|
| Singapore | 9% (GST) | L001-L005, L008, L017, L018, L022, L025, L028-L030 |
| Korea | 10% | L006, L011, L013, L019, L021, L023, L024, L031 |
| Malaysia | 6% | L007, L009, L010, L026 |
| Hong Kong | 0% | L015, L020 |
| Test | mixed (artifact) | LSETUP |

Source: `RentTaxRatesRetrieve` SOAP + per-site `units_info` audit.

### 2.11 IDs are site-scoped, NOT globally unique
Critical for the entity model:
- Payment path: `site_code → tenant_id → ledger_id → charge_id`
- Inventory path: `site_code → unit_id → tenant_id`
- Same `ledger_id=500` exists at multiple sites — every operation MUST include `site_code`

---

## 3. Current Architecture

### 3.1 File map (this work)

```
backend/python/
  common/
    movein_cost_calculator.py          # NEW — internal cost calculator
    models.py                          # +3 new models (see 3.4)
  datalayer/                           # All renamed to ccws_ prefix
    ccws_charge_descriptions_to_sql.py # NEW — sync charge type config
    ccws_insurance_coverage_to_sql.py  # NEW — sync insurance plans
    ccws_site_billing_config_to_sql.py # NEW — sync per-site proration
    ccws_discount_plans_to_sql.py      # RENAMED from cc_*
  migrations/
    052_charge_descriptions_and_insurance.sql  # NEW
    053_ccws_site_billing_config.sql           # NEW (idempotent rename)
  web/
    routes/
      billing.py        # 25 SOAP routes fixed + new tax-rates route
      reservations.py   # InsuranceCoverageRetrieve V3→V2 + discount-plans route
      units.py          # NEW blueprint — available units + price list
      admin.py          # +3 routes for site billing config UI
      app.py            # registers units_bp
    templates/
      admin/site_billing_config/
        list.html       # NEW
        edit.html       # NEW
      base.html         # +1 sub-nav link "Billing Config"
  config/
    pipelines.yaml      # 3 new ccws_* pipeline registrations
    scheduler.yaml      # location_codes references
  test_lsetup_*.py      # 6 LSETUP validation scripts (regression suite)
docs/
  tools/
    smd_soap_audit_fix_plan.md          # original audit
    smd_soap_discovery_report.md        # consolidated discovery
    soap_middleware_session_handoff.md  # THIS DOCUMENT
  superpowers/
    specs/2026-04-16-soap-audit-fix-and-booking-engine-design.md
    plans/2026-04-16-soap-middleware-remediation.md
```

### 3.2 New API routes (booking engine surface area)

| Method | Path | Operation | Auth Scope |
|--------|------|-----------|------------|
| GET | `/api/units/available` | UnitsInformationAvailableUnitsOnly_v2 | reservations:read |
| GET | `/api/units/price-list` | UnitTypePriceList_v2 | reservations:read |
| GET | `/api/billing/tax-rates` | RentTaxRatesRetrieve | billing:read |
| GET | `/api/reservations/discount-plans` | (DB-backed, no SOAP) | reservations:read |
| GET | `/api/reservations/insurance-coverage` | InsuranceCoverageRetrieve_V2 (downgraded from V3) | reservations:read |

Already-existing routes used by booking engine:
- `POST /api/reservations/reserve` → `TenantNewDetailed_v3` + `ReservationNewWithSource_v6`
- `GET /api/reservations/move-in/cost` → `MoveInCostRetrieveWithDiscount_*` family
- `POST /api/reservations/move-in` → `MoveInWithDiscount_v7` / `MoveInReservation_v6`

### 3.3 Admin UI

`/admin/site-billing-config` (under existing admin Settings nav)
- **List view**: per-site billing mode, X threshold, last sync time, override status
- **Edit form**: toggle anniversary mode, set proration days, add notes
- **Clear Override button**: lets sync pipeline resume on next run
- All edits audit-logged via `AuditEvent.CONFIG_UPDATED`

### 3.4 New SQLAlchemy models (in `common/models.py`)

```python
class CcwsChargeDescription(Base, BaseModel, TimestampMixin):
    __tablename__ = 'ccws_charge_descriptions'
    # Per-site charge type config: tax rates, default prices, move-in flags
    # Surrogate PK (id); unique index on (ChargeDescID, SiteID)

class CcwsInsuranceCoverage(Base, BaseModel, TimestampMixin):
    __tablename__ = 'ccws_insurance_coverage'
    # Per-site insurance plans from V2 endpoint
    # Surrogate PK (id); unique index on (InsurCoverageID, SiteID)

class CcwsSiteBillingConfig(Base, BaseModel, TimestampMixin):
    __tablename__ = 'ccws_site_billing_config'
    # Per-site proration / billing-mode flags + override audit
    # Surrogate PK (id); SiteCode unique=True

class StripeWebhookEvent(Base, BaseModel):
    __tablename__ = 'stripe_webhook_events'
    # Idempotency tracking for Stripe webhook deliveries
    # event_id unique; status ∈ {received, processed, failed}
    # See migration 054 + stripe_payments.py guard
```

### 3.5 New data pipelines (registered in scheduler)

| Pipeline | SOAP Source | Schedule | Target Table |
|----------|-------------|----------|--------------|
| ccws_charge_descriptions | ChargeDescriptionsRetrieve | Sun 05:30 UTC | ccws_charge_descriptions |
| ccws_insurance_coverage | InsuranceCoverageRetrieve_V2 | Sun 05:45 UTC | ccws_insurance_coverage |
| ccws_site_billing_config | MoveInCostRetrieveWithDiscount_v4 (one call per site) | weekly (manual) | ccws_site_billing_config |
| ccws_discount_plans | DiscountPlansRetrieveIncludingDisabled | Sun 05:00 UTC | ccws_discount |

All pipelines: `cron: 0 5 * * 0` family, `priority: 8`, `resource_group: soap_api`.

### 3.6 Live data state (PBI)

| Table | Rows | Source | Notes |
|-------|------|--------|-------|
| ccws_discount | ~1700 | per-site sync | 15+ active plans on LSETUP, 7+ on each prod site |
| ccws_charge_descriptions | 2781 | per-site sync (28 sites) | LSETUP=101 charge types |
| ccws_insurance_coverage | 203 | 19 sites have insurance | 9 sites returned 0 (KR, HK regulatory) |
| ccws_site_billing_config | 28 | one row per site | LSETUP=1st-of-month/X=17, 27 prod=anniversary |

---

## 4. MoveInCost Internal Calculator

### 4.1 Module: `common/movein_cost_calculator.py`

**Public API:**
```python
@dataclass
class ChargeTypeTax:
    category: str
    tax1_rate: Decimal     # 9.0 = 9%
    tax2_rate: Decimal
    default_price: Decimal

@dataclass
class CostLine:
    description: str
    charge_amount: Decimal
    discount: Decimal
    tax1: Decimal
    tax2: Decimal
    total: Decimal

def calculate_movein_cost(
    std_rate, security_deposit, admin_fee, move_in_date,
    rent_tax: ChargeTypeTax,
    admin_tax=None, deposit_tax=None, insurance_tax=None,
    pc_discount=0, fixed_discount=0,
    insurance_premium=0,
    anniversary_billing=False,
    day_start_prorate_plus_next=17,
) -> List[CostLine]: ...

def estimate_total(charges) -> Decimal: ...

def load_site_billing_config(site_code: str) -> dict:
    """Reads from ccws_site_billing_config — returns proration flags."""
```

### 4.2 Formula (validated against SOAP)

**Proration:** `rate * (days_in_month - day + 1) / days_in_month`
- Day 1 → factor = 1.0 (full month)
- Late day (X+1 to EOM) → also adds full second month line

**Tax:** `truncate(amount * rate, 2)` (ROUND_DOWN)
- With discount: `tax = trunc(full*rate) - trunc(discount*rate)` (matches SOAP's two-step ledger reporting)

**Discount on rent only** — doesn't apply to admin fee, deposit, or insurance.
- Late move-in 2nd month: discount NOT applied (matches SiteLink behavior)

### 4.3 Validation results (LSETUP, 78 scenarios)

| Result | Count | Notes |
|--------|-------|-------|
| PASS (exact match) | 61 | Real-world cases |
| FAIL ($0.01 diff) | ~5 | Rounding edge cases — SiteLink uses inconsistent rounding paths |
| FAIL (>$1 diff) | ~12 | "Special" concessions: prepaid free-month plans, referral fees, late move-in + discount combos |

**Pass rate: 78%.** Remaining failures are either:
1. Edge-case rounding ($0.01) — unavoidable without reverse-engineering more
2. Plans with multi-month math (free-month, prepaid) — fundamentally different
3. Special-purpose discounts (referral fees, vouchers) — not simple coupons

**Production guidance** (encoded in docstring):
- Recurring % discounts → trustworthy
- Simple fixed coupons → trustworthy
- Free-month / prepaid plans → fall back to SOAP
- Late move-in + discount (where `move_in_day > iDayStrtProratePlusNext` from `ccws_site_billing_config`) → fall back to SOAP
- **Final binding amount → ALWAYS SOAP** (amount-match strict to cent)

**Fallback contract:** `calculate_movein_cost()` now returns `Tuple[List[CostLine], Optional[str]]`. The second element is a reason code when confidence is low, else `None`. Reason codes:
- `free_month_plan` — `pc_discount >= 100%`
- `prepaid_multi_month` — `fixed_discount >= std_rate`
- `late_move_in_with_discount` — late move-in day + any discount present
- `unknown_discount_structure` — >50% discount not matching above

Callers should treat any non-`None` reason as "must re-query SOAP for binding total".

---

## 5. Booking Engine Architecture (recommended)

### 5.1 Two confirmed flows

**Flow A — Direct move-in (walk-in / immediate):**
1. `GET /api/units/available?site_code=X` — browse
2. `GET /api/units/price-list?site_code=X` — show pricing
3. `GET /api/reservations/discount-plans?site_code=X` — show discounts (DB-backed)
4. `GET /api/reservations/insurance?site_code=X` — show insurance options
5. (optional) Calculator quote for instant display
6. `GET /api/reservations/move-in/cost` — exact total via SOAP
7. `POST /api/reservations/reserve` — creates tenant + reservation
8. `POST /api/reservations/move-in` — `MoveInWithDiscount_v7` with `iPayMethod=2`

**Flow B — Paid reservation (online booking with Stripe):**
1-6. Same as above (browse, quote, get exact cost)
7. `POST /api/reservations/reserve` — blocks unit (free)
8. **Stripe checkout** — external payment processing (PI metadata carries `reservation_id`, `site_code`)
9. **Stripe webhook** (`payment_intent.succeeded`) — signature-verified via `stripe.Webhook.construct_event`; dedup-guarded via `stripe_webhook_events` table (UNIQUE on `event_id`). On first delivery: call `MoveInReservation_v6` with `iPayMethod=2`; commit `status='processed'`. On duplicate: return 200 immediately. On SOAP failure: mark `status='failed'` and return 500 for Stripe retry (only safe if SOAP call confirmed not applied).
10. (Optional, after move-in) `PaymentSimpleCheckWithSource` with `sCheckNumber=stripe_pi_xxx` for ledger-level audit trail.

Idempotency implementation: migration `054_stripe_webhook_events.sql` + `StripeWebhookEvent` model + guard block in `web/routes/stripe_payments.py` (integration seam at the `# TODO (Flow B)` block around line 219).

### 5.2 ID hierarchy (always pass site_code)

```
SiteCode (string, e.g. "L001")
   │
   ├── TenantID (per-site)
   │     ├── LedgerID (per-tenant)
   │     │     └── ChargeID (per-ledger)
   │     │
   │     └── UnitID (rented unit)
   │
   └── UnitID (per-site, also reachable via TenantID)
```

### 5.3 Auth scopes (current)

| Scope | Routes |
|-------|--------|
| `reservations:read` | units, discount plans, insurance, move-in cost |
| `reservations:write` | reserve, move-in |
| `billing:read` | tax rates, charge types, ledger views |
| `billing:write` | charges, payments, refunds |

**Note:** `billing:read` and `billing:write` are now registered in `API_SCOPES` (`web/models/api_key.py`) and are grantable through the admin UI scope picker.

---

## 6. SOAP Endpoint Inventory

### 6.1 Production-ready (used in current code)

**Tenants:**
- TenantNewDetailed_v3 — create
- TenantUpdate_v3 — update
- TenantInfoByTenantID — get one
- TenantListDetailed_v3 — list
- TenantSearchDetailed — search
- TenantIDByUnitNameOrAccessCode — lookup
- TenantNotesRetrieve / TenantNoteInsert_v2

**Reservations:**
- ReservationNewWithSource_v6 — create
- ReservationUpdate_v4 — update/cancel
- ReservationList_v3 — list
- ReservationFeeRetrieve — fee config (read-only)
- ReservationNotesRetrieve / ReservationNoteInsert

**Move-in/out:**
- MoveInReservation_v6 — convert reservation to lease
- MoveInWithDiscount_v7 — direct move-in (no reservation)
- MoveInCostRetrieveWithDiscount_v4 — cost for direct
- MoveInCostRetrieveWithDiscount_Reservation_v4 — cost for reservation
- MoveInCostRetrieveWithDiscount_28DayBilling_v3 — 28-day cycle variant
- MoveInCostRetrieveWithPushRate_v2 — push rate variant
- ScheduleMoveOut

**Units:**
- UnitsInformationAvailableUnitsOnly_v2 — browse
- UnitTypePriceList_v2 — pricing

**Insurance:**
- **InsuranceCoverageRetrieve_V2** (NOT V3 — V3 returns 0)
- InsuranceCoverageMinimumsRetrieve
- InsuranceCoverageAddToLedger
- InsuranceLedgerStatusByLedgerID

**Charges/Billing:**
- ChargeDescriptionsRetrieve — charge type config
- ChargeAddToLedger / RecurringChargeAddToLedger_v1
- CustomerAccountsMakeFutureCharges
- CustomerAccountsBalanceDetails_v2
- CustomerAccountsBalanceDetailsWithDiscount
- ChargesAllByLedgerID / ChargesAndPaymentsByLedgerID
- LedgerStatementByLedgerID
- LedgersByTenantID_v3
- ApplyCredit
- LedgerTransferToNewTenant
- RemoveDiscountFromLedger
- TenantInvoicesByTenantID

**Payments:**
- PaymentSimpleCashWithSource
- PaymentSimpleCheckWithSource ← **Stripe→SiteLink bridge** (sCheckNumber = Stripe PI ID)
- PaymentSimpleBankTransferWithSource
- PaymentMultipleWithSource_v3 (entity model unverified)
- PaymentsByLedgerID

**Refunds:**
- RefundPaymentCash
- RefundPaymentCheck

**Site config:**
- RentTaxRatesRetrieve
- ChargeDescriptionsRetrieve

### 6.2 Deleted (don't use)
- `PaymentSimpleWithSource_v3` (single-payment **CC**) — SiteLink has no processor. NOTE: this is distinct from `PaymentMultipleWithSource_v3` in §6.1, which IS wired but has its entity model flagged as unverified.
- `RefundPaymentCreditCard_v2` (CC) — same reason
- `ReservationFeeAddWithSource_v2` (was wired, removed) — requires real processor

### 6.3 Discovered but not wired (future)
- CancelInsurancePolicy
- InsuranceRateUpdate
- SendPaymentConfirmationEmail
- SendReservationConfirmationEmail
- DiscountPlansRetrieve (returns empty — using DB instead)
- ReservationFeeAddWithSource_SCA_v1 (3DS variant — different flow)

---

## 7. Key Files for Architecture Design

If continuing in another session, the most informative files to read:

1. **`docs/tools/smd_soap_discovery_report.md`** — comprehensive findings doc
2. **`backend/python/common/movein_cost_calculator.py`** — calculator with full docstring
3. **`backend/python/web/routes/billing.py`** — see `_billing_soap_call` helper pattern (lines 30-90)
4. **`backend/python/web/routes/units.py`** — minimal blueprint pattern for new SOAP routes
5. **`backend/python/datalayer/ccws_site_billing_config_to_sql.py`** — pipeline pattern for per-site SOAP config sync
6. **`backend/python/test_lsetup_calc_matrix.py`** — regression test pattern (78 scenarios)
7. **`backend/python/test_lsetup_full_flow.py`** — end-to-end booking flow test

CLAUDE.md memory pointers worth surfacing:
- `feedback_reservation_fee_dead_path.md`
- `project_billing_modes.md`
- `feedback_smd_empty_dates.md` (date fields cannot be empty — crash SMD)

---

## 8. Open Architecture Questions

1. **Calculator vs SOAP at quote time** — current design: call SOAP for exact cost at checkout, calculator for browse-time estimates. Cache strategy not yet decided. Options:
   - Always SOAP (slow, accurate, no cache complexity)
   - Calculator with TTL cache (fast, ±$0.01, needs cache invalidation on rate changes)
   - Hybrid: calculator for "from $X" copy, SOAP for "your total: $X" at confirmation

2. **Stripe integration architecture** — webhook handler, payment intent creation, refund handling. Currently zero Stripe code. Need to design:
   - Where Stripe customer ID lives (new column on tenant? separate table?)
   - Webhook endpoint security (Stripe signature verification)
   - Idempotency for move-in retries on webhook redelivery
   - Refund flow: Stripe refund → SiteLink reversal

3. **Booking engine frontend** — currently no UI. Tech stack decision: vanilla Jinja2 + JS (matches rest of project) or separate SPA?

4. **Middleware split** — user mentioned wanting to split SOAP logic into a separate service. Current code is structured to make this easy:
   - All SOAP code lives in `web/routes/billing.py`, `web/routes/reservations.py`, `web/routes/units.py`, `common/soap_client.py`, `common/movein_cost_calculator.py`, `datalayer/ccws_*`
   - Lift these files → middleware service. Web app becomes consumer of REST API.

5. **Per-site config governance** — site_billing_config can be overridden in admin UI. Need policy:
   - Who can edit? (currently `config_required` permission)
   - Should overrides expire?
   - Audit notification (slack/email when override created)?

6. **MoveInCostRetrieve fall-back logic** — calculator handles ~78% of cases. Need a clear API contract for "this scenario can't be calculated internally, fall back to SOAP" — maybe return `None` from calculator with a reason code.

7. **Discount selection UI** — booking engine should hide concessions the calculator can't accurately model. Filter logic needs to live somewhere (probably in `discount_plans.py` route).

8. **Multi-currency** — Korea/Malaysia/HK have different currencies. Calculator returns Decimal but doesn't track currency. SiteLink uses local currency per site. Booking engine UI needs currency display logic.

---

## 9. Live Service Status (as of deploy)

```
Deploy manifest: f06c2159e0
VM: 20.6.132.108 (Azure)
PBI: esapbi.postgres.database.azure.com (Azure-hosted)
Domain: prod URL behind nginx

Services (all RUNNING):
- esa-backend.service (Flask/gunicorn 4 workers)
- backend-scheduler.service (APScheduler daemon)
- backend-mcp.service (MCP HTTP server)
```

**No outstanding deploys.** Local repo is in sync with origin.

---

## 10. Test Artifacts (regression suite)

Located in `backend/python/`:

| File | Purpose |
|------|---------|
| `test_lsetup_booking_flow.py` | First end-to-end SOAP test |
| `test_lsetup_fee_retry.py` | CC type sweep for ReservationFeeAdd |
| `test_lsetup_round2.py` | Insurance/discount/fee debug |
| `test_lsetup_round3.py` | Cost combinations exploration |
| `test_lsetup_cost_validation.py` | 60-scenario early validation |
| `test_lsetup_full_flow.py` | Final end-to-end booking flow |
| `test_lsetup_calc_matrix.py` | **78-scenario calculator vs SOAP matrix** (use this for regressions) |

All tests run against LSETUP (test site) and use `bTestMode=true` for destructive operations.

---

## 11. Quick-Start for Next Session

```bash
# Read the discovery report (most consolidated source of truth)
cat docs/tools/smd_soap_discovery_report.md

# Read this handoff
cat docs/tools/soap_middleware_session_handoff.md

# Verify everything still works
cd backend/python
python -c "
from web.app import create_app
from common.movein_cost_calculator import load_site_billing_config
app = create_app()
print('LSETUP config:', load_site_billing_config('LSETUP'))
"

# Run the calculator regression matrix
python test_lsetup_calc_matrix.py 2>&1 | tail -25

# Latest commits on master
git log --oneline -5
```

Latest commits:
- `f06c215` — refactor: rename cc_ to ccws_
- `9b6b9cd` — feat(billing-config): per-site proration + admin UI + calculator fixes
- `10f4825` — refactor(soap): drop fee/add + 78-scenario validation
- `4bae1e4` — chore(config): pipeline registrations
- `b4e38eb` — feat(soap): SOAP middleware remediation + booking engine integration
