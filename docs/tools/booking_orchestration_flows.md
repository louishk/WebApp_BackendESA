# Booking Engine — Orchestration Spec

**Source of truth for UX:** `booking_engine/jsx/booking-enginelatest.jsx` (design mock — not final; endpoint wiring is the contract)
**Purpose:** Endpoint-by-endpoint sequence for the 4-step booking flow, branching into 3 actions at checkout.
**Scope:** pure move-in flow only — no reservation-fee deposit path (`ReservationFeeAddWithSource_v2` is dead per session handoff §2.2 and §6.2).
**Out of scope:** cancellation, refund, move-out (to be designed later).

---

## UX Overview

```
Step 1: Location  →  Step 2: Storage Profile  →  Step 3: Browse  →  Step 4: Book
  (multi-site         (purpose | photo | manual          (filter/sort         (3 actions)
   or "undecided")     + climate auto/manual)             + discount           │
                                                          + coupon)            ├── Pay Now (move-in)
                                                                               ├── Schedule Viewing
                                                                               └── Agent Callback
```

Progress bar is client-side (`ProgBar` in JSX). State lives in React; server sees one write per step (steps 1–3 mostly read-only). The three actions in Step 4 are the orchestration branches. The Reserve $50 button shown in the JSX mock is **dropped** — `ReservationFeeAddWithSource_v2` has no working production path. Holding/deposit is handled implicitly by `ReservationNewWithSource_v6` inside the Pay-Now saga (hold_until window while Stripe checkout completes).

---

## Step 1 — Location

**JSX:** `Step1` component. User picks 1+ facilities from list, or `"undecided"` (all sites).

**UI state:** `facs: string[]` — array of site_codes, or `['undecided']`.

### Endpoints

| # | Method + Path | Purpose | Source |
|---|---|---|---|
| 1.1 | `GET /api/sites` | Facility list with name, address, postal, region, ratings, review count, MRT, bus routes, unit count | **TO BUILD** — currently hardcoded in JSX (`FAC` constant) |

**Caching:** 1h. Rarely changes.

**Data source:** `SiteInfo` table in esa_pbi; review count / rating from `EmbedSocial` pipeline (already synced); MRT + bus fields would need a new `site_transit` table or JSON column.

**Action:** backend work item — materialize the hardcoded `FAC` object into the DB and expose via `/api/sites`.

---

## Step 2 — Storage Profile

**JSX:** `Step2` component. Three input methods (tabs): `type`, `photo`, `manual`. Plus a shared climate picker with "Auto" (suggested from purposes) or manual override.

**Output (`profile`):** `{ method, purposeIds[], climate, sqftEst, size }` — persisted to React state, passed to Step 3.

### 2a — Purpose-based (tab: `type`)

User picks purposes from `PUR` catalog (Household, Business, Wine, Documents, …). Each implies a climate and sqft. Total sqft computed client-side.

| # | Method + Path | Purpose |
|---|---|---|
| 2.1 | `GET /api/booking/purposes` | Catalog of purposes with icon, label, default sqft, recommended climate | **TO BUILD** (currently hardcoded `PUR` in JSX) |

**Caching:** 1d.

### 2b — AI photo analysis (tab: `photo`)

User uploads up to 10 photos. Each photo analyzed individually. User can edit/correct with a text prompt → re-analyze.

| # | Method + Path | Purpose |
|---|---|---|
| 2.2 | `POST /api/booking/photo-analyze` | multipart: `photo`, optional `prompt` → returns `{items, sqft_estimate, confidence}` | **TO BUILD** — wraps Azure AI Foundry (vision) |

**Request:**
```
POST /api/booking/photo-analyze
Content-Type: multipart/form-data
  photo: <binary>
  prompt: "stack of 6 moving boxes" (optional, for re-analysis)
```
**Response:**
```json
{ "items": "Sofa (3-seater)", "sqft_estimate": 18, "confidence": 87 }
```

**Infra:** reuse `AZURE_FOUNDRY_API_KEY` vault secret. Rate-limit per session (e.g. 30/hour).

### 2c — Manual size (tab: `manual`)

Client-side only. User picks S/M/L/XL. No API.

### Climate picker

Auto-suggested from purposes (priority: Refrig > AC+DC > AC > DC > None). User can override. No API needed — client uses static `CLM` table.

---

## Step 3 — Browse

**JSX:** `Step3` component. Multi-site fan-out, 4 sort modes, discount + coupon sidebar.

### 3.1 Unit list

Single site:
```
GET /api/units/available?site_code=L001&size=S&climate=AC&min_sqft=0&max_sqft=30
```
Multi-site (multiple `facs` selected, or "undecided"):
```
GET /api/units/available?site_codes=L001,L002,L003&size=M&climate=AC
```
All sites:
```
GET /api/units/available?all_sites=1
```

**Current state:** `GET /api/units/available` exists but only accepts single `site_code` (UnitsInformationAvailableUnitsOnly_v2). **TO BUILD**: multi-site variant — either fan out server-side (parallel SOAP calls + merge) or accept `site_codes=` CSV.

**Response:** list of units with `{site_code, unit_id, size_bucket, dims, sqft, climate, price, tags[]}`.

### 3.2 Sort modes (client-side after fetch)

| Mode | Visible when | Logic |
|---|---|---|
| `selection` | always | Apply user's size/climate filters; no sort |
| `best_price` | always | Apply discount/coupon; sort ascending by effective price |
| `nearby` | only if exactly 1 site selected | EXCLUDE that site; show alternative facilities |
| `nearby_best` | only if exactly 1 site selected | Exclude + sort by price |

**Effective price** computed client-side (already in `calcP`) — uses `disc.pct` and `coupon.pct`. No API.

### 3.3 Discount plans

```
GET /api/reservations/discount-plans?site_code=L001
```

Already exists (DB-backed, reads `ccws_discount` table). Returns plans with `{id, name, type, pct, durs[], badge}`.

**Multi-site note:** if `facs` has multiple sites, the UI should intersect plans that exist at ALL selected sites (or flag per-unit availability). **TO DECIDE.**

### 3.4 Coupon validation

```
POST /api/booking/validate-coupon
{ "code": "SAVE10", "site_code": "L001" }
→ { "valid": true, "pct": 0.10, "code": "SAVE10" }
```

**TO BUILD.** Current JSX uses hardcoded `COUPS` table. Backend would check a new `booking_coupons` table with columns `(code, pct, valid_from, valid_to, site_whitelist[], max_uses, uses_count)`.

---

## Step 4 — Book (branching)

**JSX:** `Step4` component. User has chosen {unit, discount, coupon}. Picks `move_in_date`, `duration`, `insurance`. Sees binding total. Clicks one of 4 action buttons.

### 4.0 Shared: binding quote

Before rendering the total, orchestration MUST fetch the authoritative amount:

```
POST /api/booking/quote
{
  "site_code": "L001",
  "unit_id": 12345,
  "move_in_date": "2026-05-01",
  "duration_months": 6,
  "discount_id": 2,
  "coupon_code": "SAVE10",
  "insurance_id": 3
}
→ {
  "binding_total": 1248.50,
  "breakdown": { first_month, ongoing, subseq, discount, coupon, insurance, subtotal, tax, total },
  "lease_end_date": "2026-11-01",
  "confidence": "high",            # or "low" with reason_code
  "reason_code": null,             # e.g. "late_move_in_with_discount"
  "quote_expires_at": "<ISO 10min from now>"
}
```

**TO BUILD** — thin wrapper. Internally:
1. Try calculator (fast) — `calculate_movein_cost(...)` → returns `(lines, reason_code)` tuple.
2. If `reason_code is not None` OR it's the final confirmation screen → call SOAP `MoveInCostRetrieveWithDiscount_v4` for authoritative total.
3. Return `confidence: "low"` + reason_code if calculator flagged; prompt client to re-quote before submit.

**Amount-match rule (§2.6 of session handoff):** the `binding_total` from this call MUST be echoed back as `payment_amount` on move-in; $0.01 drift = SOAP rejects with `Ret_Code=-11`.

**Quote expiry:** 10 minutes. After that, client re-queries.

### Branch 4a — Pay Now (move-in)

The only paid path. Full upfront payment via Stripe → webhook triggers SOAP move-in. Reservation is created first to hold the unit during Stripe checkout; the hold window is short (minutes) since customer is paying immediately.

```
Step 1:  POST /api/tenants                        → tenant_id
         (TenantNewDetailed_v3)
Step 2:  POST /api/reservations/reserve           → waiting_id
         (ReservationNewWithSource_v6)
         { site_code, tenant_id, unit_id, move_in_date,
           hold_until: +30min }         # short — Stripe checkout window
Step 3:  POST /api/stripe/payment-intents         → client_secret, pi_id
         { amount: binding_total, currency: SGD,
           capture_method: "automatic",
           metadata: {
             flow: "pay_now",
             waiting_id, tenant_id, unit_id, site_code,
             payment_amount: binding_total,
             discount_id, insurance_id,
             start_date, end_date
           }}
Step 4:  <client redirects to Stripe Checkout>
Step 5:  Stripe → POST /api/stripe/webhook (payment_intent.succeeded)
         Webhook handler (guarded by stripe_webhook_events — migration 054):
           a. MoveInReservation_v6 (pay_method=2, cash bypass) — converts the
              waiting_id into an active lease; unit is now rented.
           b. PaymentSimpleCheckWithSource (sCheckNumber=pi_xxx) — non-blocking
              ledger audit trail bridging Stripe to SiteLink.
           c. Send confirmation email with access code / gate PIN.
```

**Failure handling:**
- Quote drift between 4.0 and webhook → Ret_Code=-11. Webhook marks `status='failed'`, 500 to Stripe, ops investigates. UX: customer sees "Booking received — confirming" + follow-up email.
- Unit grabbed by someone else between reserve and webhook — shouldn't happen (reserve blocks), but if Ret_Code says so: ops refunds Stripe, offers alternative unit.
- Stripe webhook lost: our sweeper ("Reservation expiry") would release the hold after `hold_until`; we need a separate "stripe_succeeded_but_no_webhook" detector → **orchestration layer responsibility**.

### Branch 4b — Schedule Viewing

Customer wants to see the unit before committing. No SOAP, no Stripe.

```
Step 1:  POST /api/crm/leads                      → lead_id
         { flow: "viewing",
           unit_id, site_code, tenant_name, phone, email,
           preferred_dates: ["2026-04-20", "2026-04-21"],
           notes }
```

**TO BUILD** or extend existing SugarCRM integration. Creates a SugarCRM Lead with:
- `lead_source = "Booking Engine"`
- `account_type = "viewing_request"`
- Custom fields: site_code, unit_id, preferred_dates
- Auto-assigned to site manager via SugarCRM routing rule

Follow-up: site manager confirms via SugarCRM → email to customer. Out of our codebase.

### Branch 4c — Request Agent Callback

Customer wants human contact.

```
Step 1:  POST /api/crm/leads                      → lead_id
         { flow: "callback",
           unit_id (optional), site_code (optional),
           tenant_name, phone, email,
           preferred_time, notes }
```

Same endpoint as 4b, different `flow` value. Same SugarCRM integration.

---

## Endpoint Summary (full list)

**Existing:**
- `GET /api/units/available` (single-site — needs multi-site extension)
- `GET /api/reservations/discount-plans`
- `GET /api/reservations/insurance-coverage`
- `GET /api/billing/tax-rates`
- `GET /api/reservations/move-in/cost` (SOAP-backed; used internally by `/booking/quote`)
- `POST /api/tenants` (TenantNewDetailed_v3)
- `POST /api/reservations/reserve`
- `POST /api/reservations/move-in` (from reservation — MoveInReservation_v6)
- `POST /api/reservations/move-in/direct` (walk-in — MoveInWithDiscount_v7)
- `POST /api/stripe/payment-intents`
- `POST /api/stripe/webhook` (idempotency guarded; Flow B move-in seam present)

**To build:**
- `GET /api/sites` — facility catalog with ratings, transit, address
- `GET /api/booking/purposes` — purpose catalog
- `POST /api/booking/photo-analyze` — AI vision via Azure Foundry
- `POST /api/booking/validate-coupon` — coupon engine
- `GET /api/units/available?site_codes=…` — multi-site fan-out
- `POST /api/booking/quote` — binding quote (wraps calculator + SOAP fallback)
- `POST /api/crm/leads` — viewing / callback → SugarCRM
- `booking_coupons` + `booking_sessions` + `api_idempotency_keys` tables

**Pipeline additions:**
- `reservation_expiry_sweep` — every 5min, calls `ReservationUpdate_v4` cancel on holds past `hold_until` with no successful Stripe PI. The short hold window (30min) means stale holds pile up quickly; frequent sweep is cheap.

---

## Orchestration Layer Responsibilities

The orchestration layer (inside Flask, new `web/orchestration/booking.py`) owns:

1. **Multi-step sagas** for the 3 branches. One function per flow (`pay_now_saga`, `viewing_saga`, `callback_saga`), each returning a `BookingSession` object with state transitions logged to `booking_sessions` table.
2. **Quote staleness detection.** If `quote_expires_at` is past when client submits → 409 Conflict, re-quote.
3. **Compensating actions.** E.g., if Stripe PI create fails after reservation was made → immediately call `ReservationUpdate_v4` (cancel) to release the unit so another customer can grab it.
4. **Session resume.** Customer closes browser mid-flow — `booking_session_id` in cookie lets them resume at the last committed step.
5. **Idempotency.** All `POST` writes accept `Idempotency-Key` header → `api_idempotency_keys` table. Replays within 24h return cached response.
6. **Hold reconciliation.** If Stripe webhook reports `payment_intent.succeeded` for a reservation whose hold already expired and was swept, orchestration must refund the customer and surface an ops alert — should be rare with a 5min sweep and 30min hold.

Flask routes stay thin: validate input, dispatch to orchestration, return saga result.

---

## Open Decisions

1. **Multi-site discount intersection in Step 3.** If customer picks 3 sites and a discount exists on 2 of them, show it? Hide it? Show with "available at 2/3 sites"?
2. **AI photo analysis cost.** Each photo = one Foundry call. Do we rate-limit per session, require login, or absorb the cost for anonymous browsing?
3. **Session persistence: server or client only?** Server-side `booking_sessions` lets us do analytics + resume, but adds a DB write per step. Client-side (localStorage) is simpler but loses cross-device.
4. **`/api/booking/purposes` — DB table or static YAML?** Static rarely changes; DB lets marketing edit without deploy. Lean toward YAML.
5. **Guest checkout vs account?** JSX doesn't show login. Assume guest → tenant is created fresh each time. Duplicate-tenant detection (by email or phone) is a separate concern.
6. **Hold window length.** Currently proposed 30min. Stripe Checkout session default is 24h but we don't want to hold a unit that long. 30min matches typical checkout duration; if abandoned, sweeper releases. Validate with Stripe Checkout session timeout.

---

## Build Order (recommendation)

**Phase 1 — foundational (blocking everything):**
- `GET /api/sites`
- `GET /api/booking/purposes` (YAML-backed)
- `POST /api/booking/validate-coupon` + `booking_coupons` table
- Multi-site `GET /api/units/available`
- `POST /api/booking/quote` (wraps existing calculator + SOAP)

**Phase 2 — Pay Now flow (the only paid path):**
- Wire webhook move-in at the integration seam already in `stripe_payments.py`
- `booking_sessions` saga + `api_idempotency_keys` decorator
- `reservation_expiry_sweep` pipeline
- End-to-end test against LSETUP

**Phase 3 — CRM leads (Viewing + Callback):**
- `POST /api/crm/leads` → SugarCRM (low complexity, existing integration)

**Phase 4 — AI photo analysis:**
- `POST /api/booking/photo-analyze` → Azure Foundry vision
- Rate limiting, cost monitoring
