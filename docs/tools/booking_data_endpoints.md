# Booking Engine — Data Architecture & Endpoint Inventory

**Companion doc:** `booking_orchestration_flows.md` (UX → endpoint wiring)
**UX source of truth:** `booking_engine/jsx/booking-enginelatest.jsx` (design mock, not final)
**Scope:** pure move-in flow. Reservation-fee path dropped (`ReservationFeeAddWithSource_v2` is dead per session handoff §2.2).

---

## 0. Current State (as of 2026-04-21)

- ✅ `esa_middleware` DB provisioned on the same Azure Postgres instance as `esa_pbi` (config in `backend/python/config/database.yaml`)
- ✅ New middleware orchestrator running — measurably faster than APScheduler-based `scheduler`
- ✅ Parked in middleware already: **discount plans**, **reservation feed**, **smart lock**, **orchestrator** state tables
- 🔄 Remaining `bk_*` tables + naming convention + discount matrix + Pay-Now saga tables — to build

### What the faster orchestrator unlocks

The middleware orchestrator's throughput changes two design calls that were previously constrained by SOAP latency:

1. **Unit availability can become a short-TTL replica** (was: live SOAP per request). If the orchestrator can refresh per-site availability every 30–60s, Step 3 browse reads from `bk_units_availability` instead of fanning out to SOAP. Fallback to live check at Step 4 quote for correctness.
2. **Discount-matrix rebuild becomes near-real-time** (was: daily CTAS swap + partial on edit). Orchestrator can run the partial rebuild on every `bk_discount_plans` change within seconds, not minutes.

Both are freshness-for-latency trades. Live-SOAP stays as the authoritative fallback for any correctness-critical check (binding quote on `Ret_Code=-11`, final availability confirmation before `ReservationNewWithSource_v6`).

---

## 1. Database Topology

Three physical databases. No cross-DB joins at runtime.

| DB | Purpose | Owner of booking data |
|---|---|---|
| `esa_backend` | App data (users, roles, pages, api_keys, discount_plans admin, inventory mappings) | NO — booking does not read from here at runtime |
| `esa_pbi` | Analytics + SOAP-synced reference data (`ccws_*`, `units_info`, `siteinfo`, `rentroll`) | NO — booking does not read from here at runtime |
| `esa_middleware` | **NEW** — booking orchestration + booking-native state | YES — all `bk_*` tables live here |

### Rules

1. **All new booking-engine tables are `bk_<name>` in `esa_middleware`.**
2. **Replicas over joins.** Reference data from `esa_pbi` / SOAP is replicated into `bk_*` replicas. Data refresh is orchestrated (designed now, activated later).
3. **Booking owns its own reference data.** Naming convention mappings, storage-type rules, coupons, and marketing enrichment are booking-native, not backend replicas. One-time bootstrap from existing tables; diverges thereafter.
4. **Legacy `stripe_webhook_events`** (migration 054 in `esa_backend`) is a migration candidate → `bk_stripe_webhook_events` once `esa_middleware` is provisioned.

---

## 2. SOAP Endpoints (final list)

11 distinct operations. 2 live; 4 scheduled for replicas; 4 per-booking writes; 1 on-failure fallback.

### Live (every request)

| Endpoint | Used in | Cadence |
|---|---|---|
| `UnitsInformationAvailableUnitsOnly_v2` | Pre-reserve final availability check (correctness fallback only) | per booking attempt |

### Short-TTL replica (orchestrator-driven, unlocked by faster orchestrator)

| Endpoint | Target replica | Cadence |
|---|---|---|
| `UnitsInformationAvailableUnitsOnly_v2` | `bk_units_availability` | every 30–60s (orchestrator) |

### Scheduled into `bk_*` replicas

| Endpoint | Target replica | Cadence |
|---|---|---|
| `DiscountPlansRetrieve` (active only) | `bk_soap_discount_concessions` | daily |
| `InsuranceCoverageRetrieve_V2` | `bk_insurance_coverage` | daily |
| `ChargeDescriptionsRetrieve` | `bk_charge_descriptions` | weekly |
| `RentTaxRatesRetrieve` | `bk_site_tax_rates` | weekly |
| `MoveInCostRetrieveWithDiscount_v4` (one call per site for billing config extract) | `bk_site_billing_config` | weekly |

### Per-booking writes (Pay Now saga)

| Endpoint | Used in | Cadence |
|---|---|---|
| `TenantNewDetailed_v3` | Create tenant after Stripe PI created | per booking |
| `ReservationNewWithSource_v6` | Hold unit (30min hold_until) | per booking |
| `ReservationUpdate_v4` (status=2) | Expiry sweeper + compensating cancel on Stripe failure | sweeper every 5min + per failure |
| `MoveInReservation_v6` | Stripe webhook `payment_intent.succeeded` → finalize lease | per successful payment |

### Fallback only

| Endpoint | Used in | Cadence |
|---|---|---|
| `MoveInCostRetrieveWithDiscount_v4` (same op, different use) | On calculator confidence=low OR on `Ret_Code=-11` at move-in time | rare |

### Dropped

- `DiscountPlansRetrieveIncludingDisabled` — active-only is what booking needs
- `UnitTypePriceList_v2` — standard rate already in `bk_units`
- `PaymentSimpleCheckWithSource` — redundant with `MoveInReservation_v6` (move-in already records payment in ledger)
- `ReservationFeeAddWithSource_v2` / `_SCA_v1` — dead path
- Any CC-based payment ops — no processor

### Not available

- `ReservationDelete` does not exist in SMD. Cancel via `ReservationUpdate_v4` with `status_code=2` = functional equivalent.

---

## 3. Calculator Decision

**Option B (calculator-first, gated) + Option C (SOAP fallback on rejection).**

- Calculator powers all quotes → instant UX, no SOAP latency in hot path.
- `bk_discount_plans.is_calculator_safe = TRUE` gates which discount plans are exposed to booking. Unreliable plans (free-month, prepaid-multi-month, late-move-in + discount combos) are hidden.
- If SOAP rejects move-in with `Ret_Code=-11` (amount mismatch), orchestration re-quotes via `MoveInCostRetrieveWithDiscount_v4`, surfaces "price updated" modal, user re-confirms.
- Binding quote snapshots are stored in `bk_quotes` with `quote_id` → echoed back on move-in request for server-side amount-match (prevents client-tampered totals).

---

## 4. Data Architecture

### 4.1 Replica tables

Status: ✅ = exists in middleware, 🔄 = to build, ⚡ = unlocked by the faster orchestrator (cadence tightened)

| Table | Status | Source | Refresh | Contents |
|---|---|---|---|---|
| `bk_sites` | 🔄 | `esa_pbi.siteinfo` + aggregated `embedsocial_reviews` | daily | site_code, name, address, country, region, rating, review_count, unit_count |
| `bk_units_info_raw` | 🔄 | `esa_pbi.units_info` | daily | Raw SOAP unit master (intermediate — `bk_units` is the resolved view) |
| `bk_units_availability` | ⚡🔄 | SOAP `UnitsInformationAvailableUnitsOnly_v2` fanout | **every 30–60s** (orchestrator) | `(site_code, unit_id, is_available, last_synced_at)` — NEW, replaces live-SOAP-per-request for browse |
| `bk_soap_discount_concessions` | ✅ | `esa_pbi.ccws_discount` (or direct SOAP) | daily | SOAP math authority: concession_id, pct, discount_type, durs, is_active_soap — already parked as "discount" |
| `bk_discount_plans` | partial | `esa_backend.discount_plans` | near-real-time on edit | Full marketing layer incl. `linked_concessions` JSONB + booking-only columns (`is_calculator_safe`, `is_published_for_booking`) |
| `bk_reservations` | ✅ | SOAP `ReservationList_v3` | orchestrator cadence | "reservation feed" already parked — mirror of active reservations for reconciliation + sweepers |
| `bk_smart_locks` | ✅ | Igloo / smart-lock API | orchestrator cadence | "smart lock" already parked — unit access-code mapping |
| `bk_charge_descriptions` | 🔄 | `esa_pbi.ccws_charge_descriptions` | weekly | Per-site charge-type tax rates + default prices |
| `bk_insurance_coverage` | 🔄 | `esa_pbi.ccws_insurance_coverage` | daily | Insurance plans with 8% tax |
| `bk_site_billing_config` | 🔄 | `esa_pbi.ccws_site_billing_config` | weekly | Proration mode + day threshold |
| `bk_site_tax_rates` | 🔄 | SOAP `RentTaxRatesRetrieve` (new sync) | weekly | Per-site GST/SST |

### 4.2 Booking-native tables (owned entirely by booking engine)

Naming convention (bootstrap-seeded from existing data once, then divergent):

| Table | Purpose |
|---|---|
| `bk_unit_type_mappings` | Rules mapping (site, raw_type_name, unit_name_pattern, sqft range) → (unit_type_code, size_category, size_range, shape, pillar, climate_code, storage_type). Editable via middleware admin UI. |
| `bk_unit_overrides` | Per-unit manual overrides for any of the 6 naming dimensions + optional rate override. Highest priority in resolution. |
| `bk_storage_type_rules` | Fallback rules: when `bk_unit_type_mappings` doesn't supply a `storage_type`, derive it from (unit_type_code, climate_code, sqft). |

Resolved unit catalog:

| Table | Purpose |
|---|---|
| `bk_units` | Materialized catalog — one row per (site_code, unit_id) with all 6 naming dimensions resolved + full provenance (`*_source` tags) + feature flags + physical + commercial. Rebuilt from `bk_units_info_raw` + `bk_unit_type_mappings` + `bk_unit_overrides` + `bk_storage_type_rules`. |

Marketing enrichment:

| Table | Purpose |
|---|---|
| `bk_sites_marketing` | MRT routes, bus routes, hero image, lat/lng, marketing blurb — extends `bk_sites` |
| `bk_unit_enrichment` | Per-unit marketing: tags (`Near lift`, `Near MRT`), photo URLs, blurb, display_priority, is_hidden |

Coupons + promotions:

| Table | Purpose |
|---|---|
| `bk_coupons` | Coupon codes: discount_type, value, site/plan whitelist, usage caps, validity window |
| `bk_coupon_redemptions` | Redemption ledger: coupon_code, booking_session_id, status (reserved/finalized/abandoned) |

Booking runtime state:

| Table | Purpose |
|---|---|
| `bk_quotes` | Quote snapshots: quote_id, binding_total, breakdown JSON, confidence, reason_code, expires_at — echoed on move-in for server-side amount-match |
| `bk_booking_sessions` | Saga state per booking attempt: flow, status, site, unit, tenant_id, waiting_id, pi_id, hold_until, state transitions |
| `bk_crm_leads` | Local mirror of Viewing/Callback leads pushed to SugarCRM |
| `bk_photo_analyses` | AI photo analysis audit + rate limiting ledger |
| `bk_idempotency_keys` | Generic POST dedup: key, route, response_hash, expires_at |
| `bk_stripe_webhook_events` | **Migration candidate** from `esa_backend.stripe_webhook_events` — webhook dedup |

### 4.3 Pre-computed denormalized table

| Table | Grain | Refresh | Purpose |
|---|---|---|---|
| `bk_unit_discount_matrix` | one row per `(site_code, unit_id, concession_id)` | full rebuild daily after SOAP syncs; partial rebuild on marketing edits | Every eligible discount for every unit, pre-joined with marketing + SOAP math + restrictions. Structural filters (site scope, storage_type match, active flags, published flag) applied at build time. Time + visitor-context filters applied at query time. |

**Build inputs:** `bk_units` + `bk_sites` + `bk_unit_enrichment` + `bk_soap_discount_concessions` + `bk_discount_plans` (with its `linked_concessions` expansion).

**Runtime filters (cheap WHERE on indexed columns):**
- `today BETWEEN period_start/end`
- `today BETWEEN booking_period_start/end`
- `duration_months = ANY(soap_durs)`
- `move_in_date` matches `move_in_range`
- coupon gating
- `is_calculator_safe`

**Size:** ~28 sites × ~1000 units × ~7 eligible concessions = ~200k rows. ~400MB. Comfortable.

### 4.4 Views

| View | Base | Purpose |
|---|---|---|
| `v_bk_sites_listing` | `bk_sites` + `bk_sites_marketing` | Powers `GET /api/sites` |
| `v_bk_unit_discount_matrix_bookable` | `bk_unit_discount_matrix` | Date-window + `is_calculator_safe` prefilter — most booking queries hit this, not the raw matrix |
| `v_bk_quote_inputs` | `bk_units` + `bk_site_billing_config` + `bk_site_tax_rates` + `bk_charge_descriptions` + `bk_insurance_coverage` | One row per (site, unit) with every input the calculator needs |
| `v_bk_coupons_active` | `bk_coupons` | Active, in-window, under-cap coupons |
| `v_bk_coupon_redemptions_summary` | `bk_coupons` + `bk_coupon_redemptions` | Admin dashboard |
| `v_bk_booking_sessions_sweeper` | `bk_booking_sessions` + `bk_stripe_webhook_events` | Drives expiry-sweep pipeline; excludes sessions where Stripe already succeeded |

---

## 5. Discount Authority Split

```
SOAP (SiteLink)
  │ DiscountPlansRetrieve (daily)
  ▼
bk_soap_discount_concessions          ← math authority (pct, type, durs)

esa_backend.discount_plans            ← marketing authority (existing admin UI at /discount-plans/<id>/edit)
  │ replicate on edit                   owns: badges, periods, booking windows, move_in_range,
  ▼                                            storage_type, T&Cs, lock_in, payment_terms,
bk_discount_plans                            applicable_sites, linked_concessions, offers tiers
                                             + booking-only: is_calculator_safe,
                                                             is_published_for_booking

                 JOIN via linked_concessions (JSONB: [{site_id, concession_id}])
                 CROSS JOIN LATERAL jsonb_to_recordset(linked_concessions)
                               │
                               ▼
                    bk_unit_discount_matrix
                    (structural filters applied at build)
                               │
                               ▼
                    v_bk_unit_discount_matrix_bookable
                    (time-window + calculator-safe filter)
                               │
                               ▼
                      /api/reservations/discount-plans
                      /api/units/available (enrich per unit)
                      /api/booking/quote
```

**Net:** SOAP owns the math. Marketing owns presentation + restrictions via the existing `esa_backend.discount_plans` UI. Booking reads only from its `bk_*` replicas.

---

## 6. Unit Naming Convention — Resolution

For every field of a unit, resolve in this order:

```
1. bk_unit_overrides.<field>               (per-unit manual override)
2. bk_unit_type_mappings.<field>            (highest-priority matching rule;
                                             match on site_code + raw_type_name
                                             + unit_name_pattern + sqft range)
3. Derived from bk_units_info_raw           (fallback: size_cat from sqft bucket,
                                             climate from bClimate, etc.)
```

Store resolved value + source tag (`override | type_mapping | raw`) on `bk_units` for audit.

The 6 naming dimensions:

| Dimension | Values | Example |
|---|---|---|
| `unit_type_code` | AC-LOCKER, NC-UNIT, RF-WINE, … | Standardized unit-type code |
| `size_category` | S, M, L, XL | Coarse bucket |
| `size_range` | "0-30", "30-60", "60-90", "90+" | Numeric range |
| `shape` | L (locker), W (walk-in), D (drive-up), SQ (square), R (rectangular) | Physical form factor |
| `pillar` | U, M, L | Position for stacked lockers (upper/middle/lower) |
| `climate_code` | A (air-con), NC (non-climate), RF (refrigerated/wine) | Climate authority |
| `storage_type` | self_storage, wine, documents, workspace | Customer-facing product category (derived) |

Note: `raw.bClimate` from SOAP is known-unreliable on Korean sites (seasonal AC) — we deliberately do NOT use it as authority. It's kept on `bk_units_info_raw` for audit only.

---

## 7. Endpoint → Data-source Mapping

| Route | Reads | Writes |
|---|---|---|
| `GET /api/sites` | `v_bk_sites_listing` | — |
| `GET /api/booking/purposes` | static YAML | — |
| `POST /api/booking/photo-analyze` | Azure Foundry | `bk_photo_analyses` |
| `GET /api/units/available` | `bk_units_availability` (30–60s replica) JOIN `bk_units` + `bk_unit_enrichment` + `v_bk_unit_discount_matrix_bookable`. Live SOAP call ONLY as pre-reserve final check at Step 4. | — |
| `GET /api/reservations/discount-plans` | `v_bk_unit_discount_matrix_bookable` (DISTINCT at plan level) | — |
| `GET /api/reservations/insurance-coverage` | `bk_insurance_coverage` | — |
| `GET /api/billing/tax-rates` | `bk_site_tax_rates` | — |
| `POST /api/booking/validate-coupon` | `v_bk_coupons_active` | — |
| `POST /api/booking/quote` | `v_bk_quote_inputs` + `v_bk_unit_discount_matrix_bookable`; SOAP fallback on low confidence | `bk_quotes`, `bk_booking_sessions` |
| `POST /api/tenants` | SOAP `TenantNewDetailed_v3` | `bk_booking_sessions` |
| `POST /api/reservations/reserve` | SOAP `ReservationNewWithSource_v6` | `bk_booking_sessions` |
| `POST /api/stripe/payment-intents` | Stripe | `bk_booking_sessions` |
| `POST /api/stripe/webhook` | Stripe event → SOAP `MoveInReservation_v6` | `bk_stripe_webhook_events`, `bk_booking_sessions`, `bk_coupon_redemptions` (finalize) |
| `POST /api/crm/leads` | SugarCRM | `bk_crm_leads`, `bk_booking_sessions` |

---

## 8. Restriction Engine (action at search-time)

`GET /api/reservations/discount-plans` and Step 3 browse filter `v_bk_unit_discount_matrix_bookable` by the visitor's search context:

```python
# web/orchestration/discount_filter.py (pseudocode)
def filter_applicable_offers(ctx, coupon=None):
    """
    ctx: {
        site_codes, move_in_date, duration_months, storage_type,
        today, unit_id?
    }
    """
    SELECT * FROM v_bk_unit_discount_matrix_bookable
    WHERE site_code = ANY(:site_codes)
      AND (:unit_id IS NULL OR unit_id = :unit_id)
      AND storage_type = :storage_type
      AND :duration_months = ANY(soap_durs)
      AND duration_meets_lock_in(lock_in_period, :duration_months)
      AND move_in_date_matches(move_in_range, :move_in_date)
      AND (NOT requires_coupon OR :coupon_code = ANY(coupon_whitelist));
```

Free-text parsing (`move_in_range`, `lock_in_period`) handled by helper functions post-query.

---

## 9. Pipelines (runs on the middleware orchestrator)

All new pipelines run on the **middleware orchestrator** (faster than the old APScheduler). Existing "discount / reservation feed / smart lock" pipelines already live there.

| Pipeline | Status | Schedule | Reads | Actions |
|---|---|---|---|---|
| `bk_sync_units_availability` | 🔄 ⚡ | **every 30–60s** | SOAP `UnitsInformationAvailableUnitsOnly_v2` (per site) | → `bk_units_availability` — enables browse without live SOAP |
| `bk_reservations_feed` | ✅ | orchestrator cadence | SOAP `ReservationList_v3` | → `bk_reservations` (already running) |
| `bk_smart_locks_sync` | ✅ | orchestrator cadence | Igloo API | → `bk_smart_locks` (already running) |
| `bk_reservation_expiry_sweep` | 🔄 | every 5min | `v_bk_booking_sessions_sweeper` | SOAP `ReservationUpdate_v4` (status=2) on expired holds; mark session `expired` |
| `bk_stripe_reconcile` | 🔄 | every 15min | Stripe API + `bk_stripe_webhook_events` | Detect succeeded PIs with no processed event; alert ops |
| `bk_unit_discount_matrix_rebuild` | 🔄 | daily after SOAP syncs | all replicas | Full rebuild via CTAS swap |
| `bk_unit_discount_matrix_partial_refresh` | 🔄 ⚡ | **on marketing edit (seconds)** | single plan_id | Partial rebuild triggered by the orchestrator on `bk_discount_plans` change |
| `bk_replicate_sites` | 🔄 | daily | `esa_pbi.siteinfo` + `embedsocial_reviews` | → `bk_sites` |
| `bk_replicate_units_raw` | 🔄 | daily | `esa_pbi.units_info` | → `bk_units_info_raw` |
| `bk_resolve_units` | 🔄 | daily after units raw | `bk_units_info_raw` + `bk_unit_type_mappings` + `bk_unit_overrides` + `bk_storage_type_rules` | Rebuild `bk_units` with resolved naming |
| `bk_replicate_soap_discounts` | ✅ | orchestrator cadence | `esa_pbi.ccws_discount` OR direct SOAP | → `bk_soap_discount_concessions` (already running as "discount") |
| `bk_replicate_discount_plans` | 🔄 | on edit | `esa_backend.discount_plans` | → `bk_discount_plans` (wire change-trigger → middleware) |
| `bk_replicate_charge_descriptions` | 🔄 | weekly | `esa_pbi.ccws_charge_descriptions` | → `bk_charge_descriptions` |
| `bk_replicate_insurance` | 🔄 | daily | `esa_pbi.ccws_insurance_coverage` | → `bk_insurance_coverage` |
| `bk_replicate_billing_config` | 🔄 | weekly | `esa_pbi.ccws_site_billing_config` | → `bk_site_billing_config` |
| `bk_sync_site_tax_rates` | 🔄 | weekly | SOAP `RentTaxRatesRetrieve` | → `bk_site_tax_rates` |

---

## 10. Counts

- **Replica tables:** 11 (3 already parked: discounts, reservations, smart locks; 8 to build — incl. new `bk_units_availability`)
- **Booking-native tables:** 14 (naming conv × 3, marketing × 2, coupons × 2, runtime × 7)
- **Denormalized:** 1 (`bk_unit_discount_matrix`)
- **Views:** 6
- **Pipelines:** 16 (3 already running + 13 to wire on the faster orchestrator)

---

## 11. Build Order (revised from current state)

### ✅ Phase 0 — Provision `esa_middleware` (DONE)
- DB on same Azure Postgres instance as `esa_pbi`
- `database.yaml` config present (middleware section)
- Orchestrator running, faster than scheduler
- Parked tables: `bk_soap_discount_concessions` (discount), `bk_reservations` (reservation feed), `bk_smart_locks` (smart lock) + orchestrator state

### 🔄 Phase 1 — Complete the replica + native schema (IN PROGRESS)
Remaining DDL:
- Replicas: `bk_sites`, `bk_units_info_raw`, `bk_units_availability`, `bk_discount_plans`, `bk_charge_descriptions`, `bk_insurance_coverage`, `bk_site_billing_config`, `bk_site_tax_rates`
- Naming convention: `bk_unit_type_mappings`, `bk_unit_overrides`, `bk_storage_type_rules`
- Resolved: `bk_units`
- Marketing: `bk_sites_marketing`, `bk_unit_enrichment`
- Coupons: `bk_coupons`, `bk_coupon_redemptions`
- Runtime state: `bk_quotes`, `bk_booking_sessions`, `bk_crm_leads`, `bk_photo_analyses`, `bk_idempotency_keys`
- Denormalized: `bk_unit_discount_matrix`
- All 6 views

Bootstrap seed (one-time): copy `esa_backend.inventory_type_mappings` → `bk_unit_type_mappings`, same for overrides. Thereafter divergent.

### ⚡ Phase 2 — Wire the orchestrator jobs (LEVERAGE FASTER CADENCE)
- `bk_sync_units_availability` — 30–60s (the unlock — enables browse without live SOAP)
- `bk_unit_discount_matrix_rebuild` — daily + partial refresh on plan edit
- `bk_resolve_units` — daily after raw units sync
- All `bk_replicate_*` pipelines at stated cadence
- `bk_reservation_expiry_sweep` — 5min
- `bk_stripe_reconcile` — 15min

### 🔄 Phase 3 — Read endpoints (read from `bk_*` replicas)
- `GET /api/sites`
- `GET /api/reservations/discount-plans`
- `GET /api/reservations/insurance-coverage`
- `GET /api/billing/tax-rates`
- `POST /api/booking/validate-coupon`
- `POST /api/booking/quote`
- `GET /api/units/available` — reads `bk_units_availability` (replica), NOT live SOAP

### 🔄 Phase 4 — Write endpoints + saga
- `bk_booking_sessions` saga wiring for Pay Now
- Migrate `esa_backend.stripe_webhook_events` → `esa_middleware.bk_stripe_webhook_events`
- Wire Pay Now webhook → `MoveInReservation_v6` at integration seam
- `bk_idempotency_keys` decorator
- **Pre-reserve final availability check** — one live SOAP call per booking attempt (only place live SOAP is needed in the hot path post-Phase 2)

### 🔄 Phase 5 — Admin UIs
- CRUD for `bk_sites_marketing`, `bk_unit_enrichment`
- CRUD for `bk_coupons` + redemptions dashboard
- CRUD for `bk_unit_type_mappings` + `bk_unit_overrides`
- Extend existing `/discount-plans/<id>/edit` with booking-only flags (`is_calculator_safe`, `is_published_for_booking`)

### 🔄 Phase 6 — CRM + AI
- `POST /api/crm/leads` → SugarCRM for Viewing/Callback
- `POST /api/booking/photo-analyze` → Azure Foundry

---

## 12. Open Questions

1. **`esa_middleware` physical location.** Same Azure Postgres instance (new logical DB) or separate server? Latency implication for the no-cross-DB-joins rule.
2. **Replication mechanism.** Logical replication / CDC / simple cron SELECT+UPSERT? Simplest first; evaluate if drift becomes an issue.
3. **Matrix rebuild strategy.** Full CTAS swap daily + partial DELETE+INSERT on edit. Need to decide LOCK behavior during swap — brief READ lock is fine.
4. **Hold window length.** 30min proposed; Stripe Checkout session timeout defaults to 24h but we don't want to hold that long. Validate.
5. **Multi-site "undecided" browse.** Fanout to 28 sites at once is 4–8s of SOAP latency. Need pagination / progressive loading in the JSX for production.
6. **`move_in_range` free-text format.** Today stored as arbitrary string on `discount_plans`. Need a structured schema for reliable parsing OR a parser function with fallback to "always allow".
7. **Guest checkout vs account.** JSX doesn't show login. Assume guest → tenant created fresh each time. Duplicate-tenant detection (by email/phone) is a separate concern.
8. **Coupon hard-lock vs soft-lock.** `bk_coupon_redemptions` tracks reserved/finalized/abandoned. Do we decrement `current_uses` on `reserved`, or only on `finalized`? Trade-off: hard-lock prevents overbooking a capped coupon but needs abandon-cleanup; soft-lock simpler but a lucky race can exceed the cap.
9. **Review/rating sync freshness** — acceptable at daily?
10. **Geo / map coordinates for site cards** — confirm source (currently missing from `siteinfo`; may need a new data-entry step).
