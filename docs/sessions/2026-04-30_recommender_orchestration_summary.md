# Session Summary — ESA Booking Middleware

**Date:** 2026-04-30
**Branch:** `master`
**Latest commit:** `1743099`

End-to-end build of the recommendation + booking orchestration layer for
the chatbot integration.

---

## Commits this session

| Hash | Phase | Title |
|---|---|---|
| `dc082fe` | P3.6 / 3.7 / 4.B | NL-ready slot response + mode=quote + envelope continuity |
| `8b4cefb` | bug fixes | slot 3 fires reliably + insurance options + discount summary |
| `8164500` | bug fixes | preserve concession_id=0 + bidirectional distance + sqft cap |
| `cc4f934` | guarantee ≥2 slots | dual-strategy slot 2 + pool rescue |
| `606c9f3` | P4 part 1 | discount_perpetual plan flag — discount across full lease |
| `ac90bff` | P4 part 2 | perpetual+prepay quote shape (schema + plan editor + slot disclosure) |
| `3d81a65` | P4 part 2 | perpetual+prepay orchestration via DLQ + worker |
| `7c53f36` | P4 part 2 | admin DLQ + Site Distance + sub-navbar + idempotency + sanity guard |
| `49f260a` | docs | chatbot guide + OpenAPI specs |
| `69b9c06` | smoke | 4-shape harness for 3rd-party integration testing |
| `6d116dd` | docs | bring `/api-keys/` reference up to date with P2 endpoints |
| `33f69e1` | P3 | insurance visibility in slot response — all-in monthly + fine_print |
| `e2ebe39` | bug fix | prepayment window = customer's duration_months, not fixed plan value |
| `1743099` | docs | dynamic prepayment_months semantics |

---

## Major capabilities shipped

### 1. Recommendation engine — `/api/recommendations`

- **3-slot envelope**: Best Match / Best Alternative / Best Price
- **Slot 2 dual-strategy**: same-site 2nd-cheapest → neighbour ≤50 km → ≤75 km (with `travel_warning`)
- **Slot 3 progressive relax**: drops `unit_type` → `climate_type` → `size_range` until strictly cheaper found
- **Pool rescue**: when strict filter pool is empty, progressively relaxes dims until a candidate matches
- **Floor of ≥ 2 slots** verified at 100 % across 90 random scenarios (3 customer-tier personas × 30)
- **`mode=quote`** — single-unit pricing for "tell me about unit X" queries
- **Multi-value filters** — bot can send arrays for `location` / `unit_type` / `climate_type` / `size_range` to encode customer flexibility

### 2. NL-ready slot response (P3.6 / 3.7)

- `plan_name`, `concession_name`, `discount_summary` ("30% off every month")
- `headlines` block — first_month_rent / discount / insurance / tax / deposit / admin_fee / monthly_after_promo
- `terms` block — lock_in / payment_terms / promo_valid_until / min/max_duration / discount_perpetual
- `insurance` block — selected + 11 coverage tiers + min_required
- Real `size_sqft` from `dcWidth × dcLength`
- `next_turn` continuity tokens, `pricing_note`, `reserve_template`, `relax_strategy_used`, `excluded_unit_ids_count`

### 3. Calculator hardening

- Fixed tax rounding (`HALF_UP`, not `ROUND_DOWN`) — verified to match SOAP `MoveInCostRetrieve` to the cent
- Fixed `dcMaxAmountOff=0` clamping bug (SiteLink semantics: 0 means "no cap")
- All recommend numbers come from internal calculator (no SOAP per slot)

### 4. Perpetual discount + prepayment orchestration (P4 Part 1 + Part 2)

- New plan-level flag: `discount_perpetual` — discount applies to every billing month
- Trigger reuses existing `payment_terms = "Prepaid"` field (no new schema for prepayment behaviour)
- Customer's `duration_months` request **dynamically** drives the prepayment window
- After successful SOAP MoveIn, the handler enqueues two follow-up SOAP jobs:
  - `PaymentSimpleCash` — pushes prepay surplus so SiteLink's `dPaidThru` advances
  - `ScheduleTenantRateChange_v2` — schedules ECRI uplift at end of prepay window
- Inline execution on happy path; failures land in `mw_lease_followup_jobs` DLQ
- Worker in `backend-scheduler` polls every 10 s with exponential backoff (5 attempts max → alert ops)
- Two master switches (`ecri_auto_schedule_enabled`, `perpetual_auto_payment_enabled`) for safe rollout
- All four plan shapes verified live:
  1. Regular, no prepay → ECRI at +12 mo
  2. Regular + SiteLink-native prepay → ECRI at prepaid+1
  3. Perpetual, no prepay → ECRI at +12 mo
  4. Perpetual + custom prepay → PaymentSimpleCash + ECRI at +N (dynamic per duration)

### 5. Insurance visibility

- `pricing.monthly_all_in_during_prepay` / `_after_prepay` — what customer actually pays
- `pricing.monthly_insurance_premium` / `_tax`
- `customer_disclosure.fine_print` rewritten with explicit breakdown
- Documented client-side delta pattern for "customer picks a different coverage tier"

### 6. Bot contract guarantees

- **Single-amount payment**: bot charges `pricing.total_due_at_movein`, passes same number to `/move-in`
- **`Idempotency-Key` header** on `/move-in` — 24 h cache, replays return `idempotent_replay: true`
- **Sanity guard**: `400` if `payment_amount` is more than $0.50 below SOAP-truth cost
- **`session_id` + `customer_id`** propagated for outcome reconciliation against `mw_recommendations_served`
- Recommend response includes `next_turn`, `reserve_template`, `pricing_note` so the bot has all integration hints inline

### 7. Admin tooling

- `/admin/recommendation-engine` — global tunables (slot distances, ECRI defaults, master switches, SOAP fallback toggle)
- `/admin/recommendation-engine/simulator` — test recommendations with full UI
- `/admin/recommendation-engine/unit-availability` — global exclusions
- `/admin/recommendation-engine/site-distance` — `mw_site_distance` editor (was DB-only)
- `/admin/recommendation-engine/lease-followups` — DLQ view with manual retry button
- Tertiary sub-navbar across all `/recommendation-engine/*` pages

### 8. Smoke harness for 3rd-party testing

- `scripts/smoke_4_shapes.py` — exercises every plan shape end-to-end
- **Hard safety guard**: `_ALLOWED_BOOKING_SITES = {'LSETUP'}` + `_assert_booking_site_safe()` before every reserve/move-in
- Recommend can probe live sites read-only; reserve+move-in is LSETUP-only
- Self-healing on "Unit already rented" (retry with `exclude_unit_ids`)
- `docs/api/smoke_test_guide.md` — full guide for ops + 3rd party

---

## Documentation shipped

| File | Coverage |
|---|---|
| `docs/api/chatbot_integration.md` | Full lifecycle scope guide; perpetual+prepay walkthrough; Idempotency-Key + sanity guard; multi-value flexibility; insurance re-quote pattern; dynamic-by-duration callout |
| `docs/api/recommendations.yaml` | OpenAPI 3 spec — request/response shapes, all the new pricing/terms/customer_disclosure/insurance fields |
| `docs/api/reservations.yaml` | OpenAPI for `/reserve` + `/move-in` + `/move-in/cost` + `/move-in/direct` + insurance/discount-plan readers, with Idempotency-Key parameter and three response examples |
| `docs/api/smoke_test_guide.md` | 4-shape harness usage, plan-shape matrix, failure-mode table, safety guarantees |
| `/api-keys/` self-service page | In-page catalog of every endpoint the user's scopes unlock — Recommendations group + all Move-In endpoints + Idempotency-Key callout + chatbot booking-flow guide block |

---

## Schema additions (esa_middleware)

- `mw_discount_plans.discount_perpetual` BOOLEAN
- `mw_discount_plans.post_prepay_ecri_pct` NUMERIC(5,2)
- `mw_unit_discount_candidates` — mirror of plan fields plus `concession_name`, `size_sqft`, `lock_in_months`, `promo_valid_until`
- `mw_lease_followup_jobs` — DLQ for orchestration follow-ups
- `mw_idempotency_keys` — 24 h replay cache
- `mw_recommender_settings` — 6 new keys (ECRI defaults, master switches, SOAP fallback toggle)

---

## Live-verified flows

- L017 6-mo perpetual+prepay → `$1,703.42` move-in, `$229.12/mo all-in`, ECRI scheduled `2026-10-30`
- L017 9-mo same → `$2,390.78`, ECRI `2027-01-30`
- L017 12-mo same → `$3,078.14`, ECRI `2027-04-30`
- LSETUP full booking cycle (ledger 593680): SOAP MoveIn + PaymentSimpleCash $282.30 + ScheduleTenantRateChange to $52.18 effective 2026-11-15 → `dPaidThru` advanced 9+ months
- 4-shape orchestrator decision tree verified for all combinations

---

## Pending (carry-over)

- **Task #60** — flip the `recommender` scope on the PandaAI key (admin UI action on `/admin/api-keys`)
- **Task #75** — additional SOAP discount-behaviour probes (no urgent gap)
- **Master switches** (`ecri_auto_schedule_enabled`, `perpetual_auto_payment_enabled`) currently ON for testing — flip to OFF for production rollout when ready
