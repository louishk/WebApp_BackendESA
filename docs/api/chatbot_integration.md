# Chatbot Integration — Full Lifecycle Scope Guide

This document maps every API endpoint a chatbot needs across a full
recommendation → reservation → move-in lifecycle, plus the JWT scopes
required for each.

Single source of truth for granting permissions to the chatbot's API key
on `/admin/api-keys`.

## Required scopes (minimum bundle)

For PandaAI / any conversational booking bot:

```
recommender              ← quote engine                   [REQUIRED]
reservations:read        ← cost / status lookups          [REQUIRED]
reservations:write       ← reserve / move-in / cancel     [REQUIRED]
```

## Optional scopes (depending on bot's feature set)

```
inventory:read           ← raw unit browsing outside the recommender
smart_lock:read          ← surface keypad/padlock codes to customer at move-in
```

## Scopes the bot should NOT have

```
reservations:track       ← server-to-server tracking pushes; bot doesn't push
*:write (other than reservations:write)
discount_plans:*         ← admin-only
billing:*                ← admin-only
sync:*  scheduler:*  ecri:*  statistics:*
```

---

## Endpoint × scope matrix

### Quoting (recommender)

| Method | Path | Scope | Notes |
|---|---|---|---|
| POST | `/api/recommendations` | `recommender` | 3-slot envelope, conversational |

### Booking lifecycle

| Method | Path | Scope | Notes |
|---|---|---|---|
| POST | `/api/reservations/reserve` | `reservations:write` | Tenant + reservation in one shot (no payment) |
| POST | `/api/reservations/create` | `reservations:write` | Reservation for an existing tenant |
| GET  | `/api/reservations/list` | `reservations:read` | Filter by site / tenant / status |
| GET  | `/api/reservations/<waiting_id>` | `reservations:read` | One reservation full record |
| PUT  | `/api/reservations/<waiting_id>` | `reservations:write` | Update reservation (notes, dates) |
| PUT  | `/api/reservations/<waiting_id>/cancel` | `reservations:write` | Cancel before move-in |
| GET  | `/api/reservations/<waiting_id>/notes` | `reservations:read` | Notes thread |
| POST | `/api/reservations/<waiting_id>/notes` | `reservations:write` | Add a note |

### Cost + move-in

| Method | Path | Scope | Notes |
|---|---|---|---|
| GET  | `/api/reservations/move-in/cost` | `reservations:read` | **SOAP-truth total** — call right before charging |
| POST | `/api/reservations/move-in` | `reservations:write` | Move in from existing reservation; `payment_amount` MUST equal cost |
| POST | `/api/reservations/move-in/direct` | `reservations:write` | Skip reservation, go straight to move-in (rare) |

### Reference data (read-only)

| Method | Path | Scope | Notes |
|---|---|---|---|
| GET | `/api/reservations/discount-plans` | `reservations:read` | List all active plans/concessions for a site |
| GET | `/api/reservations/insurance-coverage` | `reservations:read` | Insurance options + premiums |
| GET | `/api/reservations/insurance-minimums` | `reservations:read` | Required minimum coverage per unit type |
| GET | `/api/reservations/fees` | `reservations:read` | Per-site reservation fees |
| GET | `/api/inventory/units` | `inventory:read` | Raw unit list (use recommender instead when possible) |
| GET | `/api/smart-lock/units` | `smart_lock:read` | Keypad + padlock assignments for a site |

---

## Full lifecycle example

```
┌─ TURN 1 — customer asks "I need a 30sqft unit at Yishun for 6 months" ─┐
│                                                                        │
│ Bot translates "Yishun" → L017, "30sqft" → size_range "30-35"          │
│                                                                        │
│ POST /api/recommendations                                              │
│   scope: recommender                                                   │
│   body: {                                                              │
│     filters: { location: ["L017"], size_range: ["30-35"] },            │
│     duration_months: 6,                                                │
│     context: { request_id, customer_id: "pandai_x", channel: "chatbot" │
│   }                                                                    │
│ ← { slots: [s1, s2, s3], session_id, tracking_id }                     │
└────────────────────────────────────────────────────────────────────────┘
                                ▼
┌─ TURN 2 — customer accepts slot 1's unit ─────────────────────────────┐
│                                                                        │
│ POST /api/reservations/reserve                                         │
│   scope: reservations:write                                            │
│   body: {                                                              │
│     site_code: s1.facility,                                            │
│     unit_id:   s1.unit_id,                                             │
│     concession_id: s1.concession_id,                                   │
│     first_name, last_name, phone, email, ... (customer data),          │
│     needed_date: "2026-05-01",                                         │
│     # STRONGLY RECOMMENDED — outcome reconciliation falls back to      │
│     # heuristics when these are missing and may misattribute on a      │
│     # busy day. Always pass the recommend response's session_id +      │
│     # request_id and the customer_id you used to recommend.            │
│     session_id, customer_id,                                           │
│     previous_request_id: <recommend response.request_id>,              │
│     plan_id: s1.plan_id                                                │
│   }                                                                    │
│ ← { tenant_id, waiting_id, global_waiting_num }                        │
│                                                                        │
│ Reservation locked. NO payment yet. Concession_id is recorded so it    │
│ applies automatically at move-in.                                      │
└────────────────────────────────────────────────────────────────────────┘
                                ▼
                    (time passes — could be days)
                                ▼
┌─ TURN 3 — customer arrives / proceeds to move-in ─────────────────────┐
│                                                                        │
│ Step A — fetch the real cost from SOAP                                 │
│   GET /api/reservations/move-in/cost?                                  │
│       site_code=L017&unit_id=...&concession_id=...&move_in_date=...    │
│       &insurance_id=...&waiting_id=...&variant=reservation             │
│   scope: reservations:read                                             │
│ ← { line_items: [...], total: 298.66 }                                 │
│                                                                        │
│ Step B — execute move-in with that exact total                         │
│   POST /api/reservations/move-in                                       │
│   scope: reservations:write                                            │
│   body: {                                                              │
│     site_code, waiting_id, tenant_id, unit_id,                         │
│     payment_amount: 298.66,           ← MUST match step A exactly      │
│     pay_method: 2,                    ← cash bypass (or 1 = CC)        │
│     concession_id, insurance_id,                                       │
│     start_date, end_date,                                              │
│     test_mode: false                                                   │
│   }                                                                    │
│ ← { ledger_id, lease_num }                                             │
│                                                                        │
│ Lease created. Payment captured.                                       │
│                                                                        │
│ Outcome auto-reconciled: middleware finds the matching                 │
│ mw_recommendations_served row (by session_id / customer_id / unit_id)  │
│ and stamps booked_unit_id, booked_plan_id, booked_concession_id,       │
│ booked_slot, booked_at. Bot does NOT need to call any "mark booked"    │
│ endpoint.                                                              │
└────────────────────────────────────────────────────────────────────────┘
                                ▼
┌─ TURN 4 (optional) — customer wants door code ────────────────────────┐
│                                                                        │
│ GET /api/smart-lock/units?site_ids=...&unit_ids=...                    │
│   scope: smart_lock:read                                               │
│ ← { units: [{ assignment: { keypad_id, padlock_id, gate_code }} ]}     │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Common patterns

### "Customer changed their mind, cancel the reservation"

```
PUT /api/reservations/<waiting_id>/cancel
  scope: reservations:write
  body: { reason: "customer_changed_mind" }
```

### "Customer asks about their existing reservation"

```
GET /api/reservations/<waiting_id>
  scope: reservations:read
```

### "Customer is flexible on location / size / type" — send arrays

`filters.location`, `filters.unit_type`, `filters.climate_type`, and
`filters.size_range` all accept either a scalar string or an array. When
the customer signals flexibility ("I'd take Yishun OR Tampines",
"climate-controlled OR not", "30 to 50 sqft"), pass arrays so the
recommender's pool is naturally larger and Slot 2 / Slot 3 are more
likely to fire with meaningful options.

```jsonc
POST /api/recommendations
{
  "mode": "recommendation",
  "duration_months": 6,
  "filters": {
    "location":     ["L017", "L018", "L022"],   // any of these sites
    "unit_type":    ["W", "L"],                  // walk-in OR locker
    "climate_type": ["A", "AD"],                 // climate-controlled
    "size_range":   ["30-35", "35-40", "40-45"]  // a band, not a point
  },
  "context": { "channel": "chatbot", "request_id": "...", "session_id": "..." }
}
```

Slot definitions with multi-value input:
- **Slot 1 Best Match** — cheapest unit matching the union of all values.
- **Slot 2 Best Alternative** — first available of three strategies:
  1. *Same-site 2nd-cheapest* — second-cheapest unit at the same site as
     Slot 1, different unit_id. No travel cost. Tagged
     `match_flags.alternative_strategy = "same_site_2nd"`.
  2. *Neighbour close* — cheapest match at the nearest other site within
     `max_distance_km` (default 50 km). Tagged `"neighbour_close"` with
     `match_flags.distance_km`.
  3. *Neighbour far* — same as above but within 1.5× radius. Tagged
     `"neighbour_far"` AND `match_flags.travel_warning = true` so the bot
     can disclose the distance.
- **Slot 3 Best Price** — progressively relaxes dimensions to find a
  strictly cheaper unit. `match_flags.relaxed_dims` lists what was dropped.

### "Perpetual + prepayment plans" — single-amount contract for the bot

Some plans are configured for **perpetual discount** with an optional
**N-month prepayment**. The bot integration surface stays **identical** to
the standard flow — the middleware orchestrates the multi-call SOAP
chain internally.

**Bot's only obligation**: charge the customer `pricing.total_due_at_movein`
and pass that same number as `payment_amount` on `/move-in`. Read
`customer_disclosure.fine_print` verbatim to the customer so they
understand what they're committing to.

```jsonc
// /api/recommendations response (slot 1 of a perpetual + 6-mo prepay plan)
{
  "slot": 1, "label": "Best Match",
  "unit_id": 106096, "facility": "LSETUP",
  "plan_name": "Moving Season SG",
  "concession_name": "30% Recurring Discount",
  "discount_summary": "30% off every month",

  "terms": {
    "discount_perpetual":     true,
    "prepayment_months":      6,
    "post_prepay_uplift_pct": 5.0
  },

  "pricing": {
    "first_month_total":      140.18,    // legacy — calculator's move-in cost
    "total_contract":         574.03,
    "total_due_at_movein":    574.03,    // ← bot charges this
    // ── Rate-only (technical — what SOAP records via TenantRate) ──
    "rate_during_prepay":     49.70,     // rent only, post-discount, ex-tax
    "rate_after_prepay":      52.18,     // = 49.70 × 1.05 after ECRI
    "rate_change_date":       "2026-11-15",
    // ── All-in (what the customer actually pays each month) ──
    "monthly_all_in_during_prepay": 57.41,  // rent + insurance + rent_tax + insurance_tax
    "monthly_all_in_after_prepay":  60.22,  // post-ECRI all-in
    "monthly_insurance_premium":     3.00,
    "monthly_insurance_tax":         0.27,
    "monthly_average":               95.67,
    "breakdown": [...]
  },

  "customer_disclosure": {
    "fine_print": [
      "Pay $574.03 today to lock 6 months at $57.41/month all-in.",
      "That's $49.70 rent + $3.00 insurance + $4.71 tax.",
      "After 2026-11-15, monthly adjusts to $60.22 all-in (rent +5% ECRI).",
      "Insurance is changeable — see insurance.options for higher coverage tiers.",
      "You can move out at any time after move-in."
    ],
    "fine_print_template": {
      "amount":                574.03,
      "lock_months":           6,
      "lock_rate":             49.70,    // rent only, technical
      "new_rate":              52.18,    // rent only, post-ECRI
      "monthly_all_in":        57.41,    // bot uses this for headline language
      "monthly_all_in_after":  60.22,
      "insurance_premium":      3.00,
      "insurance_tax":          0.27,
      "from_date":             "2026-11-15",
      "uplift_pct":            5.0
    }
  }
}
```

### Why TWO sets of monthly numbers?

| Field | Type | Use |
|---|---|---|
| `rate_during_prepay` / `rate_after_prepay` | **rent only**, post-discount, ex-tax | Matches what SOAP `ScheduleTenantRateChange_v2` records as the lease's `dcNewRate`. Use for technical / audit purposes. |
| `monthly_all_in_during_prepay` / `monthly_all_in_after_prepay` | **rent + insurance + tax** | What the customer actually sees on their bill. Use this for customer-facing language. |

Don't quote `rate_during_prepay` to a customer — they'll be billed `monthly_all_in_during_prepay` and feel surprised by the gap.

### Customer changes the insurance choice

The default `insurance.selected` is the cheapest available premium. If
the customer picks a different tier from `insurance.options[]`, the bot
has two paths:

**Option A — client-side delta (preferred, no extra API call)**:

```javascript
const chosen = slot.insurance.options.find(o => o.id === userPickedId);
const default = slot.insurance.selected;
const delta = chosen.premium - default.premium;     // e.g. $7 - $3 = $4
const tax_rate = slot.pricing.monthly_insurance_tax / slot.pricing.monthly_insurance_premium;
const new_all_in = slot.pricing.monthly_all_in_during_prepay + delta * (1 + tax_rate);
const new_total_due = slot.pricing.total_due_at_movein + delta * (1 + tax_rate) * slot.terms.prepayment_months;
```

Pass `new_total_due` as `payment_amount` on `/move-in` and pass
`chosen.id` as `insurance_id` — the SOAP MoveIn applies the right
coverage and the orchestrator's prepayment surplus matches.

**Option B — re-call `/api/recommendations`**: omitted in v1; client-side
delta is the simpler shape and avoids the round-trip.

**Plan-shape behaviour** — the same response shape covers all 4 plan
combinations; `terms.discount_perpetual` + `terms.prepayment_months`
tell the bot which one it's quoting:

| Plan shape | `total_due_at_movein` | Customer commits to |
|---|---|---|
| Regular, no prepay | 1 month + admin + dep + ins | First month at discounted rate; standard renewal at +12 mo |
| Regular + SiteLink-native prepay (`bPrepay=true`) | N months bundled by SOAP | Locked discount for N mo; standard renewal at +N+1 mo |
| Perpetual, no prepay | 1 month + admin + dep + ins | Discount applies forever; rate adjusts at +12 mo |
| **Perpetual + custom prepay** | 1 month move-in + (N−1) × discounted recurring | Discount + prepay locked for N mo; rate adjusts at +N |

Behind the scenes, after the bot's `/move-in` call succeeds, the middleware
fires up to two follow-up SOAP calls (PaymentSimpleCash for the prepay
surplus, ScheduleTenantRateChange_v2 for the future ECRI). These run
inline on the happy path; failures land in a DLQ that ops drains via
`/admin/recommendation-engine/lease-followups`. The bot's response will
include `followups: { enqueued, inline_ok, pending_retry, ... }` so the
caller can see the orchestration outcome, but **the lease is fully
created either way** — bot can confirm the booking with the customer
the moment it sees `success: true`.

### Idempotency on `/move-in`

The bot **should** pass `Idempotency-Key: <opaque-string>` on every
`POST /api/reservations/move-in`. Within 24 hours, replays of the same
key return the cached response with `idempotent_replay: true` instead
of firing SOAP again. This protects against retry-on-network-blip
double-MoveIn, which would otherwise create two leases.

```http
POST /api/reservations/move-in
Idempotency-Key: bot-booking-abc123-2026-04-30
X-API-Key: ...

{ "site_code": "L017", "waiting_id": 852447, ... }
```

Bot can pick any opaque string as long as it's stable across retries
of the **same logical booking**. UUID v4 per booking attempt is fine.

### Sanity guard on `payment_amount`

The handler computes the SOAP-truth move-in cost (calculator, validated
exact match) and rejects with **400** if the bot sent a `payment_amount`
that's more than 50¢ short. Hint pointing back to `/move-in/cost`
included in the response. Prevents half-funded leases from network
glitches mid-call.

### "I want a unit at L017 but L017 is fully booked"

When the strict filter yields zero candidates at the requested location(s),
the recommender automatically relaxes dimensions in this order:

1. drop `unit_type`
2. also drop `climate_type`
3. also expand `size_range` ±2 buckets
4. also drop `size_range` entirely

Geography stays fixed — Slot 2's neighbour search is the only path that
reaches another site. The response carries:

- `stats.pool_rescue_step` — comma list of dims that were relaxed
- `stats.saturation_signal = true` — a flag the bot can use to phrase
  "your preferred site/spec is in high demand, here's what we found"
- Each rescued slot's `match_flags.relaxed_dims` lists the same dims
  so per-slot disclosure is possible.

If even the loosest relax + neighbour search yields nothing, slots come
back null — the bot should then escalate to a human or offer a callback.

### "Show me what's available at Yishun" (raw inventory, no quote)

Prefer `POST /api/recommendations` so concessions/restrictions/channel
filtering apply. Only fall back to `/api/inventory/units` when the bot
genuinely needs unstructured raw inventory (e.g. for a site map).

```
GET /api/inventory/units?site_codes=L017&available_only=true
  scope: inventory:read
```

---

## Scope grant checklist for PandaAI key

On `/admin/api-keys` → Edit the chatbot's key → tick:

- [x] `recommender` — POST /api/recommendations
- [x] `reservations:read` — list/get/cost/notes lookups
- [x] `reservations:write` — reserve/cancel/move-in
- [ ] `smart_lock:read` — only if bot tells customer keypad code
- [ ] `inventory:read` — only if bot does raw inventory queries

Save.

---

## What's NOT covered by these scopes

Things the bot might want but currently can't do without admin involvement:

- **Update plan-level config** (discount %, restrictions, channels) — admin web UI only
- **Manage smart-lock assignments** — admin tool
- **Push tracking events** (`reservations:track`) — server-to-server only, not customer-facing
- **Read aggregated stats** — admin-only

These are intentional — the bot operates within the rails ops sets up,
not on top of them.
