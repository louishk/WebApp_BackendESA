# Extra Space Asia — Recommendation & Booking API

**Audience:** integration partners building chat / web / mobile experiences that quote and book self-storage units through Extra Space Asia.

**Base URL:** `https://backend.extraspace.com.sg`

**Auth:** every request must carry `X-API-Key: esa_<prefix>.<secret>` in the header. Your key was provisioned with the scopes you need; missing-scope calls return HTTP 403.

**Format:** JSON in, JSON out. UTF-8. Decimals are JSON numbers (e.g. `91.01`), never strings. Dates are ISO `YYYY-MM-DD`.

**Rate limits:** `/api/recommendations` 120/min · `/api/reservations/reserve` 10/min · `/api/reservations/move-in/cost` 30/min. HTTP 429 on overage with `retry_after` seconds.

---

## 1 · Booking lifecycle at a glance

```
┌───────────────────────────────────────────────────────────────────────┐
│  POST /api/recommendations                                            │
│      → Returns up to 3 priced unit slots matching the customer.       │
│      → Repeat with continuation context to refine the search          │
│        (max 3 levels deep per booking).                               │
└───────────────────────────────────────────────────────────────────────┘
                              ↓ customer accepts a slot
┌───────────────────────────────────────────────────────────────────────┐
│  POST /api/reservations/reserve                                       │
│      → Holds the unit. Creates a tenant + reservation record.         │
│      → No payment yet. Returns waiting_id + tenant_id.                │
└───────────────────────────────────────────────────────────────────────┘
                              ↓ at booking confirmation time
┌───────────────────────────────────────────────────────────────────────┐
│  GET  /api/reservations/move-in/cost                                  │
│      → Authoritative price right before charging the customer.        │
└───────────────────────────────────────────────────────────────────────┘
```

> **Note:** the move-in / lease-creation endpoint is documented separately and is not yet part of this integration scope. For now your integration ends at confirming the price via `/move-in/cost` — actual lease creation is handled by Extra Space Asia operations.

Optional endpoints to manage a held reservation before it is moved in:

- `GET  /api/reservations/<waiting_id>` — read state
- `PUT  /api/reservations/<waiting_id>` — modify (move-in date, contact info)
- `PUT  /api/reservations/<waiting_id>/cancel` — cancel

---

## 2 · Identity model (REQUIRED on every recommend call)

The recommender is **stateful per conversation**. Four context fields tie every step together — from first quote, through follow-up turns, to reserve and price confirmation. They are how the engine identifies which run / turn it is processing and how outcomes get reconciled to the recommendation that produced them.

| Field | Required | Format | Lifetime | Who mints | Notes |
|---|---|---|---|---|---|
| `channel` | yes | `chatbot` \| `web` \| `api` \| `admin` | per call | bot — hardcoded for the channel | Some discount plans are gated by channel. |
| `request_id` | yes | string, ≤ 64 chars (UUID v4 recommended) | unique per turn | bot, fresh per call | Other turns reference it via `previous_request_id`. |
| `session_id` | yes | string, ≤ 64 chars (UUID v4 or opaque) | per **conversation** — same on every recommend turn AND on /reserve | bot, once at chat start | The link that ties downstream calls back to the originating recommendation. If you change it mid-conversation, attribution breaks. |
| `customer_id` | yes | string, ≤ 64 chars | per **customer** lifetime | your channel-side stable ID (e.g. user id) | Used for outcome attribution and per-customer ranking. For anonymous flows, use a stable session-bound surrogate. |
| `previous_request_id` | optional | string, ≤ 64 chars | set on follow-up turns | bot copies from the previous recommend response | Triggers continuation logic (see §4). |

Whitespace-only is rejected. Any field exceeding 64 chars returns HTTP 400.

---

## 3 · `POST /api/recommendations`

Quote engine. Returns up to **3 priced slots** matching the customer's intent.

### 3.1 · Request body

```jsonc
{
  "mode": "recommendation",        // optional · "recommendation" | "quote"
  "duration_months": 6,            // required · integer 1–120
  "filters": {
    "location":     ["L017"],      // required · array of site codes
    "unit_type":    ["W", "L"],    // optional · W=walk-in, L=locker, U=upper, etc.
    "climate_type": ["A", "AD"],   // optional · A=air-con, AD=air-con+dehumid, NC=non-climate, RF=refrigerated
    "size_range":   ["30-35"],     // optional · sqft buckets
    "unit_id":      [107197],      // optional · pin to specific unit(s) (required for mode=quote)
    "concession_id": 11872,        // optional · single int — pin a specific concession (mainly for mode=quote)
    "plan_id":      [10],          // optional · array — surface only units offering these plans (cross-site)
    "coupon_code":  "PROMO123"     // optional · gates coupon-only plans
  },
  "context": {                      // see §2 — all 4 required
    "channel":     "chatbot",
    "request_id":  "550e8400-e29b-41d4-a716-446655440000",
    "session_id":  "sess-conv-7",
    "customer_id": "user_xyz_123",
    "previous_request_id": "...",   // optional · only on follow-up turns
    "picked_slot": 1,               // optional · 1|2|3 — paired with previous_request_id
    "action":      "more_like_this" // optional · see §4
  },
  "constraints": {                  // optional
    "max_distance_km":   50,        // overrides slot-2 default neighbour radius
    "exclude_unit_ids":  [107197]   // up to 200 ids — prevent these from being shown
  }
}
```

### 3.2 · Response

```jsonc
{
  "mode": "recommendation",
  "level": "standard",
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "served_at": "2026-05-04T01:23:45Z",
  "ttl_seconds": 60,

  "stats": {
    "recommendation_level":   1,           // 1, 2, or 3 (see §4)
    "candidates_pool_size":   14,
    "relax_strategy_used":    "none",
    "excluded_unit_ids_count": 0,
    "saturation_signal":      false,       // true when pool was rescued via relaxation
    "pool_rescue_step":       null         // e.g. "unit_type,climate_type" when rescue fired
  },

  "slots": [
    {
      "slot": 1,                  "label": "Best Match",
      "unit_id": 107197,          "facility": "L017",
      "unit_type": "W",           "climate_type": "AD",
      "size_range": "30-35",      "size_sqft": 32.5,
      "plan_id": 10,              "plan_name": "Moving Season SG",
      "concession_id": 11872,     "concession_name": "SS-TAC-Move-R-30%",
      "discount_summary": "30% off every month",

      "match_flags": {
        "alternative_strategy": null,         // null | "same_site_2nd" | "neighbour_close" | "neighbour_far"
        "relaxed_dims":         [],           // dims that were relaxed for this slot
        "distance_km":          0,            // for neighbour-* strategies
        "travel_warning":       false         // true on neighbour_far
      },

      "terms": {
        "discount_perpetual":    true,        // does the discount apply every month, or first month only
        "prepayment_months":     6,           // months prepaid at move-in (when applicable)
        "post_prepay_uplift_pct": 5.0,        // rate change scheduled at end of prepay
        "lock_in_months":        0,
        "promo_valid_until":     "2026-12-31"
      },

      "pricing": {
        "first_month_total":              238.71,
        "total_due_at_movein":            693.76,
        "total_contract":                 693.76,
        "monthly_average":                115.63,
        "rate_during_prepay":             80.50,
        "rate_after_prepay":              84.52,
        "rate_change_date":               "2026-11-15",
        "monthly_all_in_during_prepay":   91.01,
        "monthly_all_in_after_prepay":    95.40,
        "monthly_insurance_premium":      3.00,
        "monthly_insurance_tax":          0.27,
        "breakdown": [
          {"month_index": 1, "billing_date": "2026-05-15", "rent": 115.00, "discount": 34.50,
           "rent_proration_factor": 1.0, "insurance": 3.00, "deposit": 296.00, "admin_fee": 30.00,
           "rent_tax": 7.25, "insurance_tax": 0.27, "total": 238.71},
          {"month_index": 2, "billing_date": "2026-06-15", "rent": 115.00, "discount": 34.50,
           "rent_proration_factor": 1.0, "insurance": 3.00, "deposit": 0, "admin_fee": 0,
           "rent_tax": 7.25, "insurance_tax": 0.27, "total": 91.01}
          /* ... one entry per month of duration_months ... */
        ]
      },

      "insurance": {
        "selected":     {"id": 1, "coverage": 1000, "premium": 3.00},
        "options":      [{"id": 1, "coverage": 1000, "premium": 3.00},
                         {"id": 2, "coverage": 2000, "premium": 7.00}],
        "min_required": 1000
      },

      "customer_disclosure": {
        "fine_print": [
          "Pay $693.76 today to lock 6 months at $91.01/month all-in.",
          "After 2026-11-15, monthly adjusts to $95.40 all-in (rent +5% adjustment).",
          "Insurance is changeable — see insurance.options for higher coverage tiers.",
          "You can move out at any time after move-in."
        ]
      }
    },
    {
      "slot": 2, "label": "Best Alternative",
      "match_flags": {"alternative_strategy": "same_site_2nd"},
      /* ... same shape as slot 1 ... */
    },
    {
      "slot": 3, "label": "Best Price",
      "match_flags": {"relaxed_dims": ["unit_type"], "savings_pct": 18.3},
      /* ... same shape ... */
    }
  ],

  "next_turn": {
    "previous_request_id": "550e8400-e29b-41d4-a716-446655440000",
    "session_id":          "sess-conv-7",
    "supported_actions":   ["more_like_this", "bigger_size", "smaller_size",
                            "expand_locations", "different_type", "different_duration"],
    "next_level_allowed":  true        // false on level-3 responses (see §4)
  },

  "reserve_template": {
    "endpoint": "POST /api/reservations/reserve",
    "required": ["site_code", "unit_id", "concession_id"],
    "site_code": "L017", "unit_id": 107197, "concession_id": 11872, "plan_id": 10
  },

  "pricing_note": "Calculator-quoted; re-fetch GET /api/reservations/move-in/cost at booking time for the authoritative price."
}
```

### 3.3 · Slot semantics

- **Slot 1 — Best Match**: cheapest unit that matches every filter the customer provided.
- **Slot 2 — Best Alternative**: the most convenient alternative. `match_flags.alternative_strategy` tells you which:
  - `same_site_2nd` — different unit at the same site (cleanest UX, no travel).
  - `neighbour_close` — cheapest match at the nearest other site within `max_distance_km`.
  - `neighbour_far` — same as above but in the extended radius. `match_flags.travel_warning = true` — disclose `match_flags.distance_km` to the customer.
- **Slot 3 — Best Price**: a strictly cheaper unit at the same site, found by progressively relaxing one dimension (size → climate → type). `match_flags.relaxed_dims` tells you what was dropped, `match_flags.savings_pct` how much cheaper.

When the candidate pool is empty after strict filtering, the engine runs a **pool rescue**: it relaxes dimensions until a candidate is found and surfaces this in `stats.saturation_signal: true` and `stats.pool_rescue_step`. Each slot's `match_flags.relaxed_dims` lists what was relaxed for that specific slot.

The response is guaranteed to contain **≥ 2 slots in normal operation**. Slot 3 is best-effort — `null` if no strictly-cheaper unit exists.

### 3.4 · `mode=quote`

For "what does unit X cost?" — pass `mode: "quote"` plus `filters.unit_id: [N]`. Returns a single slot keyed to that unit. Useful for re-confirming pricing on a slot the customer accepted, or replying to a customer who named a specific unit.

```jsonc
{
  "mode": "quote",
  "duration_months": 6,
  "filters": { "location": ["L017"], "unit_id": [107197] },
  "context": { /* same 4 required fields */ }
}
```

Response shape is identical, with one slot. Pricing is **cent-identical** to what `mode=recommendation` returned for the same unit.

---

## 4 · Multi-turn search — three levels deep

The recommender is hierarchical. A booking flow can chain at most **three** recommend calls before the customer must either accept a slot or pivot to a fresh search.

```
Level 1   →   "I need 30 sqft in Yishun for 6 months"
              No previous_request_id. Strict filters. Relax strategy = none.

Level 2   →   "Show me something a bit bigger"
              previous_request_id = level-1 request_id. action = "bigger_size".
              Engine inherits level-1 filters, applies the directional shift,
              auto-excludes unit_ids already shown.

Level 3   →   "What about nearby locations as well?"
              previous_request_id = level-2 request_id. action = "expand_locations".
              Engine inherits level-2's effective filters AND applies a new mutation.

Level 4   →   REJECTED with HTTP 400. Either accept a slot via /reserve, or
              start a fresh level-1 (drop previous_request_id; same session_id is fine).
```

`stats.recommendation_level` (1, 2, or 3) tells you where you are. `next_turn.next_level_allowed` flips to `false` on level-3 responses — that's your signal to stop chaining.

### 4.1 · Supported `action` values

Each action expresses a refinement direction. All require `previous_request_id`.

| Action | What the engine does |
|---|---|
| `more_like_this` | Slot-specific tightening. With `picked_slot=1` or `3`: widens size_range by ±1 bucket. With `picked_slot=2`: hops to the next-nearest site. |
| `bigger_size` | Shifts `size_range` one bucket UP (e.g. 30-35 → 35-40). |
| `smaller_size` | Shifts `size_range` one bucket DOWN. |
| `expand_locations` | Adds the nearest neighbour sites (within `max_distance_km`, default 50 km) to `filters.location`. You don't need to know which sites are nearby. |
| `different_type` | Drops the `unit_type` filter. Useful when the customer is open to any layout. |
| `different_duration` | Analytics signal. Bot also changes `duration_months` in the body so the engine re-quotes against the new length. |

### 4.2 · "What if the customer wants something completely different?"

Treat it as a fresh **Level 1**. Mint a new `request_id`, change the filters to the new intent, **keep** `session_id` + `customer_id` (still the same conversation), and **do not** pass `previous_request_id`, `picked_slot`, or `action`. The conversation chain stays linked through `session_id` for analytics.

### 4.3 · "Customer just wants more options, nothing specific"

Pass `previous_request_id` alone — no `action`. The engine inherits prior filters and auto-excludes already-shown unit_ids; the next pool naturally surfaces 3 fresh candidates.

---

## 5 · `POST /api/reservations/reserve`

Holds the unit and creates a tenant + reservation in our backing system. **No payment is taken at this step.**

### 5.1 · Request body

```jsonc
{
  "site_code":   "L017",            // required · the slot's facility
  "unit_id":     107197,            // required · the slot's unit_id
  "concession_id": 11872,           // required* · the slot's concession_id (0 = standard rate, do NOT collapse to null)
  "plan_id":     10,                // optional · attribution

  "first_name":  "Jane",            // required
  "last_name":   "Tan",             // required
  "phone":       "+6591234567",     // required
  "email":       "jane@example.com",// optional
  "mobile":      "+6591234567",     // optional
  "address":     "...",             // optional
  "city":        "Singapore",       // optional
  "postal_code": "569933",          // optional
  "country":     "SG",              // optional

  "needed_date": "2026-05-15",      // optional · move-in date, default = tomorrow
  "comment":     "Booking via chatbot",
  "quoted_rate": 80.50,             // optional

  "session_id":          "sess-conv-7",       // REQUIRED — match the recommend call
  "customer_id":         "user_xyz_123",      // REQUIRED — match the recommend call
  "previous_request_id": "req-bbbb-2222"      // optional · the picked slot's recommend request_id (strongest attribution)
}
```

\* `concession_id`: pass the value from the picked slot. `0` is the explicit "standard rate, no discount" sentinel — do not send `null` for it; the discount path collapses if you do.

### 5.2 · Response

```jsonc
{
  "success": true,
  "site_code": "L017",
  "unit_id": 107197,
  "tenant_id": "1109544",
  "waiting_id": "848757",
  "global_waiting_num": "809363726",
  "message": "Reservation created"
}
```

Hold on to `waiting_id` and `tenant_id` — both identify the held reservation for any subsequent read / modify / cancel call.

---

## 6 · `GET /api/reservations/move-in/cost`

Authoritative cost — call this **immediately before charging the customer**. Confirms the pricing your bot quoted is still accurate.

### 6.1 · Query string

```
?site_code=L017
&unit_id=107197
&concession_id=11872          // pass the same concession used on /reserve
&insurance_id=1               // pass the slot's selected insurance, or 0 for none
&move_in_date=2026-05-15
&waiting_id=848757            // for variant=reservation
&variant=reservation          // standard | reservation | 28day | push_rate (default: standard)
```

For a chatbot booking flow, use `variant=reservation` and pass the `waiting_id` from `/reserve` so the price reflects the held reservation exactly.

### 6.2 · Response

```jsonc
{
  "site_code": "L017", "unit_id": 107197, "move_in_date": "2026-05-15",
  "tenant_rate": 80.50, "discount": 24.15,
  "total": 238.71,                  // <-- this is the dollar amount to charge
  "charges": [
    {"description": "First Monthly Rent Fee", "amount": 80.50,  "tax": 7.25, "total": 87.75},
    {"description": "Administrative Fee",     "amount": 30.00,  "tax": 2.70, "total": 32.70},
    {"description": "Security Deposit",       "amount": 296.00, "tax": 0.00, "total": 296.00},
    {"description": "First Month Insurance",  "amount":  3.00,  "tax": 0.27, "total":   3.27}
  ]
}
```

The `total` field is the authoritative amount for the booking. Surface it to the customer right before confirming the booking.

---

## 7 · Reservation lifecycle (after reserve)

For reading or modifying a held reservation.

### 7.1 · Read

```
GET /api/reservations/<waiting_id>?site_code=L017
```

Returns the reservation's current state.

### 7.2 · Modify

```
PUT /api/reservations/<waiting_id>
Content-Type: application/json

{
  "site_code": "L017",
  "needed_date": "2026-05-20",     // new move-in date
  "first_name": "Jane",            // any contact field can be updated
  "phone": "+6591234567",
  "comment": "Customer changed move-in date"
}
```

Only fields you include in the body are written. Returns `{"success": true, "waiting_id": ..., "message": "Reservation updated"}`.

### 7.3 · Cancel

```
PUT /api/reservations/<waiting_id>/cancel
Content-Type: application/json

{ "site_code": "L017" }
```

Idempotent — cancelling an already-cancelled reservation returns success.

---

## 8 · End-to-end worked example

A 3-turn chat ending in a held reservation with the authoritative price confirmed.

```jsonc
// ─── Turn 1 — fresh recommend ───
POST /api/recommendations
{
  "duration_months": 6,
  "filters": { "location": ["L017"], "size_range": ["30-35"] },
  "context": {
    "channel":     "chatbot",
    "request_id":  "req-aaaa-1111",
    "session_id":  "sess-conv-7",
    "customer_id": "user_xyz_123"
  }
}
// → 3 slots, recommendation_level=1, next_level_allowed=true

// ─── Turn 2 — customer wants something bigger ───
POST /api/recommendations
{
  "duration_months": 6,
  "filters": { "location": ["L017"], "size_range": ["30-35"] },
  "context": {
    "channel":     "chatbot",
    "request_id":  "req-bbbb-2222",
    "session_id":  "sess-conv-7",
    "customer_id": "user_xyz_123",
    "previous_request_id": "req-aaaa-1111",
    "action":      "bigger_size"
  }
}
// → 3 fresh slots in 35-40 size, recommendation_level=2

// ─── Customer picks slot 1 ───
POST /api/reservations/reserve
{
  "site_code":   "L017",
  "unit_id":     107298,
  "concession_id": 11872,
  "first_name":  "Jane", "last_name": "Tan", "phone": "+6591234567",
  "email":       "jane@example.com",
  "needed_date": "2026-05-15",
  "session_id":  "sess-conv-7",
  "customer_id": "user_xyz_123",
  "previous_request_id": "req-bbbb-2222"
}
// → { tenant_id: 1109544, waiting_id: 848757 }

// ─── Confirm authoritative price right before showing total to customer ───
GET /api/reservations/move-in/cost
    ?site_code=L017&unit_id=107298&concession_id=11872&insurance_id=1
    &move_in_date=2026-05-15&waiting_id=848757&variant=reservation
// → { total: 238.71, charges: [...] }
```

---

## 9 · Field reference cheat sheet

### Unit type codes (`filters.unit_type`)

| Code | Meaning |
|---|---|
| `W`  | Walk-in (full-size storage room) |
| `WN` | Wine storage |
| `L`  | Locker (smaller, rack-style) |
| `U`  | Upper position locker |
| `M`  | Mid position locker |
| `LL` | Lower position locker |

### Climate codes (`filters.climate_type`)

| Code | Meaning |
|---|---|
| `A`  | Air-conditioned |
| `AD` | Air-conditioned + dehumidified |
| `NC` | Non-climate |
| `RF` | Refrigerated (specialised) |

### Channel codes (`context.channel`)

| Code | Meaning |
|---|---|
| `chatbot` | Conversational bot integration |
| `web`     | Direct web booking |
| `api`     | Generic API caller |
| `admin`   | Admin tooling |

---

## 10 · Best practices

1. **Always send the four context IDs** (`channel`, `request_id`, `session_id`, `customer_id`) on every recommend call. Keep `session_id` + `customer_id` consistent across recommend → reserve.
2. **Mint a fresh `request_id` per turn**. Don't reuse it; it's the link key for `previous_request_id` on the next turn.
3. **Re-fetch `/move-in/cost` immediately before showing the customer the total**. Pricing can shift if a customer's session lasts longer than expected.
4. **Treat HTTP 200 + `success: false` as a failure** (most often "unit no longer available"). Re-recommend and try again.
5. **Stop chaining at level 3**. Watch `next_turn.next_level_allowed` — when `false`, the bot should encourage the customer to pick a slot or rephrase.
6. **`concession_id=0` means standard rate**. Pass `0`, never `null`, when the picked slot has no discount.
7. **Don't share API keys**. One key per integration; rotate via Extra Space Asia ops if compromised.

---

## 11 · Support

- Integration questions: contact your Extra Space Asia integration manager.
- Live status: `GET /api/health` returns `{"status": "ok"}` when the API is up.
- API key management: visit `https://backend.extraspace.com.sg/api-keys/` (auth required).
