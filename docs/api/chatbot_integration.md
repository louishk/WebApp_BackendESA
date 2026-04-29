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
│     # optional but recommended for outcome attribution                 │
│     session_id, customer_id, plan_id: s1.plan_id                       │
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
