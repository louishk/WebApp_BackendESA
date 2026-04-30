# Booking Middleware — 4-Shape Smoke Test Guide

End-to-end smoke harness that exercises every module of the booking
middleware against a single test site. Built for both internal QA and
3rd-party integration verification.

## What it tests

For each of the 4 plan shapes the middleware supports, the smoke runs
the full booking flow:

```
recommend → /move-in/cost → reserve → move-in (with Idempotency-Key)
            ↓                                  ↓
        SOAP-truth check                    orchestration check
                                             (followups payload)
```

This means every behavior the middleware promises gets exercised in one
shot — recommendation engine, candidate pool, calculator, SOAP MoveIn,
SOAP PaymentSimpleCash, SOAP ScheduleTenantRateChange, idempotency cache,
DLQ enqueue, worker drain, outcome reconciliation.

## The 4 plan shapes

| Shape | Concession `bPrepay` | Plan `discount_perpetual` | Plan `prepayment_months` | What's tested |
|---|---|---|---|---|
| **1** | false | false | NULL | Standard concession path. Bot pays 1 month at move-in. ECRI auto-scheduled at move_in + 12 mo. |
| **2** | **true** | false | NULL | SiteLink-native multi-month prepay. SOAP MoveInCost bundles N months. ECRI at +N+1 mo. **No PaymentSimpleCash** (SiteLink already bundled it). |
| **3** | false | **true** | NULL | Discount perpetual via Tenant's Rate (auto-applied by SiteLink at move-in for compatible concessions). ECRI at +12 mo. **No PaymentSimpleCash**. |
| **4** | false | **true** | **N** | Custom prepay window. Bot prepays N months up front; middleware splits into SOAP MoveIn + `PaymentSimpleCash` for the surplus. ECRI at +N mo. **Both follow-up SOAP calls fire.** |

The harness picks one slot per shape from the recommend response by
inspecting `terms.discount_perpetual`, `terms.prepayment_months`, and
`terms.native_prepay_months`.

## Expected `followups` count per shape

```
shape 1  →  1 job  (schedule_rate_change)
shape 2  →  1 job  (schedule_rate_change at prepaid+1 mo)
shape 3  →  1 job  (schedule_rate_change at +12 mo)
shape 4  →  2 jobs (prepayment + schedule_rate_change at +N mo)
```

When the master switches `ecri_auto_schedule_enabled` and
`perpetual_auto_payment_enabled` are OFF, all shapes return `enqueued=0`
and the lease is created without follow-ups (manual ops workflow).

## Prerequisites

| | |
|---|---|
| API key | `esa_<keyid>.<secret>` with scopes `recommender + reservations:read + reservations:write` |
| Test site | Has at least one available unit and at least one plan of each target shape configured (LSETUP recommended) |
| Master switches | Toggled via `/admin/recommendation-engine` Settings page when you want the orchestration to fire |

If a shape's required plan isn't configured on the test site, the harness
reports `SKIP` for that shape (rest still run).

## Running the harness

```bash
BACKEND_API_KEY=esa_abc...   python3 scripts/smoke_4_shapes.py
```

Optional flags:

```bash
# Run a specific shape only
BACKEND_API_KEY=esa_...   python3 scripts/smoke_4_shapes.py --shape 4

# Hit a different site
BACKEND_API_KEY=esa_...   python3 scripts/smoke_4_shapes.py --site L030

# Hit a different backend (defaults to https://backend.extraspace.com.sg)
BACKEND_BASE_URL=https://staging.example.com  BACKEND_API_KEY=esa_...   python3 scripts/smoke_4_shapes.py
```

Per-shape pause is 13 s to respect the `/move-in` rate limit
(5 requests / 60 s). Full 4-shape run takes ~70 s.

Exit code 0 = all shapes passed. Non-zero = at least one shape failed.

## Reading the output

```
SHAPE 4  —  Perpetual + custom prepay
  ✓ 1. recommend  unit=106096 W/AD/14-16
       plan='Moving Season SG'  concession='30% Recurring Discount'
       terms.discount_perpetual=True
       terms.prepayment_months=6
       terms.native_prepay_months=None
       pricing.total_due_at_movein = $574.03
  ✓ 2. /move-in/cost  SOAP-truth total = $140.18
       bot will charge: $574.03
  ✓ 3. reserve  waiting_id=852514  tenant_id=1112556
  ✓ 4. move-in  ledger_id=593680
       followups: enqueued=2 inline_ok=2 pending_retry=0 failed_permanent=0
  ✓ 5. idempotency replay returned cached response
  ✓ 6. followups count matches expected (2)
```

Each step:

1. **recommend** — `/api/recommendations` returned a slot of the right shape
2. **`/move-in/cost`** — SOAP confirmed the move-in portion (use this number to charge)
3. **reserve** — `/api/reservations/reserve` got a `waiting_id`
4. **move-in** — full booking with `Idempotency-Key`; checks the `followups` orchestration outcome
5. **idempotency** — re-fires the same key; expects `idempotent_replay: true` cached response
6. **followups count** — verifies the orchestrator emitted the expected number of jobs for this shape

## What can go wrong (and what it tells you)

| Failure | Likely cause | Fix |
|---|---|---|
| `SKIP — No slot of shape N found` | No plan/concession of that shape exists on the test site | Configure a plan with the right flags on `/discount-plans/<id>/edit`, or pick a different site with `--site` |
| `1.recommend` 401 | API key missing `recommender` scope | Tick the scope on `/admin/api-keys` |
| `2.cost` 502 | SOAP issue at MoveInCostRetrieve | Retry; if persistent, SOAP outage |
| `3.reserve` "Unit is already rented" | Previous smoke run left the unit occupied | Pick a different unit_id manually or re-run after sync |
| `4.move-in` 400 with `required_minimum` | `payment_amount` < SOAP cost − $0.50 | Bot bug: charge `total_due_at_movein` exactly, not a smaller value |
| `4.move-in` `followups.failed_permanent > 0` | Worker exhausted 5 retries on a SOAP call | Inspect `/admin/recommendation-engine/lease-followups` for the `last_error` |
| `5.idempotency` did not return `idempotent_replay: true` | Cache table missing, or different API key | Check `mw_idempotency_keys` row for the test key |
| `6.followups count` mismatch | Master switches off, or plan misconfigured | Flip `ecri_auto_schedule_enabled` (and `perpetual_auto_payment_enabled` for shape 4) on Settings page |

## What this verifies for 3rd-party integration

A 3rd party (e.g. PandaAI) running this against their own API key with
their own authentication confirms that:

- ✓ Their key has the right scopes
- ✓ The `pricing.total_due_at_movein` field is present and correct per shape
- ✓ The `customer_disclosure.fine_print` text reads correctly
- ✓ The `/move-in/cost` total can be used as the bot's charge amount
- ✓ The single-amount contract works (bot doesn't decompose the total)
- ✓ Idempotency-Key support works (no double-MoveIn on retry)
- ✓ All 4 plan shapes produce coherent quotes and successful move-ins

A passing 4-shape smoke is the green light for the bot to go live.

## Side effects

- Each successful shape creates a real lease **only on the booking site** (LSETUP).
- Reservations and tenants are created via SOAP — they remain on the test site after the smoke.
- The middleware's DLQ accumulates rows in `mw_lease_followup_jobs` (one per follow-up SOAP call).
- The middleware's idempotency cache (`mw_idempotency_keys`) gets one row per shape.

LSETUP is throwaway — leave the artifacts; they self-cleanup as the
table fills.

## Safety: never reserve or move-in on a live site

The harness has a **hard split** between the two layers:

| Layer | Where it runs | Purpose |
|---|---|---|
| Recommend (read-only) | Across `--recommend-sites` (live sites OK by default) | Verifies the recommend response shape per plan type using rich plan inventory on production sites |
| Reserve + Move-in (mutating) | **LSETUP only** | Verifies the booking flow without ever creating a real lease on a live site |

This is enforced two ways:

1. The `_ALLOWED_BOOKING_SITES = {'LSETUP'}` allowlist at the top of
   `scripts/smoke_4_shapes.py`. `main()` rejects with exit code 2 if
   `--booking-site` isn't in the set.
2. `_assert_booking_site_safe()` runs before every `/api/reservations/reserve`
   and `/api/reservations/move-in` call. Raises a `RuntimeError` if the
   slot's facility isn't in the allowlist — even if logic somehow
   tried to feed a live-site slot in by mistake.

Both layers belt-and-braces — extending the allowlist requires editing
the source AND ops sign-off. Default `LSETUP` is the only safe
booking site.

If a shape's plan isn't configured on LSETUP, that shape returns
`recommend_only` (validates the recommend layer) but skips
reserve+move-in cleanly. No silent fall-through to live sites.

## Related docs

- [`docs/api/chatbot_integration.md`](chatbot_integration.md) — full lifecycle scope guide
- [`docs/api/recommendations.yaml`](recommendations.yaml) — OpenAPI spec for the recommend endpoint
- [`docs/api/reservations.yaml`](reservations.yaml) — OpenAPI spec for reserve / move-in / cost
