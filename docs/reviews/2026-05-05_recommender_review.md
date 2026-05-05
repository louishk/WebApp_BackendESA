# Recommendation + Booking Engine — Multi-Agent Review

**Date:** 2026-05-05
**Scope:** ~7,200 LOC across recommender service, booking routes, orchestrator, DLQ worker, calculator, settings, idempotency module
**Reviewers (parallel):** pentest-code-reviewer · backend-api-architect · middleware-architect

---

## Executive summary

The recommendation engine is **functionally solid** — the prior pentest backlog is largely closed (H1/H4/H5/M1–M6) and chain integrity, calculator parity, and idempotency replay protection are all verified. **Three classes of issue surfaced** in this review:

1. **Two HIGH security regressions still open** — H3 (`effective_rate=0` propagates to a $0 ECRI rate-change SOAP call) and `/move-in/direct` has zero payment guard.
2. **Two MEDIUM data-leak surfaces** — `GET /reservations/<id>` returns raw SOAP including tenant PII; `mw_recommendations_served` attribution can be poisoned by spoofing `session_id` (no `api_key_id` filter).
3. **Performance sloppiness** — `/api/recommendations` does **15+ DB roundtrips** per call where ~7 would do; two avoidable session opens on `/move-in`; chain walk is a Python loop instead of a recursive CTE.

**Bottom line for prioritization**:
- Land the **3 HIGH security fixes** before any wider partner exposure (≈ 4h total).
- Land the **N+1 query elimination + flag-load consolidation** for ~50% latency win on the hot path (≈ 2h total).
- Schedule the **API design corrections** (slot-2 label mismatch, idempotency on `/reserve`, error-shape standardisation) as a coordinated v1.1 contract update.

---

## Critical findings at a glance

| # | Severity | Area | Finding | Effort | Blocker for partner handoff? |
|---|---|---|---|---|---|
| S1 | **HIGH** | Security | H3: `_compute_effective_rate` returns `Decimal('0')` on DB exception → silent $0 ECRI rate-change | quick | **YES** |
| S2 | **HIGH** | Security | `/move-in/direct` has no payment sanity guard at all | small | **YES** |
| S3 | **HIGH** | API | Slot 2 label is `"Nearest Available"` in code, doc says `"Best Alternative"` | quick | yes (contract drift) |
| S4 | **HIGH** | API | `reserve_template` ships without per-slot values populated | small | yes (doc lies) |
| S5 | **HIGH** | API | `/reserve` and `/move-in/direct` lack `Idempotency-Key` support | small/med | yes (retry-double-tap) |
| P1 | **HIGH** | Perf | 15+ DB roundtrips per `/api/recommendations` (N+1 in `_serialise_slot`) | quick | no (latency) |
| P2 | **HIGH** | Perf | Two redundant `get_middleware_session()` opens on `/move-in` | quick | no |
| S6 | MEDIUM | Security | `session_id` attribution poisoning — no `api_key_id` scope on the matcher | small | no |
| S7 | MEDIUM | Security | `GET /reservations/<id>` returns raw SOAP (tenant PII) | small | yes (PII leak) |
| S8 | MEDIUM | Security | Pricing intelligence enumeration via 120/min recommend rate | small/med | no |
| A1 | MEDIUM | API | Inconsistent error shapes across endpoints (4+ patterns) | medium | no (breaking) |
| A2 | MEDIUM | API | `total_due_at_movein` vs `total` vs `payment_amount` naming drift | small | yes (charge wrong amount) |
| A3 | MEDIUM | API | `mode=quote` reuses heavy logging path; should be lightweight | small | no |
| A4 | MEDIUM | API | `next_level_allowed` + `recommendation_level` redundant; partners hard-code wrong one | quick | yes (admin tunable breaks bots) |
| P3 | MEDIUM | Perf | Chain walk = N sequential SELECTs (could be 1 recursive CTE) | small | no |
| P4 | MEDIUM | Perf | `build_slot2` re-fetches pool per neighbour site | small | no |
| P5 | MEDIUM | Perf | DLQ worker creates new SOAP client per job (no TLS reuse) | small | no |
| P6 | MEDIUM | Perf | `mw_unit_discount_candidates` freshness vs booking — `/move-in/cost` quotes stale availability | medium | no |
| P7 | MEDIUM | Perf | Watchdog UPDATE on every 10s tick (could be every minute) | quick | no |
| S9 | LOW | Security | Idempotency-Key collision possible if `api_key_id` is ever NULL | quick | no (defence in depth) |
| S10 | LOW | Security | `recommender_settings.update` audit log lacks key/value detail | quick | no |
| A5 | LOW | API | Missing `cheaper_only` action for "I want a deal at same size" intent | small | no |
| A6 | LOW | API | `plan_name` vs `concession_name` naming hierarchy unclear to partners | quick | no (doc-only) |

---

# Section 1 — Security findings

## S1 · [HIGH] H3 still open — `_compute_effective_rate` silent zero → $0 rate-change

**Location:** `backend/python/web/routes/reservations.py:385-387`, `backend/python/web/services/perpetual_orchestrator.py:134`
**CWE:** CWE-20 — Improper Input Validation / Business Logic Flaw

**Current behavior:** On any DB exception, `_compute_effective_rate()` swallows the error and returns `Decimal('0')`. That zero propagates into `OrchestrationContext.effective_rate`. The orchestrator then computes `new_rate = 0 × (1 + ecri_pct/100) = 0.00` and enqueues `ScheduleTenantRateChange_v2` to set the tenant's future rent to **$0**.

```python
# reservations.py:385-387
except Exception as exc:
    logger.warning("effective_rate calc failed: %s", exc)
    return Decimal('0')   # ← silent zero; no guard downstream
```

**Attack/incident scenario:** A transient middleware DB outage during a real move-in causes `_compute_effective_rate` to throw → returns 0 → orchestrator schedules a rent drop to $0 effective at the ECRI date. No human review, no audit warning, just a quietly broken lease.

**Fix:** Guard at the orchestrator before enqueue + raise (don't swallow) inside `_compute_effective_rate`:

```python
# perpetual_orchestrator.py — before rate change job enqueue
if ctx.effective_rate <= Decimal('0'):
    logger.error("Refusing $0 rate change: ledger=%s eff_rate=%s",
                 ctx.ledger_id, ctx.effective_rate)
    return jobs  # skip; ops alerted
```

**Effort:** quick (≤30 min)
**Pros:** Eliminates the $0-rent failure mode; aligns with the rest of the orchestrator's defensive style.
**Cons:** If the DB is genuinely down at move-in time, orchestration silently skips — acceptable given the DLQ worker can re-attempt later.

---

## S2 · [HIGH] `/move-in/direct` has no payment sanity guard

**Location:** `backend/python/web/routes/reservations.py:2877-2880`
**CWE:** CWE-285 — Missing Payment Validation

**Current behavior:** `POST /api/reservations/move-in/direct` (which fires the heavier `MoveInWithDiscount_v7` SOAP op) requires only `reservations:write` scope and has **no payment cross-check at all**. The only floor is `payment_amount > 0`.

**Attack scenario:** Any holder of a `reservations:write` key calls `/move-in/direct` with `payment_amount=0.01`. SMD accepts the cash payment. Tenant moves in for a penny.

**Fix:** Apply the same `_compute_soap_movein_cost` $0.50-tolerance check from `/move-in` to `/move-in/direct`. If the operator decision was deliberate (per H2), at minimum lock the scope to `admin:*` and add an explicit code comment so a future contributor doesn't re-expose it.

**Effort:** small (≤2 h)
**Pros:** Closes undercharge attack on the direct path.
**Cons:** Adds one SOAP cost-retrieve to that endpoint's latency.

---

## S3 · [HIGH] Slot 2 label mismatch — code says `"Nearest Available"`, doc says `"Best Alternative"`

**Location:** `backend/python/web/routes/recommendations.py:50` vs `docs/api/recommendation_engine_public.md`

**Issue:** `_SLOT_LABELS` hardcodes slot 2 as `"Nearest Available"`. Every public-doc example, the slot-semantics table, and the `/api-keys/` UI all say `"Best Alternative"`. Partners string-matching on `slot.label` for UI templating get a value that never matches the doc.

**Fix:** Change the dict to `"Best Alternative"` (or add a stable `label_key="best_alternative"` and tell partners to key on that).

**Effort:** quick (1-line change + verify smoke).
**Cons:** Technically a breaking change if any existing partner already keys on the live string — survey before flipping.

---

## S4 · [HIGH] `reserve_template` ships scaffold without per-slot values populated

**Location:** `backend/python/web/routes/recommendations.py:677-685`

**Issue:** `reserve_template` is a static field-name list; it does NOT populate `site_code`, `unit_id`, or `concession_id` from any slot. The public-doc sample shows them pre-filled — the live response delivers an empty scaffold.

**Fix:** Populate from `slot1_row` (or move `reserve_template` into each `slots[N]` entry so the bot can pick the one matching the customer's choice).

**Effort:** small.
**Pros:** Doc and payload agree; bot doesn't need to remember to copy fields manually.
**Cons:** None material.

---

## S5 · [HIGH] `/reserve` and `/move-in/direct` lack `Idempotency-Key` support

**Location:** `reservations.py:566` (`/reserve`), `reservations.py:2877` (`/move-in/direct`)

**Issue:** `/reserve` performs **two** mutating SOAP calls (`TenantNewDetailed_v3` + `ReservationNewWithSource_v6`). A network blip after tenant creation but before the reservation succeeds leaves an orphan tenant in SiteLink with no safe retry path. `/move-in/direct` has the same gap.

**Fix:** Apply the same `Idempotency-Key` middleware that `/move-in` already uses (`web/utils/idempotency.py` with body-hash mismatch detection). Keyed on `(api_key_id, idem_key, endpoint)`, 24-h TTL.

**Effort:** small (`/reserve`) + medium (`/move-in/direct` if the latter stays partner-callable).
**Pros:** Uniform retry safety across all three booking entry points; partner can ship aggressive retry logic without orphan-data risk.
**Cons:** None.

---

## S6 · [MEDIUM] `session_id` attribution poisoning — no `api_key_id` scope

**Location:** `backend/python/web/services/booking_outcomes.py:255-272`
**CWE:** CWE-284

**Issue:** `_find_candidate()` Priority-1 path matches recommendation rows on `session_id` only. A competing key can pass another key's `session_id` and steal the conversion attribution on `mw_recommendations_served`.

**Fix:** Stamp `api_key_id` on `mw_recommendations_served` at insert time (already in `g.current_user`); add `AND api_key_id = :akid` to all three `_find_candidate` queries.

**Effort:** small.
**Pros:** Locks attribution to the originating key; protects partner conversion stats and commission accounting.
**Cons:** Breaks any legitimate cross-key SSO scenario. Add an `allow_cross_key_attribution` setting if needed.

---

## S7 · [MEDIUM] `GET /reservations/<id>` returns raw SOAP — tenant PII enumerable

**Location:** `reservations.py:1123-1126`
**CWE:** CWE-200

**Issue:** Returns `results[0]` from the SOAP `ReservationList_v3` response verbatim. Includes tenant name, phone, email, address, DOB, internal status codes, notes. A `recommender:read`-scoped key can iterate `waiting_id` 1, 2, 3… and harvest tenant profiles across a site.

**Fix:** Whitelist returnable fields (`WaitingID`, `UnitID`, `dNeeded`, status-only). Or require `tenant_id` to be paired with `waiting_id` (caller must already possess it via `/reserve`).

**Effort:** small.
**Pros:** Limits PII surface; prevents enumeration harvesting.
**Cons:** May break existing internal consumers — discovery pass before flipping.

---

## S8 · [MEDIUM] Pricing intelligence enumeration via 120/min recommend rate

**Location:** `recommendations.py:357-363`

**Issue:** Response surfaces `plan_id`, `plan_name`, `concession_id`, `is_hidden_rate`, `authorised_channels`, full multi-month pricing breakdown. 120 req/min × all sites × all unit types = competitor scrape of the entire pricing + discount catalog.

**Fix:** (1) Daily call-count alerting beyond the per-minute rate limit (data already in `mw_recommendations_served`). (2) Strip `plan_id`/`concession_id`/`authorised_channels` from public-channel responses (the bot needs `concession_id` for `/reserve`, but a `web` channel partner without booking scope doesn't).

**Effort:** small (alerting) / medium (response scrubbing per channel).
**Pros:** Slows competitive intelligence collection.
**Cons:** Need to keep `concession_id` in chatbot responses since it's the load-bearing handoff field — gate by scope, not channel.

---

## S9 · [LOW] Idempotency cache: NULL `api_key_id` would collide

**Location:** `web/utils/idempotency.py:70-80`

**Issue:** Lookup uses `(api_key_id = :aid OR (api_key_id IS NULL AND :aid IS NULL))`. `require_auth` always sets it today, but the schema permits NULL — defence-in-depth gap.

**Fix:** `ALTER TABLE mw_idempotency_keys ALTER COLUMN api_key_id SET NOT NULL`. Skip and warn in `store()` if NULL.

**Effort:** quick.

---

## S10 · [LOW] `recommender_settings.update` audit log lacks key/value detail

**Location:** `backend/python/web/services/recommender_settings.py:246-279`

**Issue:** Audit message is `"Updated N setting(s)"` — no list of which keys, no from/to. Insufficient for incident reconstruction on master switches (`ecri_auto_schedule_enabled`, `perpetual_auto_payment_enabled`).

**Fix:** Return `[(key, old, new)]` tuples; log each individually at audit level.

**Effort:** quick.

---

# Section 2 — API design findings

## A1 · [MEDIUM] Inconsistent error shapes across endpoints

**Issue:** Four distinct shapes in use today:
| Shape | Where | Used for |
|---|---|---|
| `{"error": "...", "field": null}` | `/recommendations` | input validation |
| `{"error": "..."}` | most reservation routes | input validation |
| `{"success": false, "error": "..."}` | `_reservation_soap_call` helper | SOAP-layer rejection |
| `{"status": "success", "data": [...]}` | insurance-coverage endpoint | success |

A partner writing one generic error handler must inspect three different keys (`error`, `success`, `status`) to decide outcome.

**Fix:** Standardise:
- All 4xx → `{"error": "<machine-code>", "message": "<human>", "field": "<name>"|null}`, no `success` key.
- All 2xx → bare data, no `success` wrapper.

**Effort:** medium (touches many handlers; breaking change).
**Pros:** One error-handling path for the partner.
**Cons:** Coordinated v1.1 release with version negotiation.

---

## A2 · [MEDIUM] `total_due_at_movein` vs `total` vs `payment_amount` — naming drift

**Location:** `recommendations.py:287` (`pricing.total_due_at_movein`) vs `reservations.py:2405` (`/move-in/cost` returns `total`)

**Issue:** Recommend response puts the authoritative payment amount at `pricing.total_due_at_movein` (perpetual+prepay = full multi-month total). `/move-in/cost` returns `total` at the top level. Doc tells partners to use `total`. A partner who reads the recommend response and looks for `total` finds nothing — and may use `pricing.first_month_total` (just month 1) as `payment_amount`, triggering a SOAP cost-mismatch rejection.

**Fix:** Always emit a top-level `pricing.payment_amount` on every slot equal to `total_due_at_movein` (when it exists) else `first_month_total`. Document `payment_amount` as **the** field to pass to `/move-in`. Keep the others for transparency.

**Effort:** small. Additive only.

---

## A3 · [MEDIUM] `mode=quote` reuses the heavy recommend logging path

**Issue:** `mode=quote` is conceptually a re-price — but it still requires all four context fields, writes to `mw_recommendations_served`, enforces `request_id` uniqueness, and counts against the recommend rate limit. Partners doing mid-conversation re-quotes have to mint fresh UUIDs for what should be a cheap stateless lookup.

**Fix:** Either (a) add `GET /api/recommendations/quote?unit_id=X&concession_id=Y&duration_months=Z&site_code=S` (stateless, no logging, separate rate budget); or (b) keep the body field but make it lightweight server-side (skip `log_served`, relax `request_id` uniqueness).

**Effort:** small (option b) / medium (option a).

---

## A4 · [MEDIUM] `next_level_allowed` + `recommendation_level` redundant

**Issue:** Both expose chain depth. The doc tells partners to watch `next_turn.next_level_allowed`. But `stats.recommendation_level == 3` is also exposed — and partners will hard-code on it. The moment ops changes max depth via the admin setting, hardcoded checks break.

**Fix:** Mark `recommendation_level` as **diagnostic only** in the doc; explicitly call out: *"Do not hard-code the depth; `next_level_allowed: false` is the only safe signal."*

**Effort:** quick (doc-only clarification).

---

## A5 · [LOW] Missing `cheaper_only` action

**Issue:** Six actions cover size shifts, geo expansion, type change, duration change. None for "I want cheaper at the same spec" — a frequent real chatbot turn. Slot 3 (Best Price) already shows it once, but after the customer rejects all three slots and asks for cheaper, there's no clean way to re-quote.

**Fix:** Add `cheaper_only` → keep filters, re-rank by `effective_rate ASC`, return 3 different units. One branch in `relax_strategy()`.

**Effort:** small.

---

## A6 · [LOW] `plan_name` vs `concession_name` hierarchy not documented

**Issue:** Both surface in every slot. `plan_name` = "Moving Season SG" (customer-facing). `concession_name` = "SS-TAC-Move-R-30%" (SiteLink internal code). Partners pick arbitrarily and may render the operator code to the customer.

**Fix:** Doc note in §10 reference: *"`plan_name` is the customer-facing brand; `concession_name` is an operator-internal SiteLink code — do not display."*

**Effort:** quick (doc-only).

---

# Section 3 — Architecture & performance findings

## P1 · [HIGH] N+1 queries per slot in `_serialise_slot`

**Location:** `recommendations.py:159-181` + `recommender.py:1023-1086` (`quote_slot`)

**Current:** A single `/api/recommendations` 3-slot response runs:
- 1 candidate pool fetch
- 1 chain walk
- 1 session-served-ids fetch
- 1 settings lookup
- **Per slot × 3**: billing_config + charge_descriptions + insurance_premium + insurance_options = **12 queries**

= **15+ DB roundtrips** per call. When all 3 slots share the same `site_id` (the common case), 12 of those 15 are redundant.

**Fix:** Add a request-scoped per-`site_id` memo dict inside the route's slot-build loop. Derive `insurance_premium` from `insurance_options` (it's `min(opt['premium'])`) — eliminates one query entirely.

**Effort:** quick.
**Estimated win:** 15 → 4–7 queries. **~35–50ms p50 saving** at typical DB RTT.

---

## P2 · [HIGH] Two redundant session opens for feature flags on `/move-in`

**Location:** `reservations.py:2573` and `reservations.py:2749`

**Issue:** `movein_soap_cost_check_enabled` and `movein_failure_postmortem_enabled` are both in the same settings cache (60s TTL, in-process dict). They're read via two independent `get_middleware_session()` opens — one on the happy path, one on the failure branch. Each one checks out a connection from the pool only to read from an in-memory dict.

**Fix:** Open one session at the top of the handler, call `recommender_settings.get_all_settings()` once, use the dict for both flags. Remove both inline opens.

**Effort:** quick.
**Pros:** One fewer pool checkout per `/move-in`; removes a fragile inner try/finally.

---

## P3 · [MEDIUM] Chain walk = N sequential SELECTs

**Location:** `recommender.py:420-445`

**Current:** Loop issues one `SELECT FROM mw_recommendations_served WHERE request_id = :rid` per chain hop (max 4 with default depth 3, max 7 at hard ceiling).

**Fix:** Recursive CTE in one query:
```sql
WITH RECURSIVE chain AS (
  SELECT … FROM mw_recommendations_served WHERE request_id = :prev
  UNION ALL
  SELECT r.… FROM mw_recommendations_served r
   JOIN chain c ON r.request_id = c.previous_request_id
)
SELECT * FROM chain LIMIT :max
```

**Effort:** small.
**Estimated win:** 5 → 2 sequential RTTs on L3+L4 chains; ~20–40ms saved per continuation request. Verify `previous_request_id` is indexed.

---

## P4 · [MEDIUM] `build_slot2` re-fetches pool per neighbour site

**Location:** `recommender.py:1057-1078`

**Issue:** When same-site 2nd-cheapest fails, iterates neighbour sites one at a time, calling `fetch_candidate_pool()` per neighbour. Up to 3 extra full-pool queries.

**Fix:** Pass all neighbour codes in a single `location` array to one pool fetch; resolve distance from the already-fetched `dist_rows` dict.

**Effort:** small.

---

## P5 · [MEDIUM] DLQ worker creates a new SOAP client per job (no TLS reuse)

**Location:** `lease_followup_queue.py:278-305`

**Issue:** Each `_execute_action` instantiates a fresh `SOAPClient`. On a batch tick of 10 jobs = 10 TLS handshakes.

**Fix:** Create one `SOAPClient` per `execute_pending_batch` invocation; close after the batch loop.

**Effort:** small.
**Estimated win:** ~9 × ~50–200ms TLS handshakes per batch when there's a backlog.

---

## P6 · [MEDIUM] `mw_unit_discount_candidates` freshness vs `/move-in/cost`

**Issue:** `/move-in/cost` reads from the same pre-computed table the recommender uses. If a unit gets rented between recommend and `/move-in/cost`, the cost endpoint still happily returns a price for it. The first failure surfaces only at `/move-in` itself.

**Fix:** Add a lightweight `UnitStatusRetrieve` SOAP check inside `/move-in/cost` (gated behind the existing `movein_soap_cost_check_enabled` flag). Surfaces the unit-rented condition one step earlier.

**Effort:** medium.
**Pros:** Earlier failure; better UX; same total SOAP calls across the booking flow (the bot already calls /cost before /move-in).
**Cons:** Adds one SOAP roundtrip to `/move-in/cost` latency.

---

## P7 · [MEDIUM] Watchdog UPDATE on every 10s tick

**Location:** `lease_followup_queue.py:144-147`

**Issue:** `recover_stuck_running` issues an UPDATE on every scheduler tick (6/min × 60 = 360/h), even when no rows are stuck.

**Fix:** Run watchdog every 6th tick (≈ once per minute). Stale threshold is 5 minutes, so 60s detection latency is fine.

**Effort:** quick (one-line in the scheduler job).

---

# Action plan

## Sprint 1 — Pre-handoff blockers (≈ 1 day)

Land all of these before opening to a wider partner audience:

1. **S1** — H3 `_compute_effective_rate` zero guard (≤30 min)
2. **S2** — `/move-in/direct` payment guard (2 h)
3. **S3** — Slot 2 label fix (15 min)
4. **S4** — `reserve_template` populated values (1 h)
5. **S5** — `/reserve` Idempotency-Key (2 h)
6. **A2** — `pricing.payment_amount` canonical field (1 h)
7. **A4** — Doc clarification on `next_level_allowed` (15 min)
8. **S7** — `GET /reservations/<id>` PII whitelist (1.5 h)

## Sprint 2 — Performance + remaining HIGH security (≈ 0.5 day)

9. **P1** — Per-site memo in `_serialise_slot` (1 h, biggest latency win)
10. **P2** — Single session open for `/move-in` flags (30 min)
11. **S6** — `api_key_id` scope on attribution matcher (2 h)
12. **A3** — `mode=quote` lightweight path (1 h)

## Sprint 3 — Quality of life (≈ 1 day)

13. **P3** — Chain walk recursive CTE
14. **P4** — slot 2 single-query neighbour fetch
15. **P5** — DLQ worker SOAP client reuse
16. **P7** — Watchdog tick frequency
17. **S8** — Daily call-count alerting
18. **S9/S10** — Idempotency NOT NULL + audit-log detail
19. **A1** — Coordinated error-shape v1.1 release (next contract revision)
20. **A5** — `cheaper_only` action
21. **A6** — Doc clarification on `plan_name` vs `concession_name`

## Sprint 4 — Future (no urgency)

22. **P6** — Live availability check in `/move-in/cost`

---

## Verification gates

After each sprint:

- `python3 scripts/smoke_hard.py` — all 14 scenarios PASS
- Manual probe of changed endpoints with the standard test API key
- For security fixes: pentest agent re-run on the diff
- For performance fixes: latency probe before/after with `time` on a 10-call sample

## Out of scope for this review (intentionally)

- React/Vue migration (project rule: stay vanilla JS)
- SOAP layer abstraction (stable, working)
- Sync pipeline tuning (handled by separate review cadence)
- ECRI long-term scheduling (handled by data team)
