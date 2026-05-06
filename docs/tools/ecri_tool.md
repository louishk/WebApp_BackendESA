# ECRI Tool — Operational Reference

**Status as of 2026-04-20.** This is the current-state reference for the Existing Customer Rate Increase (ECRI) module. For historical planning context see `ecri_workflow_plan.md` and `ecri_objection_tracker_plan.md`.

## 1. Purpose

Automate the end-to-end rate-increase process for existing tenants across all ESA sites:

1. Select eligible tenants from the live pipeline.
2. Build a batch with target % (or control groups) and billing-cycle-aware effective dates.
3. Site ops reviews and can request exclusions with reason.
4. Revenue decides each exclusion, finalises, and pushes to SMD via SOAP.
5. After the letter goes out, ops can raise objections (modifications); approval is rank-gated and the corrected rate re-pushes to SMD.
6. Churn outcomes track daily within a 90-day attribution window; calendar-year revenue impact rolls up to the end of the year.

## 2. Six-stage workflow

```
draft ──(Revenue: Submit for Site Review)──► site_review
                                                  │
                                       (Revenue: Close Site Review)
                                                  ▼
                                              rev_review
                                                  │
                                      (Revenue: Finalize Batch)
                                                  ▼
                                             rev_approved
                                                  │
                                       (Revenue: Push Live)
                                                  ▼
                                              executing ──► executed
                                                  │
                                           + per-row objections
```

| Stage | Status | Owner | Key actions |
|-------|--------|-------|-------------|
| 1 Build | `draft` | Revenue (`can_manage_ecri`) | Create from eligibility. Submit for Site Review. |
| 2 Site review | `site_review` | Ops (`can_request_ecri_exclusion`, site-scoped) | Request/withdraw exclusions per row with reason. |
| 3 Revenue review | `rev_review` | Revenue (`can_finalize_ecri_batch`) | Approve/reject each exclusion. Finalize when all decided. |
| 4 Approved | `rev_approved` | Revenue (`can_execute_ecri_batch`) | Push Live (async SOAP to SMD). Reopen possible. |
| 5 Executed | `executed` | Ops (`can_create_ecri_objection`) + Revenue | Raise objections; approve/apply. |

**Reopen:** `rev_approved → rev_review → site_review` via `POST /ecri/api/batch/<id>/reopen-review` (button auto-labels based on current stage).

**Force-finalize** past `site_review_deadline` (= earliest `notice_date − 3 days`) works the same as finalize but logs `ECRI_BATCH_FORCE_FINALIZED`.

## 3. Permissions (roles)

Granted on `roles` table; checked via `current_user.can_<permission>()` methods on `User`.

| Permission | Description | Typical role |
|------------|-------------|--------------|
| `can_access_ecri` | View dashboards + analytics | Everyone with ECRI access |
| `can_manage_ecri` | Legacy umbrella flag (create batch, push) — retained for compat | Revenue |
| `can_request_ecri_exclusion` | Request/withdraw exclusions on own site(s) | Ops |
| `can_finalize_ecri_batch` | Close site review, approve/reject exclusions, finalize | Revenue |
| `can_execute_ecri_batch` | Push finalized batch to SMD | Revenue |
| `can_create_ecri_objection` | Raise post-push objections | Ops + Revenue |
| `can_approve_ecri_objection` | Approve above-user-limit objections (rank-gated — see §4) | Senior ops / Revenue |
| `can_manage_ecri_reasons` | Edit exclusion + objection reason tables | Admin |

**Seeded roles:** `ecri_ops` (view + exclusion + objection), `ecri_admin` (all).

## 4. Per-user authority (rank chain)

Two columns on `users`:

- `ecri_max_pct_reduction` — how many percentage points an objection can reduce the ECRI increase without triggering approval.
- `ecri_max_abs_reduction` — absolute SGD reduction ceiling.

Logic at objection submit:
```
reduction_pct = original_increase_pct - new_increase_pct
reduction_abs = original_new_rent     - new_new_rent
if reduction_pct < 0 or reduction_abs < 0:              # bigger hike than planned
    requires_approval = True
elif reduction_pct <= user.ecri_max_pct_reduction
     AND reduction_abs <= user.ecri_max_abs_reduction:
    status = 'approved'                                 # auto-approved
else:
    status = 'pending_approval'
```

**Approver gate** (in `api_approve_objection`): the approver's own limits must ≥ requested reduction. Otherwise 403 — escalate to a higher-rank user. This creates the rank chain without an explicit org-chart table.

Configure per-user via `/admin/users/ecri-limits` (`can_manage_users`).

## 5. Site scoping

`users.allowed_site_ids INTEGER[]`:

- `NULL` or empty → unrestricted (Revenue / admins)
- Non-empty → ops restricted to those `site_id`s

Enforced by:
- `api_request_exclusion`: 403 if tenant's site not in allowed list
- `api_objections_search`: filters results to allowed sites
- `api_get_batch` / batch_review ledger list: filtered client-side using `USER_PERMS.allowed_site_ids`

## 6. Billing-cycle-aware effective date (`common/ecri_dates.py`)

Each ECRI notice carries two dates computed per ledger at batch creation:

- `effective_date` = next Lease Anniversary Date (`dAnniv.day`) after today, pro-rated / clamped to month-end
- `notice_date` = `next_BGD − 1 day` = `effective_date − 15 days`

where `next_BGD = next_LAD − 14 days` (SMD generates bills 14 days before anniversary).

Bucket (colour-coded in the UI):

- 🟢 **green** — notice deadline in future AND `dPaidThru < next_LAD` → catches this cycle
- 🟡 **amber** — tenant currently prepaid (`dPaidThru > today`) but unlocks before `next_BGD` → still catches this cycle
- 🔴 **red** — either notice deadline already passed OR `dPaidThru ≥ next_LAD` → pushed to `next_LAD + 1 month`
- ⚪ **unknown** — no `dAnniv` on file (fallback `today + notice_days`)

**Rounding** (`round_new_rent`): ceil to next whole unit per currency — SGD/MYR/HKD → next dollar, KRW → next 1,000 Won. Never under-captures target increase, clean invoices.

## 7. Data sources

All ECRI queries go through `vw_ecri_eligible_ledgers` (esa_pbi) which joins `ccws_ledgers` (live, active-only) with the latest `rentroll_enriched` extract. View emits `dPaidThru`, `dAnniv`, and exposes `dMovedOut` as NULL (ccws is active-only; absence = moved out).

**Pipeline**: `datalayer/tenant_ledger_charges_to_sql.py` populates `ccws_ledgers` + `ccws_tenants` + `ccws_charges` via SOAP. Ran manually today; needs to be scheduled if you want daily freshness (currently not in `pipelines.yaml`).

**Outcome tracking**: `datalayer/ecri_outcome_tracking.py` — daily cron (`0 8 * * *`). Marks tenants `moved_out` / `scheduled_out` / `stayed` per attribution window; writes to `ecri_outcomes`.

## 8. SOAP integration

**Operation**: `ScheduleTenantRateChange` (v1) on `CallCenterWs.asmx`. V2 adds `iRatesTaxInclusive` but introduces GST gross/net ambiguity — v1 is simpler and matches our tenant-facing rent-stored semantics.

**Params sent** (per ledger):
- `sLocationCode` — from `SiteInfo.SiteCode`
- `LedgerID` (int) — tenant's SMD ledger id (NOT `iLedgerID`)
- `dcNewRate` (decimal, 2dp string) — rounded `new_rent` in native currency
- `dScheduledChange` — `%Y-%m-%dT00:00:00` format (full .NET datetime; bare dates cause HTTP 500)

**SMD returns** `Ret_Code` + `Ret_Msg`. `Ret_Code == -1` → rejected (logged as `failed` in `ecri_batch_ledgers.api_response`).

**Execution model**: async background thread via `_execute_batch_worker`. `POST /api/batch/<id>/execute` returns 202 immediately; UI polls `/api/batch/<id>/progress` every 2s for live counts. Commits per-row so partial progress persists if worker crashes.

**Guards on Push Live**:
- LSETUP allowlist (`config/ecri.yaml → lsetup_allowlist_site_ids: [27525]`) — non-allowlisted sites require `confirm_live=true`
- Typed confirmation phrase `PUSH N LIVE` in UI
- Stale-batch guard: rejects if any `effective_date < today`

## 9. Objections

**Entry point: `/ecri/objections`** — tenant-first search panel at top. Type name / unit / ledger_id / tenant_id → matches across all executed batches → click **Raise Objection** inline.

Applied objections overwrite the original ledger's `new_rent` / `increase_pct` / `increase_amt` and re-push to SMD (idempotent overwrite). The `planned_*` snapshot columns preserve the original for plan-vs-delivered analytics.

Reasons dropdown editable at `/ecri/admin/ecri-reasons` (both exclusion + objection lists, active/inactive toggle + sort order).

## 10. Analytics

**Per batch** (`/ecri/batch/<id>/analytics`):
- Revenue Impact (90-day attribution): stayed / moved_out / churn rate / monthly gain / net
- Control Group Comparison (if A/B enabled)
- **Plan vs Delivered**: planned uplift vs actual delivered (after objections), leakage by reason + staff
- **2026 Revenue Contribution**: month-by-month, day-prorated from effective_date, actual+forecast to EOY

**Portfolio** (`/ecri/`):
- **ECRI Impact on 2026 Book**: cumulative uplift across all executed batches, month-by-month, verified monthly as time passes

Calculation is in `common/ecri_year_impact.py` — pure function, day-prorated, respects `ecri_outcomes.moved_out` for past months and assumes current active persists for future months.

## 11. Key files

**Backend**
- `backend/python/web/routes/ecri.py` — all ECRI routes (~2100 lines)
- `backend/python/common/ecri_dates.py` — effective-date + rounding helpers
- `backend/python/common/ecri_year_impact.py` — year-impact calculator
- `backend/python/common/models.py` — `ECRIBatch`, `ECRIBatchLedger`, `ECRIObjection`, `ECRIOutcome`
- `backend/python/web/models/ecri_reasons.py` — reason models
- `backend/python/datalayer/ecri_outcome_tracking.py` — daily churn pipeline

**Templates** (`backend/python/web/templates/ecri/` and `admin/`)
- `ecri/dashboard.html` — landing with portfolio year impact
- `ecri/batches.html` — full batch list with stage-aware "Needs Action" column
- `ecri/eligibility.html` — run eligibility query, select tenants
- `ecri/batch_create.html` — confirm target % / control groups (from eligibility)
- `ecri/batch_review.html` — mode-switching review (all 5 stages in one template)
- `ecri/analytics.html` — per-batch performance
- `ecri/objections.html` — cross-batch objections + tenant-first search
- `admin/ecri_reasons.html` — reasons CRUD
- `admin/ecri_user_limits.html` — per-user entitlement/limits/sites

**Migrations on esa_pbi** (since ECRI inception): `002, 029, 030, 031, 032, 033, 034, 036, 037, 038, 043, 045` — batches + ledgers + view + objections + planned_* + workflow statuses.

**Migrations on esa_backend**: `039 (allowed_site_ids), 040 (user ECRI limits), 041 (reasons tables), 042 (role perms), 044 (drop ecri_entitled)`.

## 12. Gotchas — learned the hard way

- `ScheduleRentIncrease` does not exist on SMD. Use `ScheduleTenantRateChange`.
- `dScheduledChange` must be a full .NET datetime (`YYYY-MM-DDT00:00:00`); bare date → HTTP 500.
- Eligibility uses `ccws_ledgers.dAnniv` as billing anchor, NOT `dMovedIn` (can differ after lease transfers).
- KRW tenants: `dcNewRate` in Won; rounding uses 1,000 Won increments.
- Delinquent accounts (`dPaidThru` far in the past) may be rejected by SMD with `Ret_Code=-1 "Error retrieving ledger data"` — site ops must clear balance first.
- Stale `ccws_ledgers` (pipeline not run) produces wrong buckets — always check `MAX(extract_date)` before a batch push.

## 13. Operational quickstart

**To run a new batch:**
1. Confirm `ccws_ledgers` freshness — `SELECT MAX(extract_date) FROM ccws_ledgers`. If stale, run `datalayer/tenant_ledger_charges_to_sql.py --mode incremental --location <codes>`.
2. `/ecri/eligibility` → filter sites → Run Eligibility Check → select tenants → Create Batch from Selection.
3. Confirm target % / control groups on the Create page → Submit.
4. Click Submit for Site Review. Ops (site-scoped) get the batch in their queue.
5. When ops is done, click Close Site Review → Revenue Review.
6. Approve/Reject each exclusion. Click Finalize Batch.
7. Click Execute — Push to SiteLink → type confirmation phrase → watch progress modal.
8. After letters go out and tenants call in, handle objections via `/ecri/objections`.
9. Track performance via per-batch analytics (90-day) and portfolio year impact (EOY).

**To delete a test batch:** `DELETE FROM ecri_batches WHERE batch_id=?`; cascade handles ledgers/outcomes/objections. SMD scheduled changes are NOT rolled back automatically — manually cancel in SiteLink if needed.
