# ECRI Objection Tracker — Plan

> ⚠️ **Superseded — fully absorbed into the shipped ECRI workflow.**
> See `ecri_tool.md` §9 for the current objections flow.
> Kept as historical planning context only.

**Purpose:** Let site ops log modifications made to a tenant's rate-change after the ECRI batch has been pushed to SMD, so ECRI performance (plan-vs-actual) can be measured rather than assumed. Track who changed what, why, and with what authority.

**Triggering context:** A tenant receives the rate-increase notice, pushes back, ops negotiates a smaller increase (or a delayed effective date), the change gets applied in SiteLink manually. Today that negotiation happens outside the tool — we lose all visibility.

## 1. Scope (MVP)

In-scope:
- Record an objection per `ecri_batch_ledgers` row that has already been pushed to SMD (`api_status='success'`).
- Capture: new % increase OR new absolute rent; objection reason; free-text notes; staff name (auto from `current_user`); date/time.
- Role-based approval limit: if the requested reduction is within the staff member's allowance, auto-approved; otherwise routes to an approver.
- Once approved, push the corrected rate to SMD via `ScheduleTenantRateChange` (re-push overwrites the existing scheduled change — we already confirmed this works).
- Separate "Objections" window, filterable across all batches, with staff + batch + status filters.
- Admin UI to set per-role approval limits.

Out-of-scope (v2):
- Objection templates / merge-field letters.
- Bulk objections (one ledger at a time in v1).
- Objections that change the effective date (only %/rent in v1).
- Tenant-facing communication workflow (email the tenant).

## 2. Data model

### New table: `ecri_objections` (esa_pbi)

```sql
CREATE TABLE ecri_objections (
    id                    BIGSERIAL PRIMARY KEY,
    batch_ledger_id       BIGINT NOT NULL REFERENCES ecri_batch_ledgers(id) ON DELETE CASCADE,
    batch_id              UUID NOT NULL,                    -- denormalised for fast batch filters
    site_id               INTEGER NOT NULL,                 -- denormalised
    ledger_id             INTEGER NOT NULL,                 -- denormalised

    -- Snapshot of the ECRI values being objected to
    original_increase_pct NUMERIC(5, 2) NOT NULL,
    original_new_rent     NUMERIC(14, 4) NOT NULL,
    currency              VARCHAR(3) NOT NULL,

    -- Proposed values
    new_increase_pct      NUMERIC(5, 2),                    -- at least one of these two is required
    new_new_rent          NUMERIC(14, 4),                   -- if pct set, compute rent; if rent set, compute pct
    new_effective_date    DATE,                             -- optional override; default = keep original

    -- Reason
    reason_code           VARCHAR(40) NOT NULL,             -- enum-ish; see §5
    reason_notes          TEXT,

    -- Workflow state
    status                VARCHAR(20) NOT NULL DEFAULT 'pending_approval'
                          CHECK (status IN ('pending_approval','approved','rejected','applied','cancelled')),
    requires_approval     BOOLEAN NOT NULL,                 -- computed at submit based on reduction vs user's limit

    -- Who
    raised_by_user_id     INTEGER NOT NULL,                 -- users.id from esa_backend; snapshot below
    raised_by_username    VARCHAR(100) NOT NULL,
    raised_at             TIMESTAMP NOT NULL DEFAULT NOW(),

    approver_user_id      INTEGER,
    approver_username     VARCHAR(100),
    approved_at           TIMESTAMP,
    approval_notes        TEXT,

    applied_at            TIMESTAMP,                        -- when pushed to SMD
    applied_ret_code      VARCHAR(20),                      -- SMD Ret_Code echo
    applied_ret_msg       TEXT
);

CREATE INDEX idx_ecri_obj_batch         ON ecri_objections(batch_id);
CREATE INDEX idx_ecri_obj_status        ON ecri_objections(status);
CREATE INDEX idx_ecri_obj_raised_by     ON ecri_objections(raised_by_user_id);
CREATE INDEX idx_ecri_obj_batch_ledger  ON ecri_objections(batch_ledger_id);
```

Migration file: `backend/python/migrations/034_ecri_objections.sql`.

### New columns on `roles` (esa_backend)

```sql
ALTER TABLE roles
    ADD COLUMN IF NOT EXISTS ecri_max_pct_reduction NUMERIC(5,2) DEFAULT 0,   -- e.g. 3.00 means can reduce pct by up to 3 points without approval
    ADD COLUMN IF NOT EXISTS ecri_max_abs_reduction NUMERIC(14,4) DEFAULT 0,  -- e.g. SGD 20 absolute reduction, whichever is smaller
    ADD COLUMN IF NOT EXISTS can_approve_ecri_objection BOOLEAN DEFAULT FALSE;
```

Migration file: `backend/python/migrations/035_ecri_approval_limits.sql`.

Per-role limits are simpler than per-user for v1. If a user has multiple roles, use the maximum across their roles (most-permissive wins).

### New permissions (on `users` / `roles` permission model)

- `can_create_ecri_objection` — create an objection. Default for ops staff with ECRI access.
- `can_approve_ecri_objection` — approve above-limit objections. Senior staff / managers.
- `can_manage_ecri_approval_limits` — edit limits in admin. Admin only.

## 3. Approval logic

When staff submits an objection:

```
reduction_pct   = original_increase_pct - new_increase_pct        (how many points lower)
reduction_abs   = original_new_rent - new_new_rent                (absolute amount)

max_pct_allowed = max(role.ecri_max_pct_reduction for role in user.roles)
max_abs_allowed = max(role.ecri_max_abs_reduction for role in user.roles)

if reduction_pct <= max_pct_allowed AND reduction_abs <= max_abs_allowed:
    requires_approval = False
    status = 'approved'        # auto-approved at submit
    approver_user_id = NULL    # system-approved
else:
    requires_approval = True
    status = 'pending_approval'
```

Increases (reduction < 0, i.e. ops wants a bigger rate hike) ALWAYS require approval — paranoid default.

## 4. Routes

### Web pages
- `GET /ecri/objections` — dashboard, all batches.
- `GET /ecri/batch/<batch_id>/objections` — per-batch objections list.
- `GET /admin/ecri-approval-limits` — admin, per-role limit matrix.

### API endpoints
| Endpoint | Method | Who | Purpose |
|----------|--------|-----|---------|
| `/ecri/api/batch/<id>/objections` | GET | ECRI access | List objections for a batch |
| `/ecri/api/objections` | GET | ECRI access | List all, with filters (status, staff, site, date) |
| `/ecri/api/objection` | POST | `can_create_ecri_objection` | Submit a new objection |
| `/ecri/api/objection/<id>/approve` | POST | `can_approve_ecri_objection` | Approve pending |
| `/ecri/api/objection/<id>/reject` | POST | `can_approve_ecri_objection` | Reject with notes |
| `/ecri/api/objection/<id>/apply` | POST | `can_create_ecri_objection` + status=approved | Push to SMD |
| `/ecri/api/objection/<id>/cancel` | POST | owner or approver | Withdraw before apply |
| `/admin/api/ecri-limits/<role_id>` | PUT | `can_manage_ecri_approval_limits` | Update limits |

### SMD integration

`apply` endpoint reuses the existing `ScheduleTenantRateChange` SOAP call (same shape as `_execute_batch_worker`). Because SMD's behaviour is "schedule overwrites previous schedule for same ledger", re-pushing is safe and deterministic. On success, UPDATE the corresponding `ecri_batch_ledgers` row's `new_rent`, `increase_pct`, `increase_amt`, `api_response` (append a history entry), and flag the row with `last_objection_id`.

## 5. Reason codes (dropdown)

Predefined list in a config file (`backend/python/config/ecri.yaml`) so it's editable without migration:

```yaml
objection_reasons:
  - code: tenant_hardship
    label: Tenant hardship / financial difficulty
  - code: competitor_match
    label: Matched competitor rate
  - code: long_term_retention
    label: Retention — long-term tenant
  - code: unit_issue
    label: Unit quality / access issue
  - code: billing_error
    label: Billing or calculation error
  - code: negotiated_discount
    label: Negotiated discount (no specific category)
  - code: other
    label: Other (specify in notes)
```

Store the `code` in `reason_code` column; resolve label at render time.

## 6. UI

### Batch review page (existing)

Add an "Objection" action per ledger row for rows where `api_status='success'`. A pencil/edit icon in a new rightmost column. Clicking opens a modal.

### Objection modal (inline in batch_review.html)

- Read-only: site / ledger / tenant / original pct / original new_rent / currency
- Input: new pct (auto-computes new rent) OR new rent (auto-computes pct) — either/or
- Input: effective date override (optional, defaults to original)
- Dropdown: reason_code (from config)
- Textarea: notes (required if reason = 'other')
- Live indicator at bottom: *"This will be auto-approved"* (green) or *"This requires manager approval"* (amber), based on computing the reduction against the user's limit client-side (recompute server-side on submit for authority).
- Submit button.

### New page: `/ecri/objections` (batch_review.html clone)

Columns: batch / site / ledger / tenant / original → new (%, rent) / reason / staff / status / date / actions.
Filters: status, batch, site, staff, date range.
Row actions:
- `pending_approval` + user can approve → "Approve" / "Reject" buttons.
- `approved` + raised-by-me-or-anyone → "Apply to SMD" button with typed confirmation phrase.
- `applied` → read-only (show applied timestamp + SMD ret_msg).

Summary cards at top: counts by status + total revenue impact in SGD (sum of `(original_new_rent - new_new_rent)` converted to SGD).

### Admin page: `/admin/ecri-approval-limits`

Matrix view of all roles × limit columns. Inline edit (number inputs). Save button. Audit the change via existing `audit_log(AuditEvent.CONFIG_UPDATED, ...)`.

## 7. Integration with existing ECRI analytics

- `api_get_batch` response already returns per-ledger detail — add `has_objection`, `active_objection_id`, `applied_objection_id` fields so the batch-review UI can badge rows that have been modified.
- ECRI analytics page (`/ecri/batch/<id>/analytics`) gets a new section: "Post-push modifications" — count, % of batch, avg reduction, revenue impact, top reasons, top staff by count.
- Outcome tracking (`ecri_outcome_tracking.py`) should consider: did the objection CHANGE churn behaviour? Add a field to the outcome join so analytics can compare churn for un-modified vs modified ledgers.

## 8. Audit & logging

- `audit_log(AuditEvent.ECRI_OBJECTION_CREATED, ...)` at submit
- `audit_log(AuditEvent.ECRI_OBJECTION_APPROVED, ...)` / `_REJECTED`
- `audit_log(AuditEvent.ECRI_OBJECTION_APPLIED, ...)` at SMD push
- `audit_log(AuditEvent.ECRI_APPROVAL_LIMIT_CHANGED, ...)` at admin edit

Add the enum members to `web/utils/audit.py`.

## 9. Files to create / modify

**New:**
- `backend/python/migrations/034_ecri_objections.sql`
- `backend/python/migrations/035_ecri_approval_limits.sql`
- `backend/python/web/routes/ecri.py` — new route handlers (add to existing blueprint)
- `backend/python/web/templates/ecri/objections.html` — new dashboard page
- `backend/python/web/templates/admin/ecri_limits.html` — admin limits editor
- `backend/python/common/models.py` — `ECRIObjection` model + add fields on `Role`

**Modified:**
- `backend/python/web/routes/ecri.py` — add objection endpoints, reuse `_execute_batch_worker` SOAP pattern for apply
- `backend/python/web/templates/ecri/batch_review.html` — per-row objection button + modal
- `backend/python/web/auth/decorators.py` — new permission decorators `can_create_ecri_objection`, `can_approve_ecri_objection`, `can_manage_ecri_approval_limits`
- `backend/python/web/models/role.py` — 3 new columns + helper `max_ecri_reduction()`
- `backend/python/web/utils/audit.py` — new enum members
- `backend/python/config/ecri.yaml` — `objection_reasons` block

## 10. Implementation order

1. **Migrations** (034, 035) — apply on esa_pbi + esa_backend respectively.
2. **Models** — `ECRIObjection`, `Role` extensions.
3. **Decorators + permissions** — `can_create_ecri_objection` etc., seeded against existing roles.
4. **Backend routes** — create/approve/reject/apply/cancel + list/filter endpoints.
5. **Objection modal** in batch_review.html.
6. **Objections dashboard** (`/ecri/objections`).
7. **Admin limits page** (`/admin/ecri-approval-limits`).
8. **Analytics hook-up** — add modification counts to batch analytics.
9. **Audit events + deploy.**

Rough size: ~800-1000 lines across backend + templates. Safely deliverable in one build pass if the spec is locked.

## 11. Open questions (need your answers before build)

1. **Limit basis** — per-role (simpler), per-user (more flexible), or both (per-user with role fallback)? Recommend: **per-role**, most-permissive-wins when user has multiple roles.
2. **Limit semantics** — use both pct AND abs (current spec), or only one? Both protects against edge cases (small rent but big %, or large rent but small %). Recommend: **both, with ALL conditions required** (pct ≤ max AND abs ≤ max).
3. **Objection on un-pushed ledgers** — should we allow objections on `pending` rows too (stop the push), or only `success` rows (post-fact)? Recommend v1: **only post-push**, matches the stated workflow. v2 can add pre-push.
4. **One active objection per ledger, or many?** — tenant might negotiate twice. Recommend: **only one active (status in ('pending_approval','approved','applied')) at a time per ledger; superseded ones are cancelled.**
5. **Effective date override** — do you actually want this in v1, or just %/rent? Adds complexity because the stale-batch guard needs to re-evaluate. Recommend: **%/rent only in v1**, effective date override in v2.
6. **Approver notifications** — email? In-app toast? Nothing (approver checks dashboard)? Recommend: **dashboard only in v1**; add email via existing Azure Foundry integration in v2.
7. **Rejection workflow** — can rejected objections be re-raised? Or is rejection terminal? Recommend: **terminal; staff can raise a new one with different values**.
8. **Default limits** — what numbers to seed? Need guidance per role. E.g. Site Manager = 2pct / SGD 15; Area Manager = 5pct / SGD 50; RVP = unlimited. Recommend: start all at **0/0 (everything needs approval)** and let you configure through the admin UI.
9. **Reason codes final list** — is the list in §5 complete or should I add/remove items?

## 12. Out of scope (v2 backlog)

- Bulk objections (CSV upload).
- Email notifications to approvers / tenants.
- Effective-date-only objections.
- Pre-push objections (block a ledger from being sent to SMD).
- Objection templates (canned reasons with pre-filled pcts).
- Tenant-facing letter template merge.
- Monthly objection summary report / cron digest.

---

**Next step:** once you answer the 9 open questions (especially #1, #2, #5, #8), I can dispatch a build team to implement end-to-end. Estimated effort: half a day of async agent time.
