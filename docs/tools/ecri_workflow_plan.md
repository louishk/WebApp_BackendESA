# ECRI End-to-End Workflow Plan

> ⚠️ **Archived plan — see `ecri_tool.md` for the current-state operational reference.**
> Shipped system differs slightly: 6-stage workflow (`rev_review` added between site_review and rev_approved), per-user limits (not per-role), DB-editable reasons.
> Kept as historical planning context only.

Supersedes `ecri_objection_tracker_plan.md`. The objection tracker is one piece of a bigger 5-stage workflow you described:

> revenue select tenant → build a batch → ops review and highlight tenant to exclude with reason → revenue verify exclusion and finalize batch → rev push the batch to sitelink → once customer received the letter then objection will happen

Two approval systems run in this pipeline: **batch exclusions** (before push) and **objections** (after push). Both are covered here.

## 1. The 5 stages

| Stage | Status | Owner | What happens |
|-------|--------|-------|--------------|
| **1. Build** | `draft` | Revenue | Revenue (RM team) runs eligibility, picks tenants, sets target %, computes billing-cycle-aware effective/notice dates, drops them into `ecri_batch_ledgers`. Same as today. |
| **2. Site opt-out** | `site_review` | Ops (site-scoped) | Each site sees ONLY its own rows. Ops can request exclusion per row with a reason. Cannot change %, rent, or dates. |
| **3. Revenue finalize** | `site_review → rev_approved` | Revenue | Revenue sees all exclusion requests, approves/rejects each, signs off on the batch. Rejected exclusions stay in the batch (will be pushed). Approved exclusions are marked `skipped` and won't be pushed. |
| **4. Push** | `rev_approved → executing → executed` | Revenue | Existing async push via `ScheduleTenantRateChange`. Only rows where exclusion isn't approved get sent. |
| **5. Objection** | `executed` + per-row objections | Ops (site-scoped) / Revenue | Tenant calls in after receiving the letter, ops logs the modification. Auto-approved or routed to approver depending on role limits. Apply re-pushes to SMD. |

Cancel is valid at any stage up to `rev_approved`. A `cancelled` batch stays in the DB for audit but does nothing.

## 2. Status model

**Batch status CHECK:** `draft`, `site_review`, `rev_approved`, `executing`, `executed`, `cancelled` — migration `036_ecri_batch_status_expand.sql` adds `site_review` (already there per 002) and `rev_approved`.

**Transitions:**

```
draft ──(submit for site review)──► site_review
draft ──(cancel)──────────────────► cancelled

site_review ──(revenue finalizes)─► rev_approved
site_review ──(cancel)─────────────► cancelled

rev_approved ──(push live)─────────► executing
rev_approved ──(back to review)────► site_review    (optional, if revenue wants another round)

executing ──(worker finishes all)──► executed
executing ──(worker fails fatally)─► rev_approved   (back for retry)

executed — terminal, but objections can be added for success rows.
```

## 3. Data model changes

### `ecri_batch_ledgers` — per-row exclusion fields (new)

Migration `037_ecri_batch_ledger_exclusion.sql`:

```sql
ALTER TABLE ecri_batch_ledgers
    ADD COLUMN IF NOT EXISTS exclusion_status      VARCHAR(12) DEFAULT 'none'
        CHECK (exclusion_status IN ('none','requested','approved','rejected')),
    ADD COLUMN IF NOT EXISTS exclusion_reason_code VARCHAR(40),
    ADD COLUMN IF NOT EXISTS exclusion_notes       TEXT,
    ADD COLUMN IF NOT EXISTS exclusion_requested_by     INTEGER,
    ADD COLUMN IF NOT EXISTS exclusion_requested_at     TIMESTAMP,
    ADD COLUMN IF NOT EXISTS exclusion_decided_by       INTEGER,
    ADD COLUMN IF NOT EXISTS exclusion_decided_at       TIMESTAMP,
    ADD COLUMN IF NOT EXISTS exclusion_decision_notes   TEXT;
```

Execute logic: inside `_execute_batch_worker`, treat `exclusion_status='approved'` like the control-group skip (`api_status='skipped'`, `api_response={'reason': 'Excluded in site review', 'detail': ...}`).

### `ecri_objections` — post-push modifications (new table)

Same as the earlier objection plan. Migration `038_ecri_objections.sql`. Schema unchanged from what I proposed — see §6 below.

### Site-scoped users — new concept

Ops staff must only see their own site's rows. Today there's no site-scoping on users. Two options:

- **A. `users.allowed_site_ids INTEGER[]`** — simple array column. NULL or empty = all sites (superuser / revenue). Specific list = scoped.
- **B. Join table `user_sites(user_id, site_id)`** — more flexible, room for future metadata.

Recommend **A** for v1 (simpler, matches existing array idiom like `ecri_batches.site_ids`). Migration `039_users_allowed_sites.sql`.

Every scoped endpoint filters ledgers against `current_user.allowed_site_ids`. An empty/null array means "no restriction" so revenue sees everything.

### Per-user approval limits (new columns on `users`)

Rank-based authority lives at the user level, not the role level — so an RM's senior can have a higher reduction allowance than a junior RM even if they share the same role.

Migration `040_users_ecri_limits.sql`:

```sql
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS ecri_entitled BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS ecri_max_pct_reduction NUMERIC(5,2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ecri_max_abs_reduction NUMERIC(14,4) DEFAULT 0;
```

Default 0/0 = everything needs approval until admin configures the user.

**Approval escalation rule:** an approver can only approve an objection if their own `ecri_max_pct_reduction` AND `ecri_max_abs_reduction` are both ≥ the requested reduction. If not, the approver must escalate (UI shows "Requires higher authority"). This creates a natural rank chain without an explicit org-chart table.

### New permissions (boolean columns on `roles`)

| Permission | Who | Used by |
|------------|-----|---------|
| `can_create_ecri_batch` | Revenue | Stage 1 |
| `can_request_ecri_exclusion` | Ops (site-scoped) | Stage 2 |
| `can_finalize_ecri_batch` | Revenue | Stage 3 |
| `can_execute_ecri_batch` | Revenue | Stage 4 (existing `can_manage_ecri` becomes this) |
| `can_create_ecri_objection` | Ops (site-scoped) + Revenue | Stage 5 |
| `can_approve_ecri_objection` | Senior ops / Revenue | Stage 5 |
| `can_manage_ecri_approval_limits` | Admin | limits editor |

Backward-compat: existing `can_manage_ecri` stays as an alias for "can_execute_ecri_batch AND can_create_ecri_batch AND can_finalize_ecri_batch" so we don't break anything today.

## 4. UI surfaces

### 4.1 Existing `batch_review.html` — gets mode switching

Single template, behaviour depends on batch status + current user role:

- **`draft` + Revenue:** see everything, edit, cancel, "Submit for Site Review" button.
- **`site_review` + Ops (site-scoped):** see ONLY your site's rows. Per-row checkbox + reason dropdown + notes to Request Exclusion. "Submit My Requests" button. Filter/search within your site's rows.
- **`site_review` + Revenue:** see everything, grouped by site. Exclusion requests pending review highlighted. Per-request Approve / Reject. Bulk-approve-all-from-site button. "Finalize Batch" button (disabled until every request is decided).
- **`rev_approved` + Revenue:** read-only review with "Push Live" button. Back-to-site-review button if changes needed.
- **`executing`:** progress modal (existing).
- **`executed`:** per-row status + "Objection" icon per success row → opens objection modal.

A mode banner at the top of the page tells the user which stage they're in and what they can do.

### 4.2 New dashboards

- **`/ecri/` (existing dashboard)** — add per-stage batch counts for the current user's role. Ops sees "3 batches awaiting your review"; Revenue sees "2 batches ready to finalize, 1 executing".
- **`/ecri/objections`** — cross-batch objections table, filters (status/staff/site/date/reason), approve/reject/apply actions per row. Site-scoped for ops.
- **`/admin/ecri-approval-limits`** — role × limit matrix. Admin only.
- **`/admin/user-sites`** — (optional, v1 could be a DB update) user → allowed_site_ids editor.

## 5. Exclusion workflow detail (Stages 2–3)

### Reason codes (DB-stored, admin-editable)

Not YAML — two tables in esa_backend so the lists are editable via an admin UI without a deploy:

```sql
CREATE TABLE ecri_exclusion_reasons (
    id SERIAL PRIMARY KEY,
    code VARCHAR(40) NOT NULL UNIQUE,
    label VARCHAR(200) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER NOT NULL DEFAULT 100,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE ecri_objection_reasons (
    id SERIAL PRIMARY KEY,
    code VARCHAR(40) NOT NULL UNIQUE,
    label VARCHAR(200) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER NOT NULL DEFAULT 100,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

Seed the tables in the migration with the lists previously in §5/§6 YAML. Admin pages `/admin/ecri-exclusion-reasons` and `/admin/ecri-objection-reasons` provide CRUD (or fold into one combined "Reasons" page).

Seed exclusion reasons:
```
scheduled_moveout   — Tenant scheduled to move out
recent_signup       — Recent sign-up / recent rate change
hardship            — Financial hardship
vip_longterm        — VIP / long-term retention
dispute             — Active dispute / complaint
unit_issue          — Unit quality / access issue
commercial_contract — Commercial contract / special agreement
other               — Other (specify in notes)
```

### Ops submits exclusion request

`POST /ecri/api/batch/<batch_id>/exclusion`:
```json
{
  "ledger_row_id": 1732,
  "reason_code": "scheduled_moveout",
  "notes": "tenant confirmed move-out May 20 via email"
}
```

Sets `exclusion_status='requested'`, stamps `exclusion_requested_by` and `_at`. Ops can withdraw their own request (status back to 'none') while batch is still in site_review.

### Revenue reviews each request

`POST /ecri/api/exclusion/<row_id>/approve` / `.../reject`:
- Approve → `exclusion_status='approved'`, ledger will be skipped at push time.
- Reject → `exclusion_status='rejected'` with `exclusion_decision_notes`, ledger WILL be pushed.

`GET /ecri/api/batch/<batch_id>/exclusions` returns all pending+decided requests, grouped by site, for the Revenue reviewer.

### Finalize

`POST /ecri/api/batch/<batch_id>/finalize` — only allowed when ZERO requests have `exclusion_status='requested'` (everything decided). Flips batch to `rev_approved`. Audit entry.

## 6. Objection workflow (Stage 5)

Unchanged from the earlier plan. Key schema:

```sql
CREATE TABLE ecri_objections (
    id                BIGSERIAL PRIMARY KEY,
    batch_ledger_id   BIGINT NOT NULL REFERENCES ecri_batch_ledgers(id) ON DELETE CASCADE,
    batch_id          UUID NOT NULL,
    site_id           INTEGER NOT NULL,
    ledger_id         INTEGER NOT NULL,
    original_increase_pct NUMERIC(5,2) NOT NULL,
    original_new_rent     NUMERIC(14,4) NOT NULL,
    currency          VARCHAR(3) NOT NULL,
    new_increase_pct  NUMERIC(5,2),
    new_new_rent      NUMERIC(14,4),
    reason_code       VARCHAR(40) NOT NULL,
    reason_notes      TEXT,
    status            VARCHAR(20) NOT NULL DEFAULT 'pending_approval'
                      CHECK (status IN ('pending_approval','approved','rejected','applied','cancelled')),
    requires_approval BOOLEAN NOT NULL,
    raised_by_user_id INTEGER NOT NULL,
    raised_by_username VARCHAR(100) NOT NULL,
    raised_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    approver_user_id  INTEGER,
    approver_username VARCHAR(100),
    approved_at       TIMESTAMP,
    approval_notes    TEXT,
    applied_at        TIMESTAMP,
    applied_ret_code  VARCHAR(20),
    applied_ret_msg   TEXT
);
```

**Auto-approval logic** (computed at submit, user-based):

```
reduction_pct = original_increase_pct - new_increase_pct
reduction_abs = original_new_rent     - new_new_rent
user_max_pct  = current_user.ecri_max_pct_reduction
user_max_abs  = current_user.ecri_max_abs_reduction

if reduction_pct < 0 or reduction_abs < 0:     # bigger rate hike than planned
    requires_approval = True
elif reduction_pct <= user_max_pct and reduction_abs <= user_max_abs:
    requires_approval = False → status='approved' immediately
else:
    requires_approval = True  → status='pending_approval'
```

**Approver gate:** when an approver clicks Approve on a pending objection, the server re-checks that the approver's own limits encompass the requested reduction:

```
if approver.ecri_max_pct_reduction < reduction_pct
   or approver.ecri_max_abs_reduction < reduction_abs:
    → 403 "Your authority is below the requested reduction. Escalate to a higher-rank approver."
```

This is the rank chain — junior approvers only clear what they personally could have auto-approved.

Reason codes — same DB-table pattern as exclusions, admin-editable. Seed:
```
tenant_hardship     — Tenant hardship / financial difficulty
competitor_match    — Matched competitor rate
long_term_retention — Retention — long-term tenant
unit_issue          — Unit quality / access issue
billing_error       — Billing or calculation error
negotiated_discount — Negotiated discount
other               — Other (specify in notes)
```

Apply flow reuses `ScheduleTenantRateChange` — idempotent overwrite confirmed on Hillview.

## 7. Routes summary

### Web pages
- `GET /ecri/` — dashboard with stage-aware counts
- `GET /ecri/batch/<id>/review` — existing batch_review, mode-aware
- `GET /ecri/objections` — cross-batch objection dashboard
- `GET /admin/ecri-approval-limits` — role × limit matrix
- `GET /admin/users/<id>/sites` — allowed-site editor (or fold into existing user admin)

### Batch-workflow APIs
| Endpoint | Method | Permission | Purpose |
|----------|--------|------------|---------|
| `/ecri/api/batch` | POST | `create_ecri_batch` | Create draft (existing) |
| `/ecri/api/batch/<id>/submit-review` | POST | `create_ecri_batch` | draft → site_review |
| `/ecri/api/batch/<id>/exclusion` | POST | `request_ecri_exclusion` | Submit exclusion request |
| `/ecri/api/exclusion/<row_id>` | DELETE | owner | Withdraw exclusion request |
| `/ecri/api/exclusion/<row_id>/approve` | POST | `finalize_ecri_batch` | Approve request |
| `/ecri/api/exclusion/<row_id>/reject` | POST | `finalize_ecri_batch` | Reject request |
| `/ecri/api/batch/<id>/finalize` | POST | `finalize_ecri_batch` | site_review → rev_approved |
| `/ecri/api/batch/<id>/reopen-review` | POST | `finalize_ecri_batch` | rev_approved → site_review |
| `/ecri/api/batch/<id>/execute` | POST | `execute_ecri_batch` | rev_approved → executing (existing) |
| `/ecri/api/batch/<id>/progress` | GET | ECRI access | existing |
| `/ecri/api/batch/<id>/cancel` | POST | `create_ecri_batch` | Any non-terminal → cancelled (existing) |

### Objection APIs (Stage 5)
| Endpoint | Method | Permission |
|----------|--------|------------|
| `/ecri/api/objection` | POST | `create_ecri_objection` |
| `/ecri/api/objections` | GET | ECRI access (site-scoped for ops) |
| `/ecri/api/objection/<id>/approve` | POST | `approve_ecri_objection` |
| `/ecri/api/objection/<id>/reject` | POST | `approve_ecri_objection` |
| `/ecri/api/objection/<id>/apply` | POST | owner + status=approved |
| `/ecri/api/objection/<id>/cancel` | POST | owner |

### Admin
| Endpoint | Method | Permission |
|----------|--------|------------|
| `/admin/api/ecri-limits/<role_id>` | PUT | `manage_ecri_approval_limits` |
| `/admin/api/users/<user_id>/sites` | PUT | existing `manage_users` |

## 8. Integration with existing ECRI code

- **`api_create_batch`** — set `status='draft'`. No change needed.
- **`_execute_batch_worker`** — add filter `AND exclusion_status != 'approved'` on the ledger query; skipped exclusions should be marked in `api_response` explicitly so the analytics view distinguishes them from other skips.
- **`api_get_batch`** — add `exclusion_counts: {none, requested, approved, rejected}` and `pending_exclusion_count` to the summary; add per-row `exclusion_status` / `exclusion_reason_code` so the UI can badge rows.
- **Analytics (`/ecri/batch/<id>/analytics`)** — new section: "Site feedback" (exclusion counts by reason + site + staff) and "Post-push modifications" (objection counts / revenue impact).
- **`ecri_outcome_tracking.py`** — no change needed; objections feed the same `ecri_outcomes` table via the same cc_ledgers churn check.

## 9. Backward compatibility for Batch1

Batch1 was built and pushed without going through site_review/rev_approved. It's in `status='draft'` today with many rows already `success`. Don't force it through the new flow — just let it stay as-is. All new batches will use the full workflow.

Migration 037 sets `exclusion_status='none'` as default, so existing rows are untouched. Nothing breaks.

## 10. Audit events (add to `web/utils/audit.py`)

- `ECRI_BATCH_SUBMITTED_FOR_REVIEW`
- `ECRI_EXCLUSION_REQUESTED`
- `ECRI_EXCLUSION_WITHDRAWN`
- `ECRI_EXCLUSION_APPROVED`
- `ECRI_EXCLUSION_REJECTED`
- `ECRI_BATCH_FINALIZED`
- `ECRI_BATCH_REOPENED`
- `ECRI_OBJECTION_CREATED`
- `ECRI_OBJECTION_APPROVED`
- `ECRI_OBJECTION_REJECTED`
- `ECRI_OBJECTION_APPLIED`
- `ECRI_APPROVAL_LIMIT_CHANGED`
- `ECRI_USER_SITES_CHANGED`

## 11. Implementation order

1. **Migrations** 036 (batch status), 037 (exclusion cols), 039 (user allowed_sites), 040 (role limits) on esa_pbi + esa_backend.
2. **Models** — extend `ECRIBatchLedger`, `User`, `Role`. Add `ECRIObjection`.
3. **Decorators + permissions** — seed the 7 new permission flags on existing roles (most permissive for existing `can_manage_ecri` holders).
4. **Exclusion API** — 7 endpoints. Reuse patterns from existing `api_*_batch` routes.
5. **Batch_review template — mode switching** — the biggest UI change. Banner + conditional per-row controls + ops/revenue variants.
6. **Stage-transition endpoints** — submit-review, finalize, reopen-review.
7. **Migrations** 038 (objections table).
8. **Objection API + modal** on batch_review + `/ecri/objections` dashboard.
9. **Admin limits + user-sites pages.**
10. **Analytics section** for exclusions + objections.
11. **Audit events + deploy.**

Rough size: ~1800-2200 lines across backend + templates. A 1-day async agent build (with good spec).

## 12. Open questions — answer before build

**Workflow**
1. **Can ops modify % or rent during site review, or only exclude?** My read: only exclude. Modifications come post-push via objections. Confirm.
2. **Can Revenue add new tenants to a batch that's in `site_review`?** Probably not — the eligibility gate is the source of truth. If new tenants surface, they go in the next batch. Confirm.
3. **Deadline / auto-advance for site review?** If sites don't respond in N days, does the batch auto-advance? Or does it block indefinitely? Recommend: no auto-advance; Revenue can force-finalize a batch with unresponsive sites (audit flags it).
4. **Reopening `rev_approved` back to `site_review`** — allow or not? Useful if Revenue spots a missed issue. Recommend: allow, logs an audit event.

**Site scoping**
5. **Do users already have site assignments anywhere** (SugarCRM, HR system), or do we seed `users.allowed_site_ids` from scratch?
6. **Revenue users** — always see all sites (empty/null array)?

**Permissions**
7. **Role defaults** — who currently holds `can_manage_ecri`? They get split into the 3 new perms (create/finalize/execute). Is the split 1:1:1 or should any of them narrow?
8. **Approval limits starting numbers** — 0/0 everywhere at v1, configure via admin UI?

**Objection workflow**
9. **One active objection per ledger, or many?** Recommend: one at a time (new one supersedes/cancels the previous pending one).
10. **Reject objection = terminal?** Recommend: yes. Staff raises a new one if needed.
11. **Increase (bigger hike) via objection** — allowed? Recommend: allowed but always requires approval.

**UI**
12. **Should the objection modal allow adjusting `effective_date` too**, or only rate? Recommend: v1 rate only; v2 adds date.
13. **Email notifications to approvers?** Recommend: v1 dashboard only; v2 adds email via Azure Foundry.

**Reason codes**
14. **Exclusion reason list in §5 final**, or want to add/remove items?
15. **Objection reason list in §6 final**?

---

**Next step:** confirm/answer the 15 questions (especially #1–#8). Once locked, I'll dispatch one build agent to knock this out end-to-end in a single pass. If you want, I can also draft a shorter "ops onboarding" doc for the site reviewers once the UI lands.
