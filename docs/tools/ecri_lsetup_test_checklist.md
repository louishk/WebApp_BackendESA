# ECRI LSETUP Dry-Run Checklist

Use this checklist before any production-site batch push to validate the full 6-stage workflow against the test site (LSETUP, `SiteID=27525`, `SiteCode=LSETUP`).

> Last validated 2026-04-20 — all stages pass, 5 SOAP pushes landed in SiteLink correctly, teardown confirmed.

## Prerequisites

- `ccws_ledgers` recent for LSETUP:
  `SELECT MAX(extract_date) FROM ccws_ledgers WHERE "SiteID"=27525;`
  If stale, run:
  ```bash
  ssh esa_bk_admin@20.6.132.108 sudo bash -c "cd /var/www/backend/backend/python && source venv/bin/activate && set -a && source .env && set +a && PYTHONPATH=/var/www/backend/backend/python python3 datalayer/tenant_ledger_charges_to_sql.py --mode incremental --location LSETUP"
  ```
- `backend/python/config/ecri.yaml` → `lsetup_allowlist_site_ids: [27525]`
- Tester has `ecri_admin` role (all 8 ECRI permissions).

## Operational params reminder

One SOAP call per ledger, `CallCenterWs.ScheduleTenantRateChange` (v1):

| SOAP arg | Source |
|----------|--------|
| `sLocationCode` | `SiteInfo.SiteCode` |
| `LedgerID` (int) | `ECRIBatchLedger.ledger_id` |
| `dcNewRate` (decimal 2dp) | rounded `new_rent` in native currency |
| `dScheduledChange` | `effective_date.strftime('%Y-%m-%dT00:00:00')` |

## Stage-by-stage test

### 1 — Build
1. `/ecri/eligibility` → pick site LSETUP → Run Eligibility Check. Expect 700+ eligible tenants.
2. Tick 5–10 rows → **Create Batch from Selection**.
3. On Create: set target % (e.g. 8%) or enable control groups → Submit.
4. Land on `/ecri/batch/<id>/review`, status = `draft`.

### 2 — Site Review
5. Click **Submit for Site Review** → status → `site_review`, deadline stamped (earliest `notice_date − 3 days`).
6. As an ops user (`ecri_ops` with `allowed_site_ids=[27525]`), click **Request Excl.** on 1–2 rows, pick reason, submit.
7. Switch back to admin.

### 3 — Revenue Review
8. Click **Close Site Review** → status → `rev_review`.
9. **Approve** or **Reject** each requested row. Approved rows skip at push; rejected rows still push.
10. **Finalize Batch** (enabled once zero requests pending) → status → `rev_approved`.

### 4 — Push Live
11. **Execute — Push to SiteLink** → type `PUSH N LIVE` → progress modal polls every 2s.
12. On completion:
    - approved-exclusion rows → `api_status='skipped'`
    - pushed rows → `api_status='success'` with SMD `Ret_Code`/`Ret_Msg`
    - SMD-rejected rows → `api_status='failed'`

### 5 — SiteLink verification
13. Open SiteLink for LSETUP. For each `success` row, open the tenant → confirm scheduled rent = `dcNewRate`, effective date = `dScheduledChange`.
14. Screenshot for the audit trail.

### 6 — Objections
15. `/ecri/objections` → search by tenant name → find the pushed ledger → **Raise Objection**.
16. Set new % / new rent, pick reason, notes. Live approval indicator: **green** if within your user's `ecri_max_pct_reduction` / `ecri_max_abs_reduction`; **amber** otherwise.
17. Submit. If auto-approved → **Apply** → SMD re-push (overwrites the previous schedule).
18. Re-check SiteLink to confirm the modified schedule.

### 7 — Analytics
19. `/ecri/batch/<id>/analytics` → confirm:
    - **Plan vs Delivered** card reflects any applied objection (leakage by reason/staff)
    - **2026 Revenue Contribution** card shows month-by-month forecast through EOY
20. `/ecri/` dashboard → **ECRI Impact on 2026 Book** includes this batch's contribution in the portfolio total.

### 8 — Teardown
21. `DELETE FROM ecri_batches WHERE batch_id='<id>'` — cascade drops ledgers/outcomes/objections.
22. SMD scheduled changes on LSETUP are NOT auto-cancelled. Harmless on a test site; manually cancel in SiteLink if you want a clean state.

## Partial retest (one ledger)

```sql
UPDATE ecri_batch_ledgers
   SET api_status='pending', api_response=NULL, api_executed_at=NULL,
       exclusion_status='none', exclusion_reason_code=NULL, exclusion_notes=NULL,
       exclusion_requested_by=NULL, exclusion_requested_at=NULL,
       exclusion_decided_by=NULL, exclusion_decided_at=NULL, exclusion_decision_notes=NULL
 WHERE batch_id='<uuid>' AND id IN (<ids>);
-- flip batch back if needed:
UPDATE ecri_batches SET status='site_review' WHERE batch_id='<uuid>';
```

## Failure modes (watched)

- **HTTP 500 from SMD on push** → usually date format wrong (must be `YYYY-MM-DDT00:00:00`).
- **`Ret_Code=-1 "Error retrieving ledger data"`** → ledger is delinquent / locked in SMD; site ops must clear balance first.
- **Stale `ccws_ledgers`** → buckets wrong, `dPaidThru` out of date; always check `MAX(extract_date)` before push.
- **Objection stuck in `pending_approval`** → no approver has high enough `ecri_max_*_reduction` to clear; escalate.
