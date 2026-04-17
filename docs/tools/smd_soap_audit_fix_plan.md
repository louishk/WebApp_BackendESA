# SMD SOAP Audit — Remediation Plan

**Status:** Generated 2026-04-16 after a full audit of `backend/python/web/routes/` against the live SMD WSDLs (`CallCenterWs.asmx`, `ReportingWs.asmx`). Triggered by ECRI push failures traced to a non-existent SOAP operation (`ScheduleRentIncrease`) and param name drift.

**Scope:** `tenants.py`, `reservations.py`, `soap_reports.py`, `billing.py`. **ECRI is already fully patched** — see `backend/python/web/routes/ecri.py` `api_execute_batch` for the reference implementation (v1 operation, correct param names, `Ret_Code == -1` → HTTP 502 guard, payload logged to `api_response`). **Does not affect ECRI Batch1 operations.**

---

## Root Cause Pattern

SMD's CallCenterWs silently accepts SOAP requests with unknown or mistyped param names and returns `Ret_Code=-1 Ret_Msg='Error retrieving ledger data from the server'` (or similar) in a 200 OK response. Python routes without a `Ret_Code` guard report success to the caller while SMD did nothing. This is how the ECRI bug survived in prod for so long.

**Every fix in this plan needs two things:**
1. Param names/types/date formats that match the WSDL exactly.
2. A `Ret_Code == -1` (or `<= 0`) guard after parsing the `RT` / result tag — log the real `Ret_Msg`, return HTTP 502 with a generic error + detail.

**WSDL sources (source of truth — do NOT rely on code comments):**
- `https://api.smdservers.net/CCWs_3.5/CallCenterWs.asmx?WSDL`
- `https://api.smdservers.net/CCWs_3.5/ReportingWs.asmx?WSDL`

**Non-destructive verification pattern** (use this before shipping any fix):

```python
# Run from backend/python/ — loads real creds from the vault
from common.config import DataLayerConfig
import requests

cfg = DataLayerConfig.from_env().soap
url = cfg.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
corp_user = f"{cfg.corp_user}:::{cfg.api_key}"
ns = "http://tempuri.org/CallCenterWs/CallCenterWs"

def try_call(op, params):
    param_xml = "".join(
        f"      <{k}>{str(v).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')}</{k}>\n"
        for k, v in params.items()
    )
    env = f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <{op} xmlns="{ns}">
{param_xml}    </{op}>
  </soap:Body>
</soap:Envelope>'''
    r = requests.post(url, data=env.encode('utf-8'),
                      headers={"Content-Type":"text/xml; charset=utf-8",
                               "SOAPAction": f"{ns}/{op}"}, timeout=60)
    print(r.status_code, r.text[:2000])
```

**Never fire destructive operations (writes/moves/payments/refunds/charges) during verification.** Trust the WSDL + param diff for those.

---

## Part 1 — `tenants.py` (✅ DONE — needs post-deploy verification)

All four bugs were patched and deployed on 2026-04-15 via the fixer agent. Verification is still needed on real data.

| # | file:line | operation | status | verification needed |
|---|-----------|-----------|--------|----------------------|
| 1 | `tenants.py:~601` | `ScheduleTenantRateChange` (downgraded from v2) | ✅ fixed | Run a dummy rate-change against an LSETUP ledger, verify in SiteLink |
| 2 | `tenants.py:~335` | `TenantNotesRetrieve` | ✅ fixed (now requires `ledger_id` query param) | Call with a known ledger_id, confirm notes return |
| 3 | `tenants.py:~417` | `TenantNoteInsert_v2` | ✅ fixed (now requires `ledger_id` body field) | Insert a test note against LSETUP ledger |
| 4 | `tenants.py:67` | `TenantSearchDetailed` | ✅ fixed (filter param names swapped) | Search a populated site (Jurong L001 or Hillview L025) with a known tenant name; confirm filter actually narrows the result |

### Post-deploy verification checklist

- [ ] Bug 1 — verify rate change on an LSETUP ledger through the web route (not via ECRI). Confirm SMD returns `Ret_Code > 0` and the scheduled change appears in SiteLink.
- [ ] Bug 2 / 3 — any existing API consumers calling `/api/tenants/<id>/notes` or posting notes will break until they're updated to pass `ledger_id`. Audit consumers before declaring done.
- [ ] Bug 4 — **potential privacy finding** from the audit: the broken filter probably meant searches returned *all* tenants at a site. After the fix, run an audit query to confirm filters now narrow results. If any consumer was relying on the "return all" behavior, they need a different endpoint.

---

## Part 2 — `billing.py` (⛔ 25 of 25 call sites broken)

Triaged into six categories. **Do not bulk-fix.** Some categories are mechanical; others need route-input changes and design review.

### Pre-requisite: usage triage

Before touching any Category A-E code, confirm which billing.py endpoints are actually called:

1. Grep the Jinja templates and any JS for `/api/billing/...` fetches.
2. Check MCP tool definitions (`mcp_esa/`) for any tool that wraps these routes.
3. Check external API consumers (API keys + scopes) — any script or integration using `billing:*` scopes.
4. Check `api_statistics` / `external_api_statistics` tables for call counts in the last 90 days.

**If an endpoint has zero recent calls**, schedule it for deletion instead of repair — the maintenance cost of a broken-but-unused route is high.

### Category F — Read-only name drift (5 sites, low-risk quick wins)

One-line renames. No semantic change, no new inputs. Safe to ship without contract changes.

| file:line | operation | change |
|-----------|-----------|--------|
| `billing.py:338` | `ChargesAllByLedgerID` | `iLedgerID` → `ledgerId` |
| `billing.py:537` | `LedgersByTenantID_v3` | `iTenantID` → `sTenantID` |
| `billing.py:741` | `ChargesAndPaymentsByLedgerID` | `iLedgerID` → `sLedgerID` |
| `billing.py:791` | `LedgerStatementByLedgerID` | `iLedgerID` → `sLedgerID` |
| `billing.py:1406` | `PaymentsByLedgerID` | `iLedgerID` → `sLedgerID` |

**Fix plan:** single PR, 5 one-line edits, run direct SOAP diagnostic (read-only) on each op against a known Hillview ledger, confirm non-empty result. Deploy.

### Category E — Date-format + minor contract fixes (3 sites)

| file:line | operation | fix | risk |
|-----------|-----------|-----|------|
| `billing.py:1802` | `ScheduleMoveOut` | `dScheduledOut` is currently `"YYYY-MM-DD"` — change to `f"{date_str}T00:00:00"`. Identical to the ECRI fix. | Medium — destructive op, test on LSETUP first |
| `billing.py:1739` | `TenantInvoicesByTenantID` | Rename `iTenantID` → `sTenantIDsCommaDelimited`, add required `dDateStart` / `dDateEnd` dateTime params (route needs two new query params, both defaulted to a sensible range like "last 90 days"). | Low — read-only, but missing fields cause HTTP 500 so it's been broken |
| `billing.py:1877` | `InsuranceCoverageAddToLedger` | Wrong entity: `iLedgerID` → `TenantID` + `UnitID`; rename `iInsuranceCoverageID` → `InsuranceCoverageID`; add required `sPolicyNumber` + `dStartDate` (both new route inputs). | High — schedules real insurance attachment, test on LSETUP, design review for new inputs |

### Category B — Charge management (3 sites)

| file:line | operation | fix | blocker |
|-----------|-----------|-----|---------|
| `billing.py:115` | `ChargeAddToLedger` | Rename: `iLedgerID`→`LedgerID`, `iChargeDescriptionID`→`ChargeDescID`, `dcAmount`→`dcAmtPreTax`. Remove phantom `sComment`. | None — straightforward |
| `billing.py:200` | `RecurringChargeAddToLedger_v1` | Rename as above; add **required** `dcRecurringRateAmt` (currently missing — the route input needs a new field `recurring_rate_amount`); rename `iFrequency`→`iQty`. Remove phantom `dStart`. | **Contract change** — caller must supply recurring rate separately from initial charge |
| `billing.py:396` | `ChargePriceUpdate` | Rename `iChargeID`→`chargeId`, `dcNewPrice`→`amount`. **Add missing required `ledgerId`** (route has no way to obtain it today — needs route-input change). | **Contract change** |

### Category D — Credit / discount / transfer (3 sites)

| file:line | operation | fix | blocker |
|-----------|-----------|-----|---------|
| `billing.py:469` | `ApplyCredit` | Rename `iLedgerID`→`ledgerId`, `dcAmount`→`amount`, `sComment`→`creditReason`. **Add missing required `chargeId`** (new route input). | **Contract change** |
| `billing.py:849` | `LedgerTransferToNewTenant` | Rename `iLedgerID`→`LedgerID`, `iNewTenantID`→`TenantID`. Add `Ret_Code` guard. | None |
| `billing.py:921` | `RemoveDiscountFromLedger` | Rename `iLedgerID`→`LedgerID`, `iDiscountPlanID`→`ConcessionID`. **Add missing required numeric `SiteID`** (different from `sLocationCode` — this is the internal SMD site id; need a lookup via `siteinfo.SiteID`). | **Contract change** — route needs to resolve `SiteID` from `site_code`, similar to ECRI's pattern |

### Category C — Future charges / account details (3 sites)

All three have **wrong entity models** (ledger-scoped code, tenant-scoped WSDL). These need design review — the route inputs don't match the domain.

| file:line | operation | required contract change |
|-----------|-----------|---------------------------|
| `billing.py:275` | `CustomerAccountsMakeFutureCharges` | Route takes `ledger_id`; SMD needs `iTenantID` + `iNumberOfFuturePeriods` (new input) + `dFutureDueDate` (rename from `dChargeThroughDate`). Map ledger→tenant via `ccws_ledgers.TenantID`. |
| `billing.py:583` | `CustomerAccountsBalanceDetails_v2` | Route takes `ledger_id`; SMD needs `iTenantID`. Same mapping. |
| `billing.py:633` | `CustomerAccountsBalanceDetailsWithDiscount` | Same entity swap + **add missing required `iUnitID` + `ConcessionPlanID`** (new inputs). |

### Category A — Payment / refund operations (8 sites) ⛔ HALT

**DO NOT FIX without design review and stakeholder sign-off.** These routes may represent one of three scenarios:

1. **Dead code** — never called from prod UI. Candidate for deletion rather than fix.
2. **Financial impact bug** — payments have been silently failing or recorded incorrectly. Urgent escalation to finance / ops.
3. **Silent redirect to another payment path** — the UI uses a different mechanism and billing.py was early scaffolding.

**Before any code change**, the usage triage in the pre-requisite section must answer: has `POST /api/billing/payment/*` or `/refund/*` been called in the last 90 days, and by what? Check `api_statistics` and `external_api_statistics` tables.

**Affected sites:**

| file:line | operation | class of brokenness |
|-----------|-----------|----------------------|
| `billing.py:1001` | `PaymentSimpleCashWithSource` | Wrong entity (ledger→tenant+unit); amount param name wrong; `sSource` string where `iSource` int required; phantom `bTestMode` |
| `billing.py:1081` | `PaymentSimpleCheckWithSource` | Same + check number field rename |
| `billing.py:1163` | `PaymentSimpleBankTransferWithSource` | Same + transfer number field rename |
| `billing.py:1244` | `PaymentSimpleWithSource_v3` | Same + missing credit-card fields |
| `billing.py:1339` | `PaymentMultipleWithSource_v3` | `sLedgerIDs` not in WSDL; entity swap; multiple renames |
| `billing.py:1516` | `RefundPaymentCash` | Wrong entity; phantom `dcRefund` (SMD derives refund amount from the payment record); `sComment`→`sReason` |
| `billing.py:1595` | `RefundPaymentCheck` | Same |
| `billing.py:1674` | `RefundPaymentCreditCard_v2` | **Missing 7 required card fields** (card number, CVV, expiry, billing name/address/zip, test mode). Route cannot possibly work as written. |

**Required before fix:**
- Confirm whether any of these endpoints are called from frontend / MCP / API consumers.
- If yes: schedule a design session to redesign route inputs, since several fields the WSDL demands are not collected by the current route.
- If no: delete them with a migration note.

### ✅ Only OK site in billing.py

| file:line | operation | status |
|-----------|-----------|--------|
| `billing.py:1937` | `InsuranceLedgerStatusByLedgerID` | Names match WSDL. No `Ret_Code` guard (acceptable for read-only, but adding one is trivial and recommended). |

---

## Suggested execution order

1. **✅ ECRI** — already done. Verify Hillview 5-tenant push works end-to-end (1 already landed via diagnostic — 4 remaining).
2. **✅ tenants.py** — already done. Post-deploy verification checklist above.
3. **Category F (billing)** — 5 one-line fixes, deploy in one PR. ~30 min of work.
4. **`ScheduleMoveOut` date fix** — one-line fix. ~10 min.
5. **Usage triage on billing.py** — grep frontend + MCP + `api_statistics` to classify live vs dead routes. **This is a blocker for categories A-E.**
6. **Category B / D non-contract-change fixes** (`LedgerTransferToNewTenant`, `ChargeAddToLedger`) — fix after triage.
7. **Category B / D contract-change fixes** (`RecurringChargeAddToLedger_v1`, `ChargePriceUpdate`, `ApplyCredit`, `RemoveDiscountFromLedger`) — fix after stakeholder agrees on new route inputs.
8. **Category C** — entity-model rewrite. Needs ccws_ledgers lookup helper (trivial — similar to how ecri.py resolves `site_codes`).
9. **Category E** — `TenantInvoicesByTenantID` and `InsuranceCoverageAddToLedger`.
10. **Category A** — design review required. Do not touch without sign-off.

---

## Acceptance criteria per fix

Every patched SOAP call site must satisfy all of:

- [ ] SOAP op name matches an `<s:element name="OP">` in the live WSDL exactly.
- [ ] All param names match the WSDL `<s:sequence>` (case-sensitive).
- [ ] All param types match (datetime is full `%Y-%m-%dT%H:%M:%S` format, not bare date; decimals are stringified to 2dp; ints are stringified; bools are "true"/"false").
- [ ] Every required (`minOccurs="1"`) param is present.
- [ ] `Ret_Code == -1` (or `<= 0`) path logs `Ret_Msg` to `logger.error` and returns HTTP 502 with generic message + detail.
- [ ] No raw exception strings leaked to the HTTP response body (CLAUDE.md rule).
- [ ] Non-destructive verification passed (direct SOAP diagnostic against LSETUP or a test ledger).
- [ ] `audit_log(...)` called on success path only, after SMD confirms `Ret_Code > 0`.

---

## Open questions for the user

1. Which billing.py endpoints are actually in use? (Blocker for Category A-E.)
2. For payment ops — is there an alternative payment path already in use, or has the existing billing.py route never been hit from prod?
3. For Category C — is it acceptable to resolve `ledger_id → tenant_id` via `ccws_ledgers` (live pipeline) inside the route, or do we want callers to pass both?
4. For `RecurringChargeAddToLedger_v1` — what's the business meaning of "recurring rate amount" vs "initial charge amount" in the Singapore storage context? Are they ever different?
5. For `RemoveDiscountFromLedger` — we need the internal numeric `SiteID`. Should the route resolve it via `siteinfo.SiteID` lookup, or should callers pass it directly?

---

## Tracking

- **ECRI fix commit / deploy:** 2026-04-15 (already live; Ledger 565680 scheduled change verified on SMD)
- **tenants.py fix commit / deploy:** 2026-04-15 (already live; post-deploy verification pending)
- **billing.py audit:** 2026-04-16 (this plan)
- **billing.py remediation:** not yet started — awaiting usage triage + stakeholder input

## Appendix — operations confirmed OK (no changes needed)

- `reservations.py` — all 11 audited call sites match WSDL (`TenantNewDetailed_v3`, `ReservationNewWithSource_v6`, `ReservationUpdate_v4`, `ReservationList_v3`, `ReservationNotesRetrieve`, `ReservationNoteInsert`, `ReservationFeeRetrieve`, `MoveInReservation_v6`, `MoveInWithDiscount_v7`, `InsuranceCoverageRetrieve_V3`, `InsuranceCoverageMinimumsRetrieve`). Move-in operations already have `int(ret_code) > 0` guards.
- `soap_reports.py` — datetime format is already `%Y-%m-%dT00:00:00`, all ReportingWs op names and param names match.
- `tenants.py` other operations (`TenantInfoByTenantID`, `TenantIDByUnitNameOrAccessCode`, `TenantListDetailed_v3`, `TenantUpdate_v3`) — verified OK in audit.
