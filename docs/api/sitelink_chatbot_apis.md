# SiteLink APIs — Pand.ai Chatbot

Reference list of SiteLink (SMD CallCenterWs) SOAP operations the Pand.ai chatbot calls. Captured from vendor. Used to scope the chatbot/booking-engine middleware surface (branch `claude/sitelink-api-middleware-*`).

Ultimately the middleware must cover all of them. Current priority is **inventory availability + discount + discount restriction** (rest tracked in parallel).

## Operations

| # | Operation | Purpose | Priority |
|---|-----------|---------|----------|
| 1 | `UnitsInformation_v3` | Retrieve details for all units at a given facility | **P0 — inventory avail** |
| 2 | `UnitsInformationByUnitID` | Retrieve details for a particular unit | **P0 — inventory avail** |
| 3 | `DiscountPlansRetrieve` | Find discounts at a specific facility | **P0 — discount** |
| 4 | `MoveInCostRetrieveWithDiscount_v2` | Retrieve the price for a particular unit (with discount restriction applied) | **P0 — discount restriction** |
| 5 | `TenantNewDetailed_v3` | Create a new tenant | P1 |
| 6 | `TenantUpdate_v3` | Update the details for a tenant | P1 |
| 7 | `TenantListDetailed_v2` | List tenants at a particular facility (find the user's tenant, if any) | P1 |
| 8 | `ReservationNewWithSource_v5` | Create reservations | P1 |
| 9 | `LedgersByTenantID` | List the units currently rented by a tenant | P1 |
| 10 | `InsuranceCoverageRetrieve` | Retrieve insurance coverages available at a facility | P1 |
| 11 | `TenantListCompleteMovedInTenantsOnly` | List all active tenants at a facility (collections) | P2 |
| 12 | `PaidThroughDateByLedgerID` | Check paid-through date for a ledger (collections) | P2 |
| 13 | `LedgerStatementByLedgerID` | Check ledger details (collections) | P2 |
| 14 | `PaymentSimple` | Payment processing | P2 |

## Notes

- Chatbot flow groups: **browse/price** (1–4), **tenant + booking** (5–10), **collections + payment** (11–14).
- Existing repo coverage (grep of `backend/python/`):
  - Implemented/called somewhere: `UnitsInformation_v3`, `DiscountPlansRetrieve`, `TenantNewDetailed_v3`, `LedgersByTenantID_v3`, `InsuranceCoverageRetrieve_V2/V3`, `PaidThroughDateByLedgerID`, `LedgerStatementByLedgerID`, `PaymentSimpleCash/Check/BankTransferWithSource`.
  - Gaps to confirm for chatbot middleware: `UnitsInformationByUnitID`, `TenantUpdate_v3`, `TenantListDetailed_v2`, `ReservationNewWithSource_v5`, `MoveInCostRetrieveWithDiscount_v2`, `TenantListCompleteMovedInTenantsOnly`, plain `PaymentSimple` (vs the `*WithSource` variants already used).
- Version drift watch: chatbot list uses base `PaymentSimple` and `LedgersByTenantID`; repo currently uses `PaymentSimple*WithSource` and `LedgersByTenantID_v3`. Confirm which variant Pand.ai actually hits before finalising middleware contracts.
- Related design docs: `docs/superpowers/specs/2026-04-16-soap-audit-fix-and-booking-engine-design.md`, `docs/superpowers/plans/2026-04-16-soap-middleware-remediation.md`.
