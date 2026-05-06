-- Migration 030: vw_ecri_eligible_ledgers view
-- Target database: esa_pbi
--
-- Context: ECRI tool was querying the legacy cc_ledgers pipeline (last
-- extract 2026-03-13, stale). The live pipeline is ccws_ledgers, but it
-- drops several columns the ECRI eligibility query depends on. The
-- "pricing deployment" script (scripts/pricing_recalibration.py) already
-- sources those missing fields from rentroll_enriched joined to
-- ccws_ledgers. This view wraps that join so ecri.py becomes a thin
-- consumer and the join logic lives in one place.
--
-- Semantic notes:
--   - ccws_ledgers is active-only: a ledger that has moved out disappears.
--     We expose dMovedOut as NULL for API compatibility with the old query.
--   - bExcludeFromRevenueMgmt does not exist in ccws or rentroll_enriched.
--     Hardcoded FALSE (in cc_ledgers only 6/33k rows were TRUE, appears dead).
--   - rentroll_enriched is a snapshot view; we pin to its MAX(extract_date).

BEGIN;

DROP VIEW IF EXISTS vw_ecri_eligible_ledgers;

CREATE VIEW vw_ecri_eligible_ledgers AS
WITH rr_latest AS (
    SELECT *
    FROM rentroll_enriched
    WHERE extract_date = (SELECT MAX(extract_date) FROM rentroll_enriched)
)
SELECT
    cw."LedgerID",
    cw."SiteID",
    cw."TenantID",
    cw."UnitID",
    cw."TenantName",
    cw."dMovedIn",
    NULL::timestamp                     AS "dMovedOut",
    cw."dSchedOut",
    rr."dRentLastChanged",
    rr."dSchedRentStrt",
    cw."dcRent",
    rr."dcSchedRent",
    FALSE                               AS "bExcludeFromRevenueMgmt",
    rr."sUnit",
    rr."sTypeName",
    rr."dcStdRate",
    cw.extract_date                     AS ccws_extract_date,
    rr.extract_date                     AS rr_extract_date
FROM ccws_ledgers cw
LEFT JOIN rr_latest rr
       ON rr."SiteID"   = cw."SiteID"
      AND rr."UnitID"   = cw."UnitID"
      AND rr."LedgerID" = cw."LedgerID";

COMMIT;
