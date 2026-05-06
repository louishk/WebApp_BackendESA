-- Migration 033: vw_ecri_eligible_ledgers v2
-- Target database: esa_pbi
--
-- Adds dPaidThru and dAnniv columns from ccws_ledgers so the ECRI batch
-- creation route can compute billing-cycle-aware effective dates without
-- a separate query.

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
    rr.extract_date                     AS rr_extract_date,
    cw."dPaidThru",
    cw."dAnniv"
FROM ccws_ledgers cw
LEFT JOIN rr_latest rr
       ON rr."SiteID"   = cw."SiteID"
      AND rr."UnitID"   = cw."UnitID"
      AND rr."LedgerID" = cw."LedgerID";

COMMIT;
