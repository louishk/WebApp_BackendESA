-- Migration 031: Add currency column to ecri_batch_ledgers
-- Target database: esa_pbi
--
-- Context: ECRI batches can mix tenants from SG/MY/KR (soon HK), each in
-- their own currency. The batch review UI needs to normalize to SGD, but
-- per-row native currency must be preserved for audit and for the SOAP
-- push (SMD expects native-currency amounts). This column stores the
-- per-ledger native currency; the API response computes SGD on the fly
-- using fx_rates.

BEGIN;

ALTER TABLE ecri_batch_ledgers
    ADD COLUMN IF NOT EXISTS currency VARCHAR(3);

-- Backfill Batch1 (and any other existing rows) from siteinfo.Country
UPDATE ecri_batch_ledgers bl
SET currency = CASE si."Country"
        WHEN 'Singapore'   THEN 'SGD'
        WHEN 'Malaysia'    THEN 'MYR'
        WHEN 'South Korea' THEN 'KRW'
        WHEN 'Hong Kong'   THEN 'HKD'
        ELSE 'SGD'  -- safe default
    END
FROM siteinfo si
WHERE bl.site_id = si."SiteID"
  AND bl.currency IS NULL;

COMMIT;
