-- 053_ccws_site_billing_config.sql
--
-- Per-site proration / billing mode configuration for the MoveInCost
-- internal calculator.
--
-- Source: extracted from MoveInCostRetrieveWithDiscount_v4 SOAP response
-- (one call per site on any active unit). Sync respects manual overrides
-- (does not overwrite a row where overridden_by IS NOT NULL).
--
-- Two billing modes supported:
-- * 1st-of-month (bAnnivDateLeasing=false): prorate partial 1st month
--   - Day 1                      → full month
--   - Day 2 to X                 → prorated current month
--   - Day X+1 to end-of-month    → prorated current + full next month
--   where X = i_day_strt_prorate_plus_next (typically 17 on LSETUP)
-- * Anniversary (bAnnivDateLeasing=true): full month from move-in date

CREATE TABLE IF NOT EXISTS ccws_site_billing_config (
    id SERIAL PRIMARY KEY,
    "SiteCode" VARCHAR(20) NOT NULL UNIQUE,
    "SiteID" INTEGER,

    -- Proration / billing mode flags (mirror SOAP field names)
    b_anniv_date_leasing BOOLEAN NOT NULL DEFAULT FALSE,
    i_day_strt_prorating INTEGER NOT NULL DEFAULT 1,
    i_day_strt_prorate_plus_next INTEGER NOT NULL DEFAULT 17,

    -- Audit / override tracking
    synced_from_soap_at TIMESTAMP,
    overridden_by VARCHAR(100),
    overridden_at TIMESTAMP,
    notes TEXT,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ccws_site_billing_config_code
    ON ccws_site_billing_config ("SiteCode");

-- If the original site_billing_config table exists from the prior
-- migration version, rename it (and any indexes) to preserve data.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'site_billing_config') THEN
        DROP TABLE ccws_site_billing_config;
        ALTER TABLE site_billing_config RENAME TO ccws_site_billing_config;
        ALTER INDEX IF EXISTS idx_site_billing_config_code
              RENAME TO idx_ccws_site_billing_config_code;
    END IF;
END $$;
