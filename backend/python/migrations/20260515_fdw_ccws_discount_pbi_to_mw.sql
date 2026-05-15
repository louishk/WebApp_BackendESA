-- Expose esa_middleware.ccws_discount as a foreign table inside esa_pbi
--
-- Target database: esa_pbi
--
-- Context: ccws_discount is canonically owned by esa_middleware (the
-- CcwsDiscountPipeline writes there). The PBI copy was a stale orphan,
-- 100 rows behind on 2026-05-15. Rather than drop the PBI reference
-- (would break vw_ecri_advance_eligible_ledgers used by ecri.py:1162),
-- we replace the physical PBI table with a postgres_fdw foreign table
-- pointing to esa_middleware. The view body is unchanged.
--
-- Prerequisites:
--   1. postgres_fdw extension allow-listed in azure.extensions server param
--   2. CREATE EXTENSION postgres_fdw; (run on both DBs)
--
-- Idempotent — safe to re-run.

BEGIN;

-- 1. Foreign server pointing to esa_middleware (same Azure Flex host)
CREATE SERVER IF NOT EXISTS esa_middleware_fdw
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host 'esapbi.postgres.database.azure.com',
        dbname 'esa_middleware',
        port '5432',
        sslmode 'require',
        fetch_size '1000'
    );

-- 2. User mapping. Replace :mw_password at apply time with the esa_pbi_admin
--    password on esa_middleware (identical to PBI_DB_PASSWORD in this deployment).
-- CREATE USER MAPPING IF NOT EXISTS FOR esa_pbi_admin
--     SERVER esa_middleware_fdw
--     OPTIONS (user 'esa_pbi_admin', password :'mw_password');

-- 3. Import the ccws_discount table as a foreign table.
--    Skip if already present.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.foreign_tables
        WHERE foreign_table_name = 'ccws_discount'
    ) THEN
        IMPORT FOREIGN SCHEMA public LIMIT TO (ccws_discount)
            FROM SERVER esa_middleware_fdw INTO public;
    END IF;
END
$$;

COMMIT;

-- After verification, drop any legacy physical table:
--   DROP TABLE IF EXISTS ccws_discount_legacy_pbi_20260515 CASCADE;
--   -- CASCADE will drop dependent views; recreate vw_ecri_advance_eligible_ledgers
--   -- by re-running migration 055 (the view body references ccws_discount by
--   -- name, so it rebinds to the foreign table).
