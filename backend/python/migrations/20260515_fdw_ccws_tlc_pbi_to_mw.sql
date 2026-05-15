-- Migrate ccws_tenants / ccws_ledgers / ccws_charges to esa_middleware as canonical
-- and expose them in esa_pbi via postgres_fdw foreign tables.
--
-- This documents the steps that were executed on 2026-05-15. Pipeline
-- ccws_ledgers.py was flipped from get_engine('pbi') to 'middleware' in the
-- same commit. siteinfo references in that pipeline were also migrated to
-- mw_siteinfo, and rentroll reads were repointed to pbi_staging.rentroll
-- (a foreign table set up in middleware as part of this migration).
--
-- Order of operations (idempotent — safe to re-run; uses IF NOT EXISTS where
-- possible):
--
-- 1. On esa_middleware:
--    - Ensure postgres_fdw extension exists
--    - Create esa_pbi_fdw server + user mapping
--    - Create pbi_staging schema with foreign tables for the source PBI copies
--    - Create empty ccws_tenants/ledgers/charges in middleware (use SQLAlchemy
--      `Base.metadata.create_all` against the CcwsTenant/CcwsLedger/CcwsCharge
--      models in common/models.py — they were already defined)
--    - INSERT INTO ccws_tenants SELECT * FROM pbi_staging.ccws_tenants (explicit
--      column lists required because column order differs between the older PBI
--      copies and the current model)
--    - Repeat for ccws_ledgers, ccws_charges
--    - Import rentroll as a foreign table (required by Phase C of the pipeline)
--
-- 2. On esa_pbi:
--    - Drop dependent views (vw_ecri_eligible_ledgers, vw_ecri_advance_eligible_ledgers)
--    - Rename ccws_tenants/ledgers/charges → ccws_*_legacy_pbi_20260515
--    - IMPORT FOREIGN SCHEMA public LIMIT TO (ccws_tenants, ccws_ledgers, ccws_charges)
--      FROM SERVER esa_middleware_fdw INTO public
--    - Recreate the two views (they rebind to the foreign tables by name)
--    - Drop the legacy backup tables
--
-- After this migration, esa_pbi has zero physical ccws_* tables; all four
-- (tenants, ledgers, charges, discount) are foreign tables proxying to
-- esa_middleware. PBI views that reference these tables continue to work via
-- FDW pushdown.

-- =============================================================================
-- ON esa_middleware
-- =============================================================================

-- (Run with PGPASSWORD=$DB_PASSWORD against dbname=esa_middleware)

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER IF NOT EXISTS esa_pbi_fdw
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host 'esapbi.postgres.database.azure.com',
        dbname 'esa_pbi',
        port '5432',
        sslmode 'require',
        fetch_size '5000'
    );

-- CREATE USER MAPPING FOR esa_pbi_admin
--     SERVER esa_pbi_fdw
--     OPTIONS (user 'esa_pbi_admin', password '<DB_PASSWORD>');

CREATE SCHEMA IF NOT EXISTS pbi_staging;

-- Import foreign tables for the bulk-copy source AND for runtime cross-DB reads
-- (rentroll is read by ccws_ledgers pipeline Phase C)
DO $$
BEGIN
    -- only import if the corresponding foreign tables don't already exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.foreign_tables
        WHERE foreign_table_schema = 'pbi_staging' AND foreign_table_name = 'ccws_tenants'
    ) THEN
        EXECUTE 'IMPORT FOREIGN SCHEMA public LIMIT TO (ccws_tenants, ccws_ledgers, ccws_charges, rentroll) FROM SERVER esa_pbi_fdw INTO pbi_staging';
    END IF;
END $$;

-- =============================================================================
-- Bulk copy step (run ONCE after the empty target tables exist in middleware)
-- =============================================================================
-- Tables must already be created via SQLAlchemy `Base.metadata.create_all`
-- targeting the CcwsTenant / CcwsLedger / CcwsCharge models in common/models.py.

-- Generate INSERT statements with explicit column lists from information_schema:
--
--   SELECT 'INSERT INTO ' || table_name || ' (' || cols || ') SELECT ' ||
--          cols || ' FROM pbi_staging.' || table_name || ';'
--   FROM (
--     SELECT table_name,
--            string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position) AS cols
--     FROM information_schema.columns
--     WHERE table_schema = 'public'
--       AND table_name IN ('ccws_tenants','ccws_ledgers','ccws_charges')
--     GROUP BY table_name
--   ) x;
--
-- Then execute the resulting 3 INSERT statements. On 2026-05-15 this took:
--   ccws_charges  (2,045,614 rows) — 2:25
--   ccws_tenants  (   83,439 rows) — 0:06
--   ccws_ledgers  (   17,589 rows) — 0:03

-- =============================================================================
-- ON esa_pbi (separate connection)
-- =============================================================================

-- BEGIN;
-- DROP VIEW IF EXISTS vw_ecri_advance_eligible_ledgers;
-- DROP VIEW IF EXISTS vw_ecri_eligible_ledgers;
--
-- ALTER TABLE ccws_tenants RENAME TO ccws_tenants_legacy_pbi_20260515;
-- ALTER TABLE ccws_ledgers RENAME TO ccws_ledgers_legacy_pbi_20260515;
-- ALTER TABLE ccws_charges RENAME TO ccws_charges_legacy_pbi_20260515;
--
-- IMPORT FOREIGN SCHEMA public LIMIT TO (ccws_tenants, ccws_ledgers, ccws_charges)
--     FROM SERVER esa_middleware_fdw INTO public;
--
-- -- Recreate views by re-applying migrations 033 + 055 view bodies (they rebind
-- -- to the new foreign tables by name).
--
-- DROP TABLE ccws_tenants_legacy_pbi_20260515 CASCADE;
-- DROP TABLE ccws_ledgers_legacy_pbi_20260515 CASCADE;
-- DROP TABLE ccws_charges_legacy_pbi_20260515 CASCADE;
-- COMMIT;
