-- Target DB: esa_pbi
-- Adds an index on ccws_ledgers(extract_date) to speed up MAX() freshness probes.
-- Without this, /ecri/api/data-freshness does a sequential scan (~470 ms on
-- current row count). With the index it drops to <10 ms.
--
-- Run from dev machine:
--   PGPASSWORD=<VM_SSH_PASSWORD> psql -h esapbi.postgres.database.azure.com \
--     -U esa_pbi_admin -d esa_pbi \
--     -f backend/python/migrations/20260515_idx_ccws_ledgers_extract_date_pbi.sql

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ccws_ledgers_extract_date
    ON ccws_ledgers (extract_date);
