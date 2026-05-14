-- 067_rename_scheduler_perm_to_sync.sql
--
-- Rename can_access_scheduler → can_access_sync on the roles table, and
-- rename the system role 'scheduler_admin' → 'sync_admin'.
--
-- Context: the APScheduler legacy stack has been fully decommissioned (commit
-- 8fb894e). The permission that previously gated the scheduler UI now gates
-- the sync orchestrator + call scoring rubric tool.
--
-- Target DB: esa_backend
-- Run with: PGPASSWORD="$DB_PASSWORD" psql "host=esapbi.postgres.database.azure.com port=5432 dbname=backend user=esa_pbi_admin sslmode=require" -f 067_rename_scheduler_perm_to_sync.sql

BEGIN;

ALTER TABLE roles RENAME COLUMN can_access_scheduler TO can_access_sync;

UPDATE roles
   SET name = 'sync_admin',
       description = 'Sync orchestrator management'
 WHERE name = 'scheduler_admin';

COMMIT;
