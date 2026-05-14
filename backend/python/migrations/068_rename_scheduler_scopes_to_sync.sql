-- 068_rename_scheduler_scopes_to_sync.sql
--
-- Migrate API key scopes: the legacy 'scheduler:read' / 'scheduler:write'
-- scopes are folded into the existing 'sync:read' / 'sync:write' (they granted
-- equivalent access to the same /api endpoints, which now live entirely under
-- the orchestrator).
--
-- Strategy: walk every api_keys row whose `scopes` JSONB array contains either
-- legacy scope, write a new array with the legacy entry replaced by the sync
-- equivalent. Idempotent — running it twice is a no-op.
--
-- Target DB: esa_backend
-- Run with: PGPASSWORD="$DB_PASSWORD" psql "host=esapbi.postgres.database.azure.com port=5432 dbname=backend user=esa_pbi_admin sslmode=require" -f 068_rename_scheduler_scopes_to_sync.sql

BEGIN;

UPDATE api_keys
   SET scopes = (
       SELECT jsonb_agg(DISTINCT
           CASE elem::text
               WHEN '"scheduler:read"'  THEN '"sync:read"'::jsonb
               WHEN '"scheduler:write"' THEN '"sync:write"'::jsonb
               ELSE elem
           END
       )
       FROM jsonb_array_elements(scopes) elem
   )
 WHERE scopes @> '["scheduler:read"]'::jsonb
    OR scopes @> '["scheduler:write"]'::jsonb;

COMMIT;
