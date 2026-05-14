-- Rollback for 068_rename_scheduler_scopes_to_sync.sql
-- DO NOT RUN unless you're explicitly reverting the scope consolidation.
-- Target DB: esa_backend
--
-- This duplicates sync:read -> scheduler:read and sync:write -> scheduler:write
-- on any api_key whose scopes JSONB contains the sync entries — it does not
-- remove the sync:* entries. This is intentional: it widens the grant rather
-- than narrowing it, so a partial revert can't accidentally lock a key out.
-- Once safe, manually strip whichever scopes are no longer wanted.

BEGIN;

UPDATE api_keys
   SET scopes = scopes || '["scheduler:read"]'::jsonb
 WHERE scopes @> '["sync:read"]'::jsonb
   AND NOT scopes @> '["scheduler:read"]'::jsonb;

UPDATE api_keys
   SET scopes = scopes || '["scheduler:write"]'::jsonb
 WHERE scopes @> '["sync:write"]'::jsonb
   AND NOT scopes @> '["scheduler:write"]'::jsonb;

COMMIT;
