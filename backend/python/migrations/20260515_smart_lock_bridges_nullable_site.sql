-- Target DB: esa_middleware
-- Allow Bridges to land in mw_smart_lock_bridges even when their site can't be
-- inferred from Igloo. Today the pipeline drops a bridge silently if it has an
-- empty linkedDevices array (e.g. on-site bridge offline, never paired with a
-- keypad, or newly added to Igloo before pairing). That hides the bridge from
-- the operator UI entirely.
--
-- Adds site_assigned_by so the pipeline knows whether a site value was set by
-- it (safe to update) or by an admin (preserve on next sync).
--
-- Run from dev machine:
--   PGPASSWORD=<PBI_DB_PASSWORD> psql -h esapbi.postgres.database.azure.com \
--     -U esa_pbi_admin -d esa_middleware \
--     -f backend/python/migrations/20260515_smart_lock_bridges_nullable_site.sql

ALTER TABLE mw_smart_lock_bridges
    ALTER COLUMN site_id DROP NOT NULL;

ALTER TABLE mw_smart_lock_bridges
    ADD COLUMN IF NOT EXISTS site_assigned_by VARCHAR(20)
        NOT NULL DEFAULT 'igloo_pipeline';
