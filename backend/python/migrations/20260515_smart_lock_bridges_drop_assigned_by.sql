-- Target DB: esa_middleware
-- Drop site_assigned_by on mw_smart_lock_bridges. The earlier migration added
-- this column to support an admin-override flow for assigning orphan bridges
-- to sites — that path is removed: Igloo is the source of truth for the
-- bridge→site mapping, manual overrides would mask Igloo misconfigurations.
--
-- site_id remains NULLABLE so orphan bridges (those Igloo can't tell us the
-- site of) still appear in the operator UI as "Unassigned"; the fix for an
-- orphan is to repair the pairing in Igloo and re-sync.
--
-- Run from dev machine:
--   PGPASSWORD=<PBI_DB_PASSWORD> psql -h esapbi.postgres.database.azure.com \
--     -U esa_pbi_admin -d esa_middleware \
--     -f backend/python/migrations/20260515_smart_lock_bridges_drop_assigned_by.sql

ALTER TABLE mw_smart_lock_bridges
    DROP COLUMN IF EXISTS site_assigned_by;
