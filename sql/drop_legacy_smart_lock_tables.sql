-- Drop legacy smart lock + igloo + gate-access tables from esa_backend.
-- All readers now go to esa_middleware (mw_smart_lock_* / ccws_gate_access / igloo_devices|properties in middleware).
-- Pre-req: orchestrator pipelines `igloo` and `ccws_gate_access` have run at least once to populate middleware copies.
-- Run with: PGPASSWORD=... psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d backend -f sql/drop_legacy_smart_lock_tables.sql

BEGIN;

DROP TABLE IF EXISTS smart_lock_audit_log;
DROP TABLE IF EXISTS smart_lock_unit_assignments;
DROP TABLE IF EXISTS smart_lock_site_config;
DROP TABLE IF EXISTS smart_lock_keypads;
DROP TABLE IF EXISTS smart_lock_padlocks;
DROP TABLE IF EXISTS igloo_access_codes;
DROP TABLE IF EXISTS gate_access_data;
DROP TABLE IF EXISTS igloo_devices;
DROP TABLE IF EXISTS igloo_properties;

COMMIT;
