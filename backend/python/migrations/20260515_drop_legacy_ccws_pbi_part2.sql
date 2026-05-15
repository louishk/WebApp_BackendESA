-- Drop stale ccws_charge_descriptions + ccws_insurance_coverage from esa_pbi
--
-- Both tables are now canonically owned by esa_middleware (pipelines write
-- there via get_engine('middleware')). PBI copies are orphans.
-- All Flask routes, services, scripts, tests, and pipelines have been
-- repointed to middleware as of 2026-05-15.

BEGIN;
DROP TABLE IF EXISTS ccws_charge_descriptions CASCADE;
DROP TABLE IF EXISTS ccws_insurance_coverage CASCADE;
COMMIT;
