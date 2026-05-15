-- Drop legacy cc_* dump tables from esa_pbi
--
-- Context: cc_tenants / cc_ledgers / cc_charges were populated by the
-- pre-orchestrator era SQL-Server-dump pipeline. They were replaced by the
-- ccws_* mirrors in esa_middleware (CcwsTenants / CcwsLedgers / CcwsCharges
-- pipelines). A 2026-05-15 codebase audit confirmed:
--   - Zero readers/writers in backend, sync_service, web routes, or tools
--   - Only references were the ORM classes themselves and historical comments
--   - cc_discount was already renamed → ccws_discount earlier
--
-- Before running: verify Power BI semantic model does NOT reference these.
-- Pre-flight check (run on esa_pbi):
--   SELECT relname, n_live_tup FROM pg_stat_user_tables
--   WHERE relname IN ('cc_tenants','cc_ledgers','cc_charges');

BEGIN;

DROP TABLE IF EXISTS cc_charges CASCADE;
DROP TABLE IF EXISTS cc_ledgers CASCADE;
DROP TABLE IF EXISTS cc_tenants CASCADE;

COMMIT;
