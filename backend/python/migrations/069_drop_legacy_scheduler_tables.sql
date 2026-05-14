-- 069_drop_legacy_scheduler_tables.sql
--
-- Final step of the APScheduler daemon decommissioning. Drops the five
-- legacy tables in esa_backend that backed the (now-deleted) scheduler
-- engine, executor, alert subsystem, dashboard, and history view.
--
-- Pre-drop row counts (recorded for the audit trail; CSV exports also
-- archived to /tmp/scheduler-db-backup-* and /var/backups/scheduler-db-final/
-- on the VM before this ran):
--   apscheduler_jobs            0
--   scheduler_resource_locks    0
--   scheduler_state             1   (singleton heartbeat row)
--   scheduler_job_history       4192 (audit history — CSV preserved)
--   scheduler_pipeline_config   0   (already empty after the cut-over)
--
-- Target DB: esa_backend
-- Run with: PGPASSWORD="$DB_PASSWORD" psql "host=... dbname=backend ..." \
--           -f 069_drop_legacy_scheduler_tables.sql

BEGIN;

DROP TABLE IF EXISTS scheduler_job_history CASCADE;
DROP TABLE IF EXISTS scheduler_pipeline_config CASCADE;
DROP TABLE IF EXISTS scheduler_resource_locks CASCADE;
DROP TABLE IF EXISTS scheduler_state CASCADE;
DROP TABLE IF EXISTS apscheduler_jobs CASCADE;

COMMIT;
