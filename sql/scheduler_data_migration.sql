-- ============================================
-- Scheduler Data Migration Script
-- Migrates data from esa_pbi to backend database
-- ============================================
--
-- Prerequisites:
-- 1. Run scheduler_tables.sql on backend database first
-- 2. Stop the current scheduler before running this script
--
-- Usage (run from psql connected to Azure PostgreSQL):
--   \i scheduler_data_migration.sql
--
-- Or use dblink for cross-database migration:
--   See instructions below
-- ============================================

-- Note: This script assumes you're running it with access to both databases.
-- For Azure PostgreSQL, you may need to use pg_dump/pg_restore or
-- a migration script with two separate connections.

-- ============================================
-- Option 1: Export/Import Method
-- ============================================
-- Step 1: Export from esa_pbi database
/*
COPY (SELECT * FROM apscheduler_jobs) TO '/tmp/apscheduler_jobs.csv' WITH CSV HEADER;
COPY (SELECT * FROM scheduler_state) TO '/tmp/scheduler_state.csv' WITH CSV HEADER;
COPY (SELECT * FROM scheduler_job_history) TO '/tmp/scheduler_job_history.csv' WITH CSV HEADER;
COPY (SELECT * FROM scheduler_pipeline_config) TO '/tmp/scheduler_pipeline_config.csv' WITH CSV HEADER;
COPY (SELECT * FROM scheduler_resource_locks) TO '/tmp/scheduler_resource_locks.csv' WITH CSV HEADER;
*/

-- Step 2: Import to backend database
/*
COPY apscheduler_jobs FROM '/tmp/apscheduler_jobs.csv' WITH CSV HEADER;
COPY scheduler_state FROM '/tmp/scheduler_state.csv' WITH CSV HEADER;
COPY scheduler_job_history FROM '/tmp/scheduler_job_history.csv' WITH CSV HEADER;
COPY scheduler_pipeline_config FROM '/tmp/scheduler_pipeline_config.csv' WITH CSV HEADER;
COPY scheduler_resource_locks FROM '/tmp/scheduler_resource_locks.csv' WITH CSV HEADER;
*/

-- ============================================
-- Option 2: Using dblink (if enabled)
-- ============================================
-- First, install dblink extension in backend database:
/*
CREATE EXTENSION IF NOT EXISTS dblink;

-- Create connection to esa_pbi
SELECT dblink_connect('esa_pbi_conn',
    'host=YOUR_HOST dbname=esa_pbi user=YOUR_USER password=YOUR_PASSWORD');

-- Migrate apscheduler_jobs
INSERT INTO apscheduler_jobs (id, next_run_time, job_state)
SELECT * FROM dblink('esa_pbi_conn',
    'SELECT id, next_run_time, job_state FROM apscheduler_jobs')
AS t(id VARCHAR(191), next_run_time DOUBLE PRECISION, job_state BYTEA)
ON CONFLICT (id) DO UPDATE SET
    next_run_time = EXCLUDED.next_run_time,
    job_state = EXCLUDED.job_state;

-- Migrate scheduler_state
INSERT INTO scheduler_state (id, status, started_at, host_name, pid, last_heartbeat, version, config_hash)
SELECT * FROM dblink('esa_pbi_conn',
    'SELECT id, status, started_at, host_name, pid, last_heartbeat, version, config_hash FROM scheduler_state')
AS t(id INTEGER, status VARCHAR(20), started_at TIMESTAMPTZ, host_name VARCHAR(100),
    pid INTEGER, last_heartbeat TIMESTAMPTZ, version VARCHAR(20), config_hash VARCHAR(64))
ON CONFLICT (id) DO UPDATE SET
    status = EXCLUDED.status,
    started_at = EXCLUDED.started_at,
    host_name = EXCLUDED.host_name,
    pid = EXCLUDED.pid,
    last_heartbeat = EXCLUDED.last_heartbeat,
    version = EXCLUDED.version,
    config_hash = EXCLUDED.config_hash;

-- Migrate scheduler_job_history (preserve existing records)
INSERT INTO scheduler_job_history (
    job_id, pipeline_name, execution_id, status, priority,
    scheduled_at, started_at, completed_at, duration_seconds,
    mode, parameters, records_processed,
    attempt_number, max_retries, retry_delay_seconds, next_retry_at,
    error_message, error_traceback,
    alert_sent, alert_sent_at,
    triggered_by, host_name,
    created_at, updated_at
)
SELECT
    job_id, pipeline_name, execution_id, status, priority,
    scheduled_at, started_at, completed_at, duration_seconds,
    mode, parameters, records_processed,
    attempt_number, max_retries, retry_delay_seconds, next_retry_at,
    error_message, error_traceback,
    alert_sent, alert_sent_at,
    triggered_by, host_name,
    created_at, updated_at
FROM dblink('esa_pbi_conn',
    'SELECT job_id, pipeline_name, execution_id, status, priority,
            scheduled_at, started_at, completed_at, duration_seconds,
            mode, parameters, records_processed,
            attempt_number, max_retries, retry_delay_seconds, next_retry_at,
            error_message, error_traceback,
            alert_sent, alert_sent_at,
            triggered_by, host_name,
            created_at, updated_at
     FROM scheduler_job_history')
AS t(job_id VARCHAR(100), pipeline_name VARCHAR(50), execution_id UUID, status VARCHAR(20), priority INTEGER,
    scheduled_at TIMESTAMPTZ, started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, duration_seconds NUMERIC(10,2),
    mode VARCHAR(20), parameters JSONB, records_processed INTEGER,
    attempt_number INTEGER, max_retries INTEGER, retry_delay_seconds INTEGER, next_retry_at TIMESTAMPTZ,
    error_message TEXT, error_traceback TEXT,
    alert_sent BOOLEAN, alert_sent_at TIMESTAMPTZ,
    triggered_by VARCHAR(50), host_name VARCHAR(100),
    created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)
ON CONFLICT (execution_id) DO NOTHING;

-- Close connection
SELECT dblink_disconnect('esa_pbi_conn');
*/

-- ============================================
-- Option 3: Python Script Migration
-- ============================================
-- See app/scheduler/python/scripts/migrate_data.py

-- ============================================
-- Post-Migration Verification
-- ============================================

-- Run these queries after migration to verify data:

-- Check record counts
SELECT 'apscheduler_jobs' AS table_name, COUNT(*) AS record_count FROM apscheduler_jobs
UNION ALL
SELECT 'scheduler_state', COUNT(*) FROM scheduler_state
UNION ALL
SELECT 'scheduler_job_history', COUNT(*) FROM scheduler_job_history
UNION ALL
SELECT 'scheduler_pipeline_config', COUNT(*) FROM scheduler_pipeline_config
UNION ALL
SELECT 'scheduler_resource_locks', COUNT(*) FROM scheduler_resource_locks;

-- Check scheduler state
SELECT * FROM scheduler_state;

-- Check recent job history
SELECT
    pipeline_name,
    COUNT(*) as total_runs,
    COUNT(*) FILTER (WHERE status = 'completed') as successful,
    COUNT(*) FILTER (WHERE status = 'failed') as failed,
    MAX(scheduled_at) as last_run
FROM scheduler_job_history
GROUP BY pipeline_name
ORDER BY pipeline_name;

-- Reset scheduler state for new deployment
UPDATE scheduler_state
SET status = 'stopped',
    pid = NULL,
    last_heartbeat = NULL
WHERE id = 1;
