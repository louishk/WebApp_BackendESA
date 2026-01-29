-- ============================================
-- Verify Scheduler Migration
-- Run this after migration to verify data integrity
-- ============================================

-- Check scheduler tables exist
SELECT 'apscheduler_jobs' AS table_name, COUNT(*) AS record_count
FROM apscheduler_jobs
UNION ALL
SELECT 'scheduler_state', COUNT(*)
FROM scheduler_state
UNION ALL
SELECT 'scheduler_job_history', COUNT(*)
FROM scheduler_job_history
UNION ALL
SELECT 'scheduler_pipeline_config', COUNT(*)
FROM scheduler_pipeline_config
UNION ALL
SELECT 'scheduler_resource_locks', COUNT(*)
FROM scheduler_resource_locks;

-- Check job history records
SELECT
    pipeline_name,
    status,
    COUNT(*) as count,
    MAX(scheduled_at) as last_run
FROM scheduler_job_history
GROUP BY pipeline_name, status
ORDER BY pipeline_name, status;

-- Check scheduler state
SELECT * FROM scheduler_state;

-- Check for any failed jobs in last 24 hours
SELECT
    pipeline_name,
    status,
    scheduled_at,
    error_message
FROM scheduler_job_history
WHERE status = 'failed'
  AND scheduled_at > NOW() - INTERVAL '24 hours'
ORDER BY scheduled_at DESC;
