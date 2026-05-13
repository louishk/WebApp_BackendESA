-- 059_add_frequency_category.sql
-- Adds an optional manual frequency-bucket override to mw_sync_pipelines.
-- NULL means: derive the bucket automatically from schedule_config.cron.
-- Used by the orchestrator UI to split the Data Freshness panel into
-- High / Med / Low sections.
--
-- Target DB: esa_middleware

ALTER TABLE mw_sync_pipelines
    ADD COLUMN IF NOT EXISTS frequency_category VARCHAR(10);

COMMENT ON COLUMN mw_sync_pipelines.frequency_category IS
    'Manual override: high|med|low. NULL = derive from cron.';
