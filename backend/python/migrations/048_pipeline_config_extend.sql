-- Migration 048: Extend scheduler_pipeline_config for full DB-driven config
-- Adds sync_config, pipeline_specific_args, and managed_by columns
-- Safe to re-run (IF NOT EXISTS / IF NOT checks)

ALTER TABLE scheduler_pipeline_config
    ADD COLUMN IF NOT EXISTS sync_config JSONB,
    ADD COLUMN IF NOT EXISTS pipeline_specific_args JSONB,
    ADD COLUMN IF NOT EXISTS managed_by VARCHAR(20) NOT NULL DEFAULT 'scheduler';

-- Constrain managed_by to known values
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_managed_by'
    ) THEN
        ALTER TABLE scheduler_pipeline_config
            ADD CONSTRAINT chk_managed_by
            CHECK (managed_by IN ('scheduler', 'orchestrator'));
    END IF;
END $$;

COMMENT ON COLUMN scheduler_pipeline_config.sync_config IS 'Sync orchestrator config (strategy, watermark_field, phases, validation, checkpoint_interval)';
COMMENT ON COLUMN scheduler_pipeline_config.pipeline_specific_args IS 'Pipeline-specific operational args (sql_chunk_size, location_codes, batch_size, etc.)';
COMMENT ON COLUMN scheduler_pipeline_config.managed_by IS 'Which engine owns execution: scheduler or orchestrator';
