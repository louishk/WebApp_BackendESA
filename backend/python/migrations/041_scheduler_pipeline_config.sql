-- Migration 041: Create scheduler_pipeline_config table
-- Stores pipeline configuration in DB for runtime changes without YAML edits.
-- The updated_at trigger drives the config polling mechanism in the scheduler daemon.

BEGIN;

CREATE TABLE IF NOT EXISTS scheduler_pipeline_config (
    pipeline_name               VARCHAR(50)     PRIMARY KEY,
    display_name                VARCHAR(100)    NOT NULL,
    description                 TEXT,
    module_path                 VARCHAR(200)    NOT NULL,

    -- Scheduling
    schedule_type               VARCHAR(20)     NOT NULL,
    schedule_config             JSONB           NOT NULL,
    enabled                     BOOLEAN         NOT NULL DEFAULT TRUE,

    -- Priority and dependencies
    priority                    INTEGER         NOT NULL DEFAULT 5,
    depends_on                  TEXT[],
    conflicts_with              TEXT[],

    -- Resource requirements
    resource_group              VARCHAR(50)     DEFAULT 'default',
    max_db_connections          INTEGER         DEFAULT 3,
    estimated_duration_seconds  INTEGER,

    -- Retry configuration
    max_retries                 INTEGER         NOT NULL DEFAULT 3,
    retry_delay_seconds         INTEGER         NOT NULL DEFAULT 300,
    retry_backoff_multiplier    NUMERIC(4,2)    DEFAULT 2.0,

    -- Timeouts
    timeout_seconds             INTEGER         DEFAULT 3600,

    -- Runtime arguments and data freshness checks
    default_args                JSONB,
    data_freshness_config       JSONB,

    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Auto-update updated_at so the config polling mechanism detects changes
CREATE OR REPLACE FUNCTION scheduler_pipeline_config_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_scheduler_pipeline_config_updated_at ON scheduler_pipeline_config;

CREATE TRIGGER trg_scheduler_pipeline_config_updated_at
    BEFORE UPDATE ON scheduler_pipeline_config
    FOR EACH ROW
    EXECUTE FUNCTION scheduler_pipeline_config_set_updated_at();

COMMIT;
