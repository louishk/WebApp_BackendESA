-- Migration 051: sync_service standalone schema
-- Fully independent from scheduler_pipeline_config.
-- Purpose: on-demand + scheduled sync orchestrator with scoped freshness.

-- =============================================================================
-- sync_pipelines — pipeline registry (source of truth for sync_service)
-- =============================================================================
CREATE TABLE IF NOT EXISTS sync_pipelines (
    pipeline_name            VARCHAR(100) PRIMARY KEY,
    display_name             VARCHAR(200) NOT NULL,
    description              TEXT,
    pipeline_class           VARCHAR(300) NOT NULL,  -- e.g. "sync_service.pipelines.reservations.ReservationsPipeline"
    enabled                  BOOLEAN NOT NULL DEFAULT true,

    -- Scheduled execution (optional — can be on-demand only)
    schedule_type            VARCHAR(20) DEFAULT 'on_demand',  -- 'cron' | 'interval' | 'on_demand'
    schedule_config          JSONB DEFAULT '{}'::jsonb,        -- {"cron": "0 */6 * * *"} etc

    -- Freshness configuration
    freshness_table          VARCHAR(100),                      -- target table for MAX() query
    freshness_column         VARCHAR(100),                      -- timestamp column
    freshness_scope_column   VARCHAR(100),                      -- scope filter column (nullable)
    freshness_ttl_seconds    INTEGER NOT NULL DEFAULT 300,      -- default TTL
    freshness_database       VARCHAR(20) NOT NULL DEFAULT 'pbi',  -- 'pbi' | 'backend'

    -- Execution controls
    max_concurrency          INTEGER NOT NULL DEFAULT 5,        -- parallel scoped runs per pipeline
    resource_group           VARCHAR(50) DEFAULT 'soap_api',    -- soap_api | http_api | db_only
    max_db_connections       INTEGER NOT NULL DEFAULT 2,
    timeout_seconds          INTEGER NOT NULL DEFAULT 600,
    max_retries              INTEGER NOT NULL DEFAULT 3,
    retry_delay_seconds      INTEGER NOT NULL DEFAULT 60,

    -- Pipeline-specific runtime args (merged with request scope)
    default_args             JSONB DEFAULT '{}'::jsonb,

    created_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_sync_schedule_type CHECK (schedule_type IN ('cron', 'interval', 'on_demand')),
    CONSTRAINT chk_sync_freshness_db CHECK (freshness_database IN ('pbi', 'backend'))
);

COMMENT ON TABLE sync_pipelines IS 'Sync orchestrator pipeline registry (independent from scheduler_pipeline_config)';
COMMENT ON COLUMN sync_pipelines.pipeline_class IS 'Fully-qualified Python class path for BasePipeline subclass';
COMMENT ON COLUMN sync_pipelines.freshness_scope_column IS 'Column to filter by when checking per-scope freshness (e.g., site_code)';

-- =============================================================================
-- sync_runs — execution history
-- =============================================================================
CREATE TABLE IF NOT EXISTS sync_runs (
    id                       BIGSERIAL PRIMARY KEY,
    execution_id             UUID NOT NULL UNIQUE,
    pipeline_name            VARCHAR(100) NOT NULL REFERENCES sync_pipelines(pipeline_name) ON DELETE CASCADE,

    -- Scope (what was requested to refresh)
    scope                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    scope_hash               VARCHAR(64) NOT NULL,               -- for dedup lookup

    -- Trigger info
    triggered_by             VARCHAR(50) NOT NULL,               -- 'api' | 'schedule' | 'cli' | 'middleware'
    triggered_by_detail      VARCHAR(200),                       -- caller identity / request id

    -- Lifecycle
    status                   VARCHAR(20) NOT NULL DEFAULT 'queued',  -- queued | running | completed | failed | deduped | timeout
    queued_at                TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at               TIMESTAMP WITH TIME ZONE,
    completed_at             TIMESTAMP WITH TIME ZONE,
    duration_ms              INTEGER,

    -- Result
    records_processed        INTEGER DEFAULT 0,
    result                   JSONB,                              -- full RunResult as dict
    error_message            TEXT,

    -- Freshness check outcome
    freshness_age_seconds    INTEGER,                            -- age at check time
    was_fresh                BOOLEAN,                            -- true if skipped as fresh
    was_deduplicated         BOOLEAN NOT NULL DEFAULT false,     -- true if attached to in-flight run

    -- Attempt tracking (for retries)
    attempt_number           INTEGER NOT NULL DEFAULT 1,

    host_name                VARCHAR(100),
    created_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_sync_run_status CHECK (
        status IN ('queued', 'running', 'completed', 'failed', 'deduped', 'timeout', 'cancelled')
    )
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_pipeline ON sync_runs(pipeline_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sync_runs_status ON sync_runs(status) WHERE status IN ('queued', 'running');
CREATE INDEX IF NOT EXISTS idx_sync_runs_scope_hash ON sync_runs(pipeline_name, scope_hash, status);

COMMENT ON TABLE sync_runs IS 'Execution history for sync_service — every on-demand and scheduled run';

-- =============================================================================
-- sync_state — cursors/watermarks per (pipeline, scope_key)
-- =============================================================================
-- Separate from Phase A's sync_state (which lives in same name but was for
-- scheduler-integrated orchestrator). Rename if collision — but that older
-- table is unused in the new service.

DROP TABLE IF EXISTS sync_state CASCADE;

CREATE TABLE sync_state (
    pipeline_name            VARCHAR(100) NOT NULL REFERENCES sync_pipelines(pipeline_name) ON DELETE CASCADE,
    scope_key                VARCHAR(300) NOT NULL DEFAULT '__all__',  -- hash of scope, or '__all__' for full
    phase                    VARCHAR(50) NOT NULL DEFAULT 'main',

    cursor_value             TEXT,                               -- watermark value
    cursor_type              VARCHAR(20) DEFAULT 'timestamp',    -- timestamp | id | offset
    last_sync_at             TIMESTAMP WITH TIME ZONE,
    last_success_at          TIMESTAMP WITH TIME ZONE,
    records_in_scope         INTEGER,

    updated_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    PRIMARY KEY (pipeline_name, scope_key, phase)
);

COMMENT ON TABLE sync_state IS 'Per-scope cursors for incremental sync (sync_service)';

-- =============================================================================
-- sync_service_state — singleton daemon state (health check)
-- =============================================================================
CREATE TABLE IF NOT EXISTS sync_service_state (
    id                       INTEGER PRIMARY KEY DEFAULT 1,
    status                   VARCHAR(20) NOT NULL DEFAULT 'stopped',
    started_at               TIMESTAMP WITH TIME ZONE,
    host_name                VARCHAR(100),
    pid                      INTEGER,
    last_heartbeat           TIMESTAMP WITH TIME ZONE,
    version                  VARCHAR(20),

    CONSTRAINT chk_sync_service_singleton CHECK (id = 1),
    CONSTRAINT chk_sync_service_status CHECK (
        status IN ('running', 'stopped', 'paused', 'starting', 'stopping')
    )
);
