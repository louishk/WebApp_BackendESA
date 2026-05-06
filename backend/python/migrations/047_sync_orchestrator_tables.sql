-- Migration 047: Sync Orchestrator Tables
-- Creates tables for sync state tracking, dead letter queue, and alert logging.
-- Database: esa_backend
-- Run: PGPASSWORD=<pw> psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d backend -f migrations/047_sync_orchestrator_tables.sql

BEGIN;

-- 1. Sync state — cursor/watermark tracking per pipeline per phase
CREATE TABLE IF NOT EXISTS sync_state (
    pipeline_name   VARCHAR(100) NOT NULL,
    phase           VARCHAR(50)  NOT NULL DEFAULT 'main',
    last_sync_at    TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    cursor_value    TEXT,
    cursor_type     VARCHAR(20)  NOT NULL DEFAULT 'datetime',
    records_processed INTEGER DEFAULT 0,
    metadata        JSONB DEFAULT '{}',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_name, phase),
    CONSTRAINT chk_cursor_type CHECK (cursor_type IN ('datetime', 'offset', 'token', 'id'))
);

COMMENT ON TABLE sync_state IS 'Sync orchestrator cursor/watermark tracking per pipeline per phase';
COMMENT ON COLUMN sync_state.cursor_value IS 'Opaque watermark — datetime, offset, token, or ID depending on cursor_type';
COMMENT ON COLUMN sync_state.metadata IS 'Pipeline-specific extras (execution_id, phase status, etc.)';

-- Trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION update_sync_state_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_state_updated
    BEFORE UPDATE ON sync_state
    FOR EACH ROW
    EXECUTE FUNCTION update_sync_state_timestamp();


-- 2. Dead letter queue — failed records quarantine
CREATE TABLE IF NOT EXISTS sync_dead_letters (
    id              SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    execution_id    UUID,
    record_key      TEXT,
    record_data     JSONB,
    error_message   TEXT,
    resolved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_pipeline
    ON sync_dead_letters (pipeline_name);
CREATE INDEX IF NOT EXISTS idx_dead_letters_execution
    ON sync_dead_letters (execution_id);
CREATE INDEX IF NOT EXISTS idx_dead_letters_unresolved
    ON sync_dead_letters (pipeline_name)
    WHERE resolved_at IS NULL;

COMMENT ON TABLE sync_dead_letters IS 'Dead letter queue for records that fail validation or processing';


-- 3. Alert log — audit trail for sent alerts
CREATE TABLE IF NOT EXISTS alert_log (
    id              SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    execution_id    UUID,
    channel         VARCHAR(50)  NOT NULL,
    event_type      VARCHAR(50)  NOT NULL,
    message         TEXT,
    delivered       BOOLEAN DEFAULT FALSE,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_alert_channel CHECK (channel IN ('slack', 'email', 'teams', 'webhook')),
    CONSTRAINT chk_alert_event_type CHECK (event_type IN ('failure', 'success', 'timeout', 'retry', 'validation_fail'))
);

CREATE INDEX IF NOT EXISTS idx_alert_log_pipeline
    ON alert_log (pipeline_name);
CREATE INDEX IF NOT EXISTS idx_alert_log_pipeline_created
    ON alert_log (pipeline_name, created_at DESC);

COMMENT ON TABLE alert_log IS 'Audit trail for pipeline alerts — tracks delivery status per channel';

COMMIT;
