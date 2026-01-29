-- ============================================
-- Scheduler Tables for Backend Database
-- Migration from esa_pbi standalone scheduler
-- ============================================

-- APScheduler Job Store
-- This table is managed by APScheduler directly
CREATE TABLE IF NOT EXISTS apscheduler_jobs (
    id VARCHAR(191) NOT NULL PRIMARY KEY,
    next_run_time DOUBLE PRECISION,
    job_state BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_apscheduler_next_run_time ON apscheduler_jobs(next_run_time);

-- Scheduler State (Singleton)
-- Tracks the daemon process state for health checks
CREATE TABLE IF NOT EXISTS scheduler_state (
    id INTEGER PRIMARY KEY DEFAULT 1,
    status VARCHAR(20) NOT NULL DEFAULT 'stopped',
    started_at TIMESTAMP WITH TIME ZONE,
    host_name VARCHAR(100),
    pid INTEGER,
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    version VARCHAR(20),
    config_hash VARCHAR(64),
    CONSTRAINT chk_singleton CHECK (id = 1),
    CONSTRAINT chk_scheduler_status CHECK (status IN ('running', 'stopped', 'paused', 'starting', 'stopping'))
);

-- Insert default state if not exists
INSERT INTO scheduler_state (id, status)
VALUES (1, 'stopped')
ON CONFLICT (id) DO NOTHING;

-- Job History
-- Records every execution attempt with status, timing, and results
CREATE TABLE IF NOT EXISTS scheduler_job_history (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(100) NOT NULL,
    pipeline_name VARCHAR(50) NOT NULL,
    execution_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 5,

    -- Timing
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(10, 2),

    -- Execution details
    mode VARCHAR(20),
    parameters JSONB,
    records_processed INTEGER,

    -- Retry handling
    attempt_number INTEGER NOT NULL DEFAULT 1,
    max_retries INTEGER NOT NULL DEFAULT 3,
    retry_delay_seconds INTEGER DEFAULT 300,
    next_retry_at TIMESTAMP WITH TIME ZONE,

    -- Error tracking
    error_message TEXT,
    error_traceback TEXT,

    -- Alerts
    alert_sent BOOLEAN DEFAULT FALSE,
    alert_sent_at TIMESTAMP WITH TIME ZONE,

    -- Metadata
    triggered_by VARCHAR(50) DEFAULT 'scheduler',
    host_name VARCHAR(100),

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_job_status CHECK (status IN ('pending', 'queued', 'running', 'completed', 'failed', 'cancelled', 'retrying'))
);

CREATE INDEX IF NOT EXISTS idx_job_history_job_id ON scheduler_job_history(job_id);
CREATE INDEX IF NOT EXISTS idx_job_history_pipeline_name ON scheduler_job_history(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_job_history_execution_id ON scheduler_job_history(execution_id);
CREATE INDEX IF NOT EXISTS idx_job_history_status ON scheduler_job_history(status);
CREATE INDEX IF NOT EXISTS idx_job_history_scheduled_desc ON scheduler_job_history(scheduled_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_history_pipeline_status ON scheduler_job_history(pipeline_name, status);

-- Pipeline Configuration
-- Stores pipeline config in database for runtime changes without YAML edits
CREATE TABLE IF NOT EXISTS scheduler_pipeline_config (
    pipeline_name VARCHAR(50) PRIMARY KEY,
    display_name VARCHAR(100) NOT NULL,
    description TEXT,
    module_path VARCHAR(200) NOT NULL,

    -- Scheduling
    schedule_type VARCHAR(20) NOT NULL,
    schedule_config JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,

    -- Priority and dependencies
    priority INTEGER NOT NULL DEFAULT 5,
    depends_on VARCHAR(50)[],
    conflicts_with VARCHAR(50)[],

    -- Resource requirements
    resource_group VARCHAR(50) DEFAULT 'default',
    max_db_connections INTEGER DEFAULT 3,
    estimated_duration_seconds INTEGER,

    -- Retry configuration
    max_retries INTEGER NOT NULL DEFAULT 3,
    retry_delay_seconds INTEGER NOT NULL DEFAULT 300,

    -- Timeouts
    timeout_seconds INTEGER DEFAULT 3600,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Resource Locks
-- Distributed locking for preventing concurrent access to shared resources
CREATE TABLE IF NOT EXISTS scheduler_resource_locks (
    resource_name VARCHAR(100) PRIMARY KEY,
    locked_by_job_id VARCHAR(100),
    locked_by_execution_id UUID,
    locked_at TIMESTAMP WITH TIME ZONE,
    lock_expires_at TIMESTAMP WITH TIME ZONE,
    max_concurrent INTEGER DEFAULT 1,
    current_count INTEGER DEFAULT 0
);

-- Trigger for updated_at on job_history
CREATE OR REPLACE FUNCTION update_scheduler_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_job_history_updated_at ON scheduler_job_history;
CREATE TRIGGER update_job_history_updated_at
    BEFORE UPDATE ON scheduler_job_history
    FOR EACH ROW EXECUTE FUNCTION update_scheduler_updated_at();

DROP TRIGGER IF EXISTS update_pipeline_config_updated_at ON scheduler_pipeline_config;
CREATE TRIGGER update_pipeline_config_updated_at
    BEFORE UPDATE ON scheduler_pipeline_config
    FOR EACH ROW EXECUTE FUNCTION update_scheduler_updated_at();

-- ============================================
-- Comments for documentation
-- ============================================

COMMENT ON TABLE apscheduler_jobs IS 'APScheduler persistent job store - managed by APScheduler library';
COMMENT ON TABLE scheduler_state IS 'Singleton table for scheduler daemon state and health checks';
COMMENT ON TABLE scheduler_job_history IS 'Complete execution history for all pipeline runs';
COMMENT ON TABLE scheduler_pipeline_config IS 'Runtime pipeline configuration (supplements YAML config)';
COMMENT ON TABLE scheduler_resource_locks IS 'Distributed locks for resource contention prevention';

COMMENT ON COLUMN scheduler_job_history.triggered_by IS 'How the job was triggered: scheduler, cli, api, manual';
COMMENT ON COLUMN scheduler_job_history.status IS 'Job status: pending, queued, running, completed, failed, cancelled, retrying';
COMMENT ON COLUMN scheduler_state.status IS 'Daemon status: running, stopped, paused, starting, stopping';
