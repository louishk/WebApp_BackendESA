-- Rollback for 069_drop_legacy_scheduler_tables.sql
--
-- DO NOT RUN unless reverting the daemon decommissioning. This re-creates
-- empty tables with the same DDL — to restore data, separately load the
-- CSV exports from /var/backups/scheduler-db-final/scheduler-db-backup-*.tar.gz.
--
-- Target DB: esa_backend

BEGIN;

CREATE TABLE IF NOT EXISTS apscheduler_jobs (
    id            VARCHAR(191) PRIMARY KEY,
    next_run_time DOUBLE PRECISION,
    job_state     BYTEA NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_apscheduler_jobs_next_run_time
    ON apscheduler_jobs(next_run_time);

CREATE TABLE IF NOT EXISTS scheduler_state (
    id              INTEGER PRIMARY KEY DEFAULT 1,
    status          VARCHAR(20),
    pid             INTEGER,
    host_name       VARCHAR(255),
    started_at      TIMESTAMP,
    last_heartbeat  TIMESTAMP,
    CONSTRAINT scheduler_state_singleton CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS scheduler_resource_locks (
    id            SERIAL PRIMARY KEY,
    resource      VARCHAR(64) NOT NULL,
    holder        VARCHAR(255) NOT NULL,
    acquired_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at    TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_scheduler_resource_locks_resource
    ON scheduler_resource_locks(resource);

CREATE TABLE IF NOT EXISTS scheduler_pipeline_config (
    pipeline_name              VARCHAR(64) PRIMARY KEY,
    display_name               VARCHAR(255),
    description                TEXT,
    module_path                VARCHAR(255) NOT NULL,
    schedule_type              VARCHAR(32) NOT NULL,
    schedule_config            JSONB,
    enabled                    BOOLEAN NOT NULL DEFAULT TRUE,
    priority                   INTEGER DEFAULT 5,
    depends_on                 JSONB,
    conflicts_with             JSONB,
    resource_group             VARCHAR(32),
    max_db_connections         INTEGER,
    estimated_duration_seconds INTEGER,
    max_retries                INTEGER DEFAULT 3,
    retry_delay_seconds        INTEGER DEFAULT 300,
    retry_backoff_multiplier   NUMERIC(4,2) DEFAULT 2.0,
    timeout_seconds            INTEGER DEFAULT 3600,
    default_args               JSONB,
    data_freshness_config      JSONB,
    sync_config                JSONB,
    pipeline_specific_args     JSONB,
    managed_by                 VARCHAR(32) DEFAULT 'scheduler',
    created_at                 TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at                 TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scheduler_job_history (
    id              SERIAL PRIMARY KEY,
    execution_id    UUID NOT NULL,
    pipeline_name   VARCHAR(64) NOT NULL,
    started_at      TIMESTAMP NOT NULL,
    ended_at        TIMESTAMP,
    status          VARCHAR(20) NOT NULL,
    records         INTEGER,
    duration_ms     INTEGER,
    error_message   TEXT,
    triggered_by    VARCHAR(64),
    attempt         INTEGER DEFAULT 1,
    metadata        JSONB
);
CREATE INDEX IF NOT EXISTS ix_scheduler_job_history_pipeline_started
    ON scheduler_job_history(pipeline_name, started_at);
CREATE INDEX IF NOT EXISTS ix_scheduler_job_history_execution_id
    ON scheduler_job_history(execution_id);

COMMIT;
