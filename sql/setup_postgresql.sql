-- PostgreSQL Database Setup for WebApp_Backend
-- Database: backend
-- Run this script on esapbi.postgres.database.azure.com

-- Connect to the backend database first:
-- \c backend

-- ============================================
-- Users table
-- ============================================
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(255) NOT NULL UNIQUE,
  email VARCHAR(255) UNIQUE,
  password VARCHAR(255),
  role VARCHAR(20) NOT NULL CHECK (role IN ('admin', 'scheduler_admin', 'editor', 'viewer')),
  auth_provider VARCHAR(20) DEFAULT 'local' CHECK (auth_provider IN ('local', 'microsoft')),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for email lookups (OAuth)
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- ============================================
-- Pages table
-- ============================================
CREATE TABLE IF NOT EXISTS pages (
  id SERIAL PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  slug VARCHAR(255) UNIQUE NOT NULL,
  content TEXT,
  is_secure BOOLEAN DEFAULT FALSE,
  extension VARCHAR(10) DEFAULT 'php',
  edit_restricted BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for slug lookups
CREATE INDEX IF NOT EXISTS idx_pages_slug ON pages(slug);

-- ============================================
-- Schema markups table (SEO tool)
-- ============================================
CREATE TABLE IF NOT EXISTS schema_markups (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  schema_type VARCHAR(100) NOT NULL,
  schema_data JSONB NOT NULL,
  form_data JSONB,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- JWT tokens for scheduler
-- ============================================
CREATE TABLE IF NOT EXISTS scheduler_tokens (
  id SERIAL PRIMARY KEY,
  user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token VARCHAR(512) NOT NULL,
  expires_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for token lookups
CREATE INDEX IF NOT EXISTS idx_scheduler_tokens_user ON scheduler_tokens(user_id);
CREATE INDEX IF NOT EXISTS idx_scheduler_tokens_expires ON scheduler_tokens(expires_at);

-- ============================================
-- Audit log
-- ============================================
CREATE TABLE IF NOT EXISTS audit_log (
  id SERIAL PRIMARY KEY,
  user_id INT REFERENCES users(id),
  action VARCHAR(100) NOT NULL,
  resource VARCHAR(255),
  details JSONB,
  ip_address VARCHAR(45),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for audit queries
CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at);

-- ============================================
-- Scheduler tables (copy from esa_pbi or create new)
-- ============================================

-- APScheduler jobs table
CREATE TABLE IF NOT EXISTS apscheduler_jobs (
  id VARCHAR(191) PRIMARY KEY,
  next_run_time DOUBLE PRECISION,
  job_state BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_apscheduler_next_run ON apscheduler_jobs(next_run_time);

-- Scheduler state
CREATE TABLE IF NOT EXISTS scheduler_state (
  id SERIAL PRIMARY KEY,
  instance_id VARCHAR(100) UNIQUE NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'stopped',
  started_at TIMESTAMP,
  last_heartbeat TIMESTAMP,
  version VARCHAR(20),
  hostname VARCHAR(255),
  metadata JSONB DEFAULT '{}'
);

-- Pipeline configuration
CREATE TABLE IF NOT EXISTS scheduler_pipeline_config (
  id SERIAL PRIMARY KEY,
  pipeline_name VARCHAR(100) UNIQUE NOT NULL,
  display_name VARCHAR(255),
  enabled BOOLEAN DEFAULT TRUE,
  schedule_type VARCHAR(50) NOT NULL,
  schedule_config JSONB NOT NULL,
  priority INT DEFAULT 5,
  resource_group VARCHAR(50) DEFAULT 'default',
  max_db_connections INT DEFAULT 3,
  timeout_seconds INT DEFAULT 3600,
  retry_config JSONB DEFAULT '{"max_attempts": 3, "delay_seconds": 300}',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Job history
CREATE TABLE IF NOT EXISTS scheduler_job_history (
  id SERIAL PRIMARY KEY,
  job_id VARCHAR(100) NOT NULL,
  pipeline_name VARCHAR(100) NOT NULL,
  status VARCHAR(50) NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  duration_seconds NUMERIC(10,2),
  records_processed INT,
  error_message TEXT,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_job_history_pipeline ON scheduler_job_history(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_job_history_status ON scheduler_job_history(status);
CREATE INDEX IF NOT EXISTS idx_job_history_started ON scheduler_job_history(started_at);

-- Resource locks
CREATE TABLE IF NOT EXISTS scheduler_resource_locks (
  id SERIAL PRIMARY KEY,
  resource_name VARCHAR(100) NOT NULL,
  lock_type VARCHAR(50) NOT NULL,
  holder_job_id VARCHAR(100),
  holder_pipeline VARCHAR(100),
  acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP,
  metadata JSONB DEFAULT '{}'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_locks_name ON scheduler_resource_locks(resource_name);

-- ============================================
-- Default admin user (change password after first login!)
-- ============================================
INSERT INTO users (username, email, password, role, auth_provider)
VALUES ('admin', 'admin@localhost', '$2y$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi', 'admin', 'local')
ON CONFLICT (username) DO NOTHING;
-- Default password: 'password' - CHANGE THIS IMMEDIATELY!

-- ============================================
-- Grant permissions (adjust as needed)
-- ============================================
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO esa_pbi_admin;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO esa_pbi_admin;
