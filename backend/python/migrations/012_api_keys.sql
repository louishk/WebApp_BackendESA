-- Migration 012: API Keys for External Access
-- Target database: backend (PostgreSQL)
-- One API key per user with scoped permissions, rate limits, and daily quotas.
-- Scopes managed by admins under User Management.

BEGIN;

CREATE TABLE IF NOT EXISTS api_keys (
    id              SERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL DEFAULT 'Default',
    key_id          VARCHAR(16) UNIQUE NOT NULL,
    key_hash        VARCHAR(64) NOT NULL,

    -- Scopes (admin-managed)
    scopes          JSONB NOT NULL DEFAULT '[]',

    -- Rate limiting
    rate_limit      INTEGER NOT NULL DEFAULT 60,
    daily_quota     INTEGER NOT NULL DEFAULT 10000,
    daily_usage     INTEGER NOT NULL DEFAULT 0,
    quota_reset_date DATE,

    -- Status
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    last_used_at    TIMESTAMP WITH TIME ZONE,
    expires_at      TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_key_id ON api_keys(key_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(is_active);

COMMIT;
