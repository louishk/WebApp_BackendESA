-- Migration 025: App Secrets Table
-- Replaces file-based .vault/ with DB-backed secrets storage in esa_backend
-- Bootstrap secrets (DB_PASSWORD, VAULT_MASTER_KEY) remain as env vars

CREATE TABLE IF NOT EXISTS app_secrets (
    id SERIAL PRIMARY KEY,
    key VARCHAR(100) UNIQUE NOT NULL,
    value_encrypted TEXT NOT NULL,
    environment VARCHAR(20) NOT NULL DEFAULT 'all'
        CHECK (environment IN ('all', 'production', 'development')),
    description VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_by VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_app_secrets_key ON app_secrets(key);
CREATE INDEX IF NOT EXISTS idx_app_secrets_env ON app_secrets(environment);

COMMENT ON TABLE app_secrets IS 'Encrypted application secrets - replaces file-based vault';
COMMENT ON COLUMN app_secrets.value_encrypted IS 'Fernet-encrypted secret value (or base64 salt for meta rows)';
COMMENT ON COLUMN app_secrets.environment IS 'all, production, or development';
