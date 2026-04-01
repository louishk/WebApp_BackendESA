-- Migration: 044 — Smart Lock site configuration (enable/disable per site)
-- Target DB: esa_backend

CREATE TABLE IF NOT EXISTS smart_lock_site_config (
    site_id      INTEGER PRIMARY KEY,
    enabled      BOOLEAN NOT NULL DEFAULT FALSE,
    site_code    VARCHAR(10),
    site_name    VARCHAR(255),
    notes        VARCHAR(255),
    updated_by   VARCHAR(255),
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    created_at   TIMESTAMPTZ DEFAULT NOW()
);
