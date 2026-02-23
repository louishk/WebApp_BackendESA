-- Migration 009: URL Shortener tables for link management and click tracking
-- Date: 2026-02-23

-- Short links table
CREATE TABLE IF NOT EXISTS short_links (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR(20) UNIQUE NOT NULL,
    original_url TEXT NOT NULL,
    title VARCHAR(255),
    tags VARCHAR(500),                -- comma-separated tags for organization
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    expires_at TIMESTAMP,             -- optional expiry
    password_hash VARCHAR(255),       -- optional password protection
    max_clicks INTEGER,               -- optional click cap
    total_clicks INTEGER NOT NULL DEFAULT 0,
    unique_clicks INTEGER NOT NULL DEFAULT 0,
    created_by VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_short_links_short_code ON short_links (short_code);
CREATE INDEX IF NOT EXISTS ix_short_links_created_by ON short_links (created_by);
CREATE INDEX IF NOT EXISTS ix_short_links_created_at ON short_links (created_at);
CREATE INDEX IF NOT EXISTS ix_short_links_is_active ON short_links (is_active);

-- Click tracking table
CREATE TABLE IF NOT EXISTS link_clicks (
    id SERIAL PRIMARY KEY,
    link_id INTEGER NOT NULL REFERENCES short_links(id) ON DELETE CASCADE,
    clicked_at TIMESTAMP NOT NULL DEFAULT NOW(),
    ip_address VARCHAR(45),
    user_agent TEXT,
    referer TEXT,
    country VARCHAR(100),
    city VARCHAR(100),
    device_type VARCHAR(20),          -- desktop, mobile, tablet, bot
    browser VARCHAR(50),
    os VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS ix_link_clicks_link_id ON link_clicks (link_id);
CREATE INDEX IF NOT EXISTS ix_link_clicks_clicked_at ON link_clicks (clicked_at);
CREATE INDEX IF NOT EXISTS ix_link_clicks_link_clicked ON link_clicks (link_id, clicked_at);

-- Add URL shortener permission to roles table
ALTER TABLE roles ADD COLUMN IF NOT EXISTS can_manage_links BOOLEAN DEFAULT FALSE;

-- Grant permission to admin role
UPDATE roles SET can_manage_links = TRUE WHERE name = 'admin';
