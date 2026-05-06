-- Migration 039: Add allowed_site_ids to users table
-- Target DB: esa_backend (db: backend)

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS allowed_site_ids INTEGER[];

COMMENT ON COLUMN users.allowed_site_ids IS
    'NULL or empty = all sites (Revenue / admin). Non-empty array = restricted to these site IDs (Ops staff).';
