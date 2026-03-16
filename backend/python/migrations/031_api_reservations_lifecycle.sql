-- 031: Add lifecycle date columns and unique constraint for reservation tracking
-- Database: esa_pbi
-- Run: PGPASSWORD=<pw> psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d esa_pbi -f backend/python/migrations/031_api_reservations_lifecycle.sql

-- Lifecycle date columns
ALTER TABLE api_reservations ADD COLUMN IF NOT EXISTS reserved_at TIMESTAMP;
ALTER TABLE api_reservations ADD COLUMN IF NOT EXISTS moved_in_at TIMESTAMP;
ALTER TABLE api_reservations ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMP;
ALTER TABLE api_reservations ADD COLUMN IF NOT EXISTS expired_at TIMESTAMP;

-- Unique constraint: one row per reservation per site (idempotent pushes)
CREATE UNIQUE INDEX IF NOT EXISTS uq_api_res_site_waiting
    ON api_reservations (site_code, waiting_id)
    WHERE waiting_id IS NOT NULL;

-- Backfill: existing rows get reserved_at = created_at
UPDATE api_reservations SET reserved_at = created_at WHERE reserved_at IS NULL;
