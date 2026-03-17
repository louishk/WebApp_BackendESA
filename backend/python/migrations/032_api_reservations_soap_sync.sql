-- 032: Add soap_synced_at column for SOAP reservation sync pipeline
-- Database: esa_pbi
-- Run: PGPASSWORD=<pw> psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d esa_pbi -f backend/python/migrations/032_api_reservations_soap_sync.sql

ALTER TABLE api_reservations ADD COLUMN IF NOT EXISTS soap_synced_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_api_res_soap_synced
    ON api_reservations (soap_synced_at)
    WHERE soap_synced_at IS NOT NULL;
