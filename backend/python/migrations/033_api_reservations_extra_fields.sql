-- 033: Add missing SOAP fields to api_reservations
-- Database: esa_pbi
-- Run: PGPASSWORD=<pw> psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d esa_pbi -f backend/python/migrations/033_api_reservations_extra_fields.sql
--
-- These fields are returned by ReservationList_v3 but were not previously captured.

ALTER TABLE api_reservations ADD COLUMN IF NOT EXISTS followup_date      DATE;
ALTER TABLE api_reservations ADD COLUMN IF NOT EXISTS inquiry_type       INTEGER DEFAULT 0;
ALTER TABLE api_reservations ADD COLUMN IF NOT EXISTS rental_type_id     INTEGER DEFAULT 0;
ALTER TABLE api_reservations ADD COLUMN IF NOT EXISTS paid_reserve_fee   NUMERIC(10,2) DEFAULT 0;
ALTER TABLE api_reservations ADD COLUMN IF NOT EXISTS reserve_fee_receipt_id INTEGER DEFAULT 0;
ALTER TABLE api_reservations ADD COLUMN IF NOT EXISTS soap_updated_at    TIMESTAMP;
