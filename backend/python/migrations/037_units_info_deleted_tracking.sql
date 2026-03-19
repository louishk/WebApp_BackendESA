-- Migration 037: Add deleted_at column to units_info
-- Target database: esa_pbi
-- Tracks when a unit was deleted from SiteLink (no longer returned by SOAP API)

ALTER TABLE units_info ADD COLUMN IF NOT EXISTS deleted_at DATE;
