-- Migration 023: Add PostalCode and PrimaryEmail to siteinfo
-- These columns are populated by the populate_siteinfo.py seed script

ALTER TABLE siteinfo ADD COLUMN IF NOT EXISTS "PostalCode" VARCHAR(20);
ALTER TABLE siteinfo ADD COLUMN IF NOT EXISTS "PrimaryEmail" VARCHAR(255);
