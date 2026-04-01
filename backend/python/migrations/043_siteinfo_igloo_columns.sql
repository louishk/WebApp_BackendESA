-- Migration: 043 — Add Igloo department/property IDs to siteinfo
-- Enables direct SiteID lookup from Igloo API without hardcoded property name mapping
-- Target DB: esa_pbi

ALTER TABLE siteinfo ADD COLUMN IF NOT EXISTS igloo_department_id VARCHAR(50);
ALTER TABLE siteinfo ADD COLUMN IF NOT EXISTS igloo_property_id VARCHAR(50);

CREATE INDEX IF NOT EXISTS idx_siteinfo_igloo_dept ON siteinfo (igloo_department_id) WHERE igloo_department_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_siteinfo_igloo_prop ON siteinfo (igloo_property_id) WHERE igloo_property_id IS NOT NULL;
