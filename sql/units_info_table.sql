-- ============================================
-- Units Info Table for esa_pbi Database
-- ============================================
-- This table stores unit information fetched from SOAP API
-- Run this script on the esa_pbi database

CREATE TABLE IF NOT EXISTS units_info (
    "SiteID" VARCHAR(20) NOT NULL,
    "UnitID" VARCHAR(50) NOT NULL,
    "UnitTypeID" VARCHAR(50),
    "sUnit" VARCHAR(100),
    "sUnitName" VARCHAR(255),
    "sTypeName" VARCHAR(255),
    "sSize" VARCHAR(50),
    "Area" NUMERIC(10, 2),
    "dcWidth" NUMERIC(10, 2),
    "dcLength" NUMERIC(10, 2),
    "iFloor" INTEGER,
    "bClimate" BOOLEAN,
    "bPower" BOOLEAN,
    "bInside" BOOLEAN,
    "bAlarm" BOOLEAN,
    "bRentable" BOOLEAN,
    "dcStdRate" NUMERIC(12, 2),
    "dcWebRate" NUMERIC(12, 2),
    "dcPushRate" NUMERIC(12, 2),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    PRIMARY KEY ("SiteID", "UnitID")
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_units_info_site ON units_info("SiteID");
CREATE INDEX IF NOT EXISTS idx_units_info_unit_type ON units_info("UnitTypeID");
CREATE INDEX IF NOT EXISTS idx_units_info_rentable ON units_info("bRentable");

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_units_info_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_units_info_updated_at ON units_info;
CREATE TRIGGER update_units_info_updated_at
    BEFORE UPDATE ON units_info
    FOR EACH ROW EXECUTE FUNCTION update_units_info_updated_at();

COMMENT ON TABLE units_info IS 'Unit information from SOAP API - types, sizes, pricing';
COMMENT ON COLUMN units_info."SiteID" IS 'Site/Location identifier';
COMMENT ON COLUMN units_info."UnitID" IS 'Unique unit identifier';
COMMENT ON COLUMN units_info."sUnit" IS 'Unit code/number';
COMMENT ON COLUMN units_info."sTypeName" IS 'Unit type name (e.g., Small, Medium, Large)';
COMMENT ON COLUMN units_info."dcStdRate" IS 'Standard rental rate';
COMMENT ON COLUMN units_info."dcWebRate" IS 'Web advertised rate';
COMMENT ON COLUMN units_info."bRentable" IS 'Whether unit is available for rent';
