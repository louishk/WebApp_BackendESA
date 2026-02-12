-- Migration 003: Inventory Checker Tables
-- Target database: backend (PostgreSQL)
-- Creates: inventory_type_mappings, inventory_unit_overrides

BEGIN;

-- ============================================================================
-- Table 1: inventory_type_mappings
-- Maps existing sTypeName values to SOP unit type codes
-- ============================================================================
CREATE TABLE IF NOT EXISTS inventory_type_mappings (
    id              SERIAL PRIMARY KEY,
    source_type_name VARCHAR(100) UNIQUE NOT NULL,
    mapped_type_code VARCHAR(10) NOT NULL,
    created_by      VARCHAR(255),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- Table 2: inventory_unit_overrides
-- Per-unit overrides for auto-calculated naming convention fields
-- NULL = use auto-calculated value
-- ============================================================================
CREATE TABLE IF NOT EXISTS inventory_unit_overrides (
    id              SERIAL PRIMARY KEY,
    site_id         INTEGER NOT NULL,
    unit_id         INTEGER NOT NULL,
    unit_type_code  VARCHAR(10),
    size_category   VARCHAR(5),
    size_range      VARCHAR(10),
    shape           VARCHAR(5),
    pillar          VARCHAR(5),
    updated_by      VARCHAR(255),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),

    UNIQUE(site_id, unit_id)
);

CREATE INDEX IF NOT EXISTS idx_inv_overrides_site ON inventory_unit_overrides(site_id);
CREATE INDEX IF NOT EXISTS idx_inv_overrides_site_unit ON inventory_unit_overrides(site_id, unit_id);

COMMIT;
