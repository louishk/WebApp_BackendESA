-- Migration 020: Create unit_category_labels staging table
-- Target database: esa_pbi
-- Stores computed final labels from Inventory Checker for reporting enrichment
-- Temporary until SiteLink has proper category fields

BEGIN;

CREATE TABLE IF NOT EXISTS unit_category_labels (
    site_id         INTEGER NOT NULL,
    unit_id         INTEGER NOT NULL,
    size_category   VARCHAR(5),
    size_range      VARCHAR(10),
    unit_type_code  VARCHAR(10),
    climate_code    VARCHAR(5),
    shape           VARCHAR(5),
    pillar          VARCHAR(5),
    final_label     VARCHAR(30) NOT NULL,
    published_by    VARCHAR(255),
    published_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (site_id, unit_id)
);

CREATE INDEX IF NOT EXISTS idx_ucl_site ON unit_category_labels(site_id);

COMMENT ON TABLE unit_category_labels IS
    'Staging table: computed inventory labels from Inventory Checker tool. '
    'Temporary until SiteLink has proper category fields.';

COMMIT;
