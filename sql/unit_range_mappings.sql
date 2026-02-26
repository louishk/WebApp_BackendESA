-- Reference DDL: unit_range_mappings table on esa_pbi
-- Maps unit number ranges to climate/NOKE/storage attributes from Excel Master sheet
-- Populated by: backend/python/scripts/populate_unit_range_mappings.py

CREATE TABLE IF NOT EXISTS unit_range_mappings (
    id               SERIAL PRIMARY KEY,
    site_code        VARCHAR(10) NOT NULL,   -- e.g. 'L017', 'L022'
    facility         VARCHAR(10) NOT NULL,   -- e.g. 'WD', 'TP', 'BK'
    unit_prefix      VARCHAR(10) NOT NULL DEFAULT '',  -- 'A', 'W', 'S', 'H', 'G', ''
    range_start      INTEGER NOT NULL,
    range_end        INTEGER NOT NULL,
    suffix_start     VARCHAR(5),             -- e.g. 'D' for 5000D-5000V range (NULL = any)
    suffix_end       VARCHAR(5),             -- e.g. 'V' for 5000D-5000V range (NULL = any)
    storage_type     VARCHAR(50),            -- 'Drive UP', 'W or WL', 'Executive'
    climate_type     VARCHAR(50) NOT NULL,   -- 'AIR-CON', 'NON AIR-CON', 'Refrigerated', etc.
    has_dehumidifier BOOLEAN NOT NULL DEFAULT FALSE,
    noke_status      VARCHAR(30) NOT NULL DEFAULT 'NO'  -- 'YES', 'NO', 'Entrance Only'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_urm_unique
    ON unit_range_mappings (site_code, unit_prefix, range_start, range_end,
                            COALESCE(suffix_start, ''), COALESCE(suffix_end, ''));
