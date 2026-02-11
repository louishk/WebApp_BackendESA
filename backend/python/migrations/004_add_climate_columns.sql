BEGIN;
ALTER TABLE inventory_type_mappings ADD COLUMN IF NOT EXISTS mapped_climate_code VARCHAR(5);
ALTER TABLE inventory_unit_overrides ADD COLUMN IF NOT EXISTS climate_code VARCHAR(5);
COMMIT;
