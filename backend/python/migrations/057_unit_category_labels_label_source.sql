-- Migration 057: Add label_source column to unit_category_labels
-- Target database: esa_pbi
--
-- label_source values:
--   'parsed'   — written by backfill from a valid sTypeName parse (SOP format)
--   'legacy'   — written by backfill via inventory_type_mappings / unit_range_mappings fallback
--   'override' — written manually via /api/inventory/publish (Inventory Checker tool)
--
-- All pre-existing rows are defaulted to 'override' because they were published
-- through the Inventory Checker UI and represent deliberate human decisions.
-- The backfill script will NOT overwrite rows where label_source = 'override'.

BEGIN;

ALTER TABLE unit_category_labels
    ADD COLUMN IF NOT EXISTS label_source VARCHAR(10);

-- Treat every existing row as a manual override (written by the now-decommissioned
-- Inventory Checker tool). The backfill script skips rows with label_source = 'override'.
UPDATE unit_category_labels
   SET label_source = 'override'
 WHERE label_source IS NULL;

-- Partial index — speeds up the backfill conflict guard and any audit queries that
-- filter on source type (e.g. "show me all unparsed units").
CREATE INDEX IF NOT EXISTS idx_ucl_label_source
    ON unit_category_labels (label_source);

COMMENT ON COLUMN unit_category_labels.label_source IS
    'Origin of this label row: parsed (sTypeName parser), legacy (mapping table fallback), override (manual publish via UI).';

COMMIT;
