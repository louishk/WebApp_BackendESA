-- Drop orphan inventory_unit_overrides table from esa_backend
--
-- Context: per-unit override store written by the old Inventory Checker tool
-- (POST /api/inventory/overrides). The tool, its template, and all six
-- /api/inventory/* routes were removed 2026-05-15. No other reader/writer
-- exists in the codebase.
--
-- Kept (NOT dropped) by this migration:
--   - inventory_type_mappings (esa_backend) — still read by
--     discount_plans._load_legacy_type_map_cached and the
--     mw_unit_discount_candidates pipeline as SOP-fallback for unmigrated
--     sites (HK permanent; L028/L029 pending June 2026 migration; L031 floor 4).
--   - unit_category_labels (esa_pbi) — still read by vw_units_inventory,
--     units_info_enriched, pricing_anomalies, visits, pricing_recalibration.
--     Now write-frozen (no more publish-labels endpoint).
--
-- Run against esa_backend.

BEGIN;

DROP TABLE IF EXISTS inventory_unit_overrides CASCADE;

COMMIT;
