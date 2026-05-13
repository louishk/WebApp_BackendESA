-- 060_add_pipeline_destinations.sql
-- Adds a JSONB `destinations` column to mw_sync_pipelines so a pipeline
-- can declare multiple write targets (e.g. ccws_units → middleware.ccws_units
-- AND pbi.units_info). When NULL or empty, the orchestrator UI falls back
-- to the legacy single freshness_table/column/database fields.
--
-- Shape:
--   [
--     {"database": "middleware", "table": "ccws_units", "column": "updated_at"},
--     {"database": "pbi",        "table": "units_info", "column": "updated_at"}
--   ]
--
-- Observability-only — pipeline code still writes wherever it was hardcoded.
-- This column drives the dashboard's "one card per destination" display and
-- the per-destination freshness query.
--
-- Target DB: esa_middleware

ALTER TABLE mw_sync_pipelines
    ADD COLUMN IF NOT EXISTS destinations JSONB;

COMMENT ON COLUMN mw_sync_pipelines.destinations IS
    'List of {database, table, column} write targets. NULL = use single freshness_* fields.';

-- Seed ccws_units with both destinations (current dual-write pattern).
UPDATE mw_sync_pipelines
SET destinations = '[
    {"database": "middleware", "table": "ccws_units",  "column": "updated_at"},
    {"database": "pbi",        "table": "units_info",  "column": "updated_at"}
]'::jsonb
WHERE pipeline_name = 'ccws_units';
