-- 048_zoom_call_logs_scoring.sql
-- Add LLM-scoring columns to zoom_call_logs.
-- Hybrid design: flat columns for the dashboard-critical fields (sortable/filterable),
-- plus a JSONB blob for the rest so adding a new dimension via the rubric UI doesn't
-- need a schema change.
-- Target: esa_pbi

ALTER TABLE zoom_call_logs
    ADD COLUMN IF NOT EXISTS score_status         VARCHAR(20) NOT NULL DEFAULT 'none',
    ADD COLUMN IF NOT EXISTS score_processed_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS score_model          VARCHAR(50),
    ADD COLUMN IF NOT EXISTS score_confidence     SMALLINT,
    ADD COLUMN IF NOT EXISTS quality_overall      SMALLINT,
    ADD COLUMN IF NOT EXISTS call_category        VARCHAR(30),
    ADD COLUMN IF NOT EXISTS call_subcategory     VARCHAR(100),
    ADD COLUMN IF NOT EXISTS sentiment            VARCHAR(20),
    ADD COLUMN IF NOT EXISTS scores_json          JSONB,
    ADD COLUMN IF NOT EXISTS score_error          TEXT;

CREATE INDEX IF NOT EXISTS idx_zoom_call_logs_score_status ON zoom_call_logs(score_status);
CREATE INDEX IF NOT EXISTS idx_zoom_call_logs_category     ON zoom_call_logs(call_category);
CREATE INDEX IF NOT EXISTS idx_zoom_call_logs_sentiment    ON zoom_call_logs(sentiment);
