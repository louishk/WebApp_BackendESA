-- 046_zoom_agent_mapping.sql
-- Mini mapping table for Zoom Phone users <-> SugarCRM users
-- Used by zoom_call_log_sync to assign pushed Call records to the right agent.

CREATE TABLE IF NOT EXISTS zoom_agent_mapping (
    id                  SERIAL PRIMARY KEY,
    zoom_user_id        VARCHAR(50) UNIQUE NOT NULL,  -- caller_user_id / callee_user_id
    zoom_email          VARCHAR(255),                  -- caller_email / callee_email (for human ref)
    zoom_name           VARCHAR(200),                  -- caller_name / callee_name (for human ref)
    sugar_user_id       VARCHAR(36),                    -- SugarCRM Users.id (nullable for unmatched stubs)
    sugar_user_name     VARCHAR(200),                  -- SugarCRM display name (for human ref)
    enabled             BOOLEAN NOT NULL DEFAULT TRUE, -- toggle to disable assignment
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_zoom_agent_mapping_email ON zoom_agent_mapping(zoom_email);
CREATE INDEX IF NOT EXISTS idx_zoom_agent_mapping_sugar ON zoom_agent_mapping(sugar_user_id);
