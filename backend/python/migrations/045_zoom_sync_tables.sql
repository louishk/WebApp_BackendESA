-- 045_zoom_sync_tables.sql
-- Zoom Phone <-> SugarCRM two-way sync tables
-- Target database: esa_pbi

-- SugarCRM -> Zoom External Contact mapping
CREATE TABLE IF NOT EXISTS zoom_contact_sync (
    id              SERIAL PRIMARY KEY,
    sugar_id        VARCHAR(36) NOT NULL,
    sugar_module    VARCHAR(20) NOT NULL,  -- 'Contacts' or 'Leads'
    zoom_contact_id VARCHAR(50),
    phone_numbers   JSONB,                 -- Snapshot of pushed phones
    name_pushed     VARCHAR(200),          -- Snapshot of name pushed to Zoom
    sync_status     VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending/synced/error/deleted
    error_message   TEXT,
    last_synced_at  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(sugar_id, sugar_module)
);

CREATE INDEX IF NOT EXISTS idx_zoom_contact_sync_status ON zoom_contact_sync(sync_status);
CREATE INDEX IF NOT EXISTS idx_zoom_contact_sync_zoom_id ON zoom_contact_sync(zoom_contact_id);

-- Zoom call record cache + CRM matching state
CREATE TABLE IF NOT EXISTS zoom_call_logs (
    id                   SERIAL PRIMARY KEY,
    zoom_call_id         VARCHAR(100) UNIQUE NOT NULL,
    direction            VARCHAR(10),        -- inbound/outbound
    caller_number        VARCHAR(30),        -- E.164
    callee_number        VARCHAR(30),        -- E.164
    caller_name          VARCHAR(200),
    callee_name          VARCHAR(200),
    duration             INTEGER,            -- seconds
    answer_start         TIMESTAMPTZ,
    call_end             TIMESTAMPTZ,
    has_recording        BOOLEAN DEFAULT FALSE,
    recording_id         VARCHAR(100),
    transcript           TEXT,
    matched_sugar_id     VARCHAR(36),
    matched_sugar_module VARCHAR(20),
    sugar_call_id        VARCHAR(36),        -- SugarCRM Call record ID
    sync_status          VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending/matched/pushed/no_match/error
    error_message        TEXT,
    raw_json             JSONB,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_zoom_call_logs_status ON zoom_call_logs(sync_status);
CREATE INDEX IF NOT EXISTS idx_zoom_call_logs_caller ON zoom_call_logs(caller_number);
CREATE INDEX IF NOT EXISTS idx_zoom_call_logs_callee ON zoom_call_logs(callee_number);
CREATE INDEX IF NOT EXISTS idx_zoom_call_logs_call_end ON zoom_call_logs(call_end);

-- High-water marks for sync state
CREATE TABLE IF NOT EXISTS zoom_sync_state (
    sync_name        VARCHAR(50) PRIMARY KEY,
    last_sync_at     TIMESTAMPTZ,
    last_success_at  TIMESTAMPTZ,
    records_processed INTEGER DEFAULT 0,
    metadata         JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed initial sync state rows
INSERT INTO zoom_sync_state (sync_name) VALUES ('contacts_push'), ('call_logs_pull')
ON CONFLICT (sync_name) DO NOTHING;
