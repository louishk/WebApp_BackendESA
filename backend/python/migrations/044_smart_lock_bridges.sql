-- Smart Lock bridges — auto-populated from Igloo /devices (type='Bridge')
-- Run against: esa_middleware DB
BEGIN;

CREATE TABLE IF NOT EXISTS mw_smart_lock_bridges (
    id              SERIAL PRIMARY KEY,
    bridge_id       VARCHAR(50) NOT NULL,
    site_id         INTEGER NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'not_assigned',
    notes           VARCHAR(255),
    created_by      VARCHAR(255),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(bridge_id)
);

CREATE INDEX IF NOT EXISTS idx_mw_sl_bridges_site ON mw_smart_lock_bridges(site_id);
CREATE INDEX IF NOT EXISTS idx_mw_sl_bridges_status ON mw_smart_lock_bridges(status);

COMMIT;
