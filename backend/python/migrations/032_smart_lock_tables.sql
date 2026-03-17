-- Smart Lock management tables
-- Run against: esa_backend DB
BEGIN;

-- Keypads: 3rd-party keypad identifiers assigned to sites
CREATE TABLE IF NOT EXISTS smart_lock_keypads (
    id              SERIAL PRIMARY KEY,
    keypad_id       VARCHAR(50) NOT NULL,
    site_id         INTEGER NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'not_assigned',
    notes           VARCHAR(255),
    created_by      VARCHAR(255),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(keypad_id)
);

CREATE INDEX IF NOT EXISTS idx_sl_keypads_site ON smart_lock_keypads(site_id);
CREATE INDEX IF NOT EXISTS idx_sl_keypads_status ON smart_lock_keypads(status);

-- Padlocks: 3rd-party padlock identifiers assigned to sites
CREATE TABLE IF NOT EXISTS smart_lock_padlocks (
    id              SERIAL PRIMARY KEY,
    padlock_id      VARCHAR(50) NOT NULL,
    site_id         INTEGER NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'not_assigned',
    notes           VARCHAR(255),
    created_by      VARCHAR(255),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(padlock_id)
);

CREATE INDEX IF NOT EXISTS idx_sl_padlocks_site ON smart_lock_padlocks(site_id);
CREATE INDEX IF NOT EXISTS idx_sl_padlocks_status ON smart_lock_padlocks(status);

-- Unit assignments: link keypads/padlocks to specific units
CREATE TABLE IF NOT EXISTS smart_lock_unit_assignments (
    id              SERIAL PRIMARY KEY,
    site_id         INTEGER NOT NULL,
    unit_id         INTEGER NOT NULL,
    keypad_pk       INTEGER REFERENCES smart_lock_keypads(id) ON DELETE SET NULL,
    padlock_pk      INTEGER REFERENCES smart_lock_padlocks(id) ON DELETE SET NULL,
    assigned_by     VARCHAR(255),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(site_id, unit_id)
);

-- Each keypad/padlock can only be assigned to one unit at a time
CREATE UNIQUE INDEX IF NOT EXISTS idx_sl_assign_keypad ON smart_lock_unit_assignments(keypad_pk) WHERE keypad_pk IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_sl_assign_padlock ON smart_lock_unit_assignments(padlock_pk) WHERE padlock_pk IS NOT NULL;

-- Audit log for all smart lock operations
CREATE TABLE IF NOT EXISTS smart_lock_audit_log (
    id              SERIAL PRIMARY KEY,
    action          VARCHAR(50) NOT NULL,
    entity_type     VARCHAR(20) NOT NULL,
    entity_id       VARCHAR(50),
    site_id         INTEGER,
    unit_id         INTEGER,
    detail          VARCHAR(500),
    username        VARCHAR(255) NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sl_audit_created ON smart_lock_audit_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sl_audit_action ON smart_lock_audit_log(action);
CREATE INDEX IF NOT EXISTS idx_sl_audit_site ON smart_lock_audit_log(site_id);

COMMIT;
