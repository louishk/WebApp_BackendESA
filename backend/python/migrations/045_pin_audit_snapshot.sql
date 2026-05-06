-- Latest pin-audit result per (site, unit, keypad slot). Upserted on every
-- /api/smart-lock/pin-audit call so the assignments page can show the prior
-- audit + when it was run, without forcing a fresh Igloo round-trip on load.
-- Run against: esa_middleware DB
BEGIN;

CREATE TABLE IF NOT EXISTS mw_smart_lock_pin_audit_snapshot (
    id              BIGSERIAL PRIMARY KEY,
    site_id         INTEGER NOT NULL,
    unit_id         INTEGER NOT NULL,
    keypad_pk       INTEGER,
    keypad_slot     VARCHAR(20),
    device_id       VARCHAR(50),
    status          VARCHAR(30) NOT NULL,
    reason          VARCHAR(30),
    is_rented       BOOLEAN,
    is_gate_locked  BOOLEAN,
    is_overlocked   BOOLEAN,
    b_rentable      BOOLEAN,
    has_gate_code   BOOLEAN,
    has_esa_pin     BOOLEAN,
    pin_type        VARCHAR(20),
    audited_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    audited_by      VARCHAR(255)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_pin_audit_snap_unit_slot
    ON mw_smart_lock_pin_audit_snapshot (site_id, unit_id, keypad_slot);
CREATE INDEX IF NOT EXISTS idx_pin_audit_snap_site
    ON mw_smart_lock_pin_audit_snapshot (site_id);
CREATE INDEX IF NOT EXISTS idx_pin_audit_snap_audited_at
    ON mw_smart_lock_pin_audit_snapshot (audited_at DESC);

COMMIT;
