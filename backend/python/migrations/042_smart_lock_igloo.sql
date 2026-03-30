-- Migration: 042 — Smart Lock Igloo access codes + second keypad support
-- Adds keypad_2_pk column to smart_lock_unit_assignments
-- Creates igloo_access_codes table for Igloo lock PIN/eKey management
-- Target DB: esa_backend

-- Add optional second keypad reference to unit assignments
ALTER TABLE smart_lock_unit_assignments
    ADD COLUMN IF NOT EXISTS keypad_2_pk INTEGER REFERENCES smart_lock_keypads(id) ON DELETE SET NULL;

-- Igloo access codes (PINs and eKeys) linked to Bluetooth devices
CREATE TABLE IF NOT EXISTS igloo_access_codes (
    id               SERIAL PRIMARY KEY,
    device_id        VARCHAR(30)   NOT NULL,              -- Bluetooth deviceId
    access_id        VARCHAR(50)   NOT NULL,              -- Igloo access entry ID (for deletion)
    access_type      VARCHAR(10)   NOT NULL DEFAULT 'pin', -- 'pin' or 'ekey'
    pin_type         VARCHAR(20),                          -- 'permanent', 'duration', 'otp', 'daily', 'hourly'
    pin_enc          TEXT,                                  -- Fernet-encrypted PIN value
    name             VARCHAR(100),                          -- access label (tenant name, etc.)
    start_datetime   TIMESTAMPTZ,
    end_datetime     TIMESTAMPTZ,
    is_custom_pin    BOOLEAN       DEFAULT FALSE,
    site_id          INTEGER,
    updated_at       TIMESTAMPTZ   DEFAULT NOW(),
    created_at       TIMESTAMPTZ   DEFAULT NOW(),

    UNIQUE(device_id, access_id)
);

CREATE INDEX IF NOT EXISTS ix_igloo_access_codes_device_id ON igloo_access_codes (device_id);
CREATE INDEX IF NOT EXISTS ix_igloo_access_codes_site_id ON igloo_access_codes (site_id);
