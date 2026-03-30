-- Migration 038: Create Igloo smart lock tables
-- Tables for syncing property and device data from Igloo API
-- Device join strategy: igloo_devices.deviceName = smart_lock_padlocks.padlock_id / smart_lock_keypads.keypad_id

BEGIN;

-- Igloo properties (site/location)
CREATE TABLE IF NOT EXISTS igloo_properties (
    id              SERIAL PRIMARY KEY,
    "propertyId"    VARCHAR(50) UNIQUE NOT NULL,
    name            VARCHAR(100) NOT NULL,
    timezone        VARCHAR(50),
    "totalLock"     INTEGER,
    site_id         INTEGER,
    raw_json        JSONB,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_igloo_prop_site_id ON igloo_properties (site_id);

-- Igloo devices (locks, keypads, bridges, keyboxes)
CREATE TABLE IF NOT EXISTS igloo_devices (
    id                  SERIAL PRIMARY KEY,
    "deviceId"          VARCHAR(30) UNIQUE NOT NULL,
    "deviceName"        VARCHAR(50) NOT NULL,
    type                VARCHAR(20) NOT NULL,
    igloo_id            VARCHAR(50),
    "batteryLevel"      INTEGER,
    "pairedAt"          TIMESTAMP WITH TIME ZONE,
    "lastSync"          TIMESTAMP WITH TIME ZONE,
    properties          JSONB,
    "linkedDevices"     JSONB,
    "linkedAccessories" JSONB,
    "propertyId"        VARCHAR(50),
    "propertyName"      VARCHAR(100),
    "departmentId"      VARCHAR(50),
    "departmentName"    VARCHAR(100),
    site_id             INTEGER,
    raw_json            JSONB,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_igloo_dev_device_name ON igloo_devices ("deviceName");
CREATE INDEX IF NOT EXISTS idx_igloo_dev_site_id ON igloo_devices (site_id);
CREATE INDEX IF NOT EXISTS idx_igloo_dev_property_id ON igloo_devices ("propertyId");

COMMIT;
