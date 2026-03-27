-- Gate Access Data from SMD GateAccessData SOAP endpoint
-- Stores per-unit gate access info with encrypted access codes
-- Target DB: esa_backend

CREATE TABLE IF NOT EXISTS gate_access_data (
    id              SERIAL PRIMARY KEY,
    location_code   VARCHAR(10)  NOT NULL,          -- sLocationCode (L001, L002, etc.)
    site_id         INTEGER      NOT NULL,          -- Numeric SiteID (48, 49, etc.)
    unit_id         INTEGER      NOT NULL,          -- StorageMaker UnitID
    unit_name       VARCHAR(50)  NOT NULL,          -- sUnitName
    is_rented       BOOLEAN      NOT NULL DEFAULT false,
    access_code_enc TEXT,                            -- Fernet-encrypted sAccessCode
    access_code2_enc TEXT,                           -- Fernet-encrypted sAccessCode2
    is_gate_locked  BOOLEAN      NOT NULL DEFAULT false,
    is_overlocked   BOOLEAN      NOT NULL DEFAULT false,
    keypad_zone     INTEGER      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_gate_access_loc_unit UNIQUE (location_code, unit_id)
);

CREATE INDEX IF NOT EXISTS ix_gate_access_location_code ON gate_access_data (location_code);
CREATE INDEX IF NOT EXISTS ix_gate_access_site_id ON gate_access_data (site_id);
CREATE INDEX IF NOT EXISTS ix_gate_access_unit_name ON gate_access_data (location_code, unit_name);
