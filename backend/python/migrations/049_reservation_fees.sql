-- Migration: 049 — Reservation fees per site (revenue tool)
-- Target DB: esa_backend

CREATE TABLE IF NOT EXISTS reservation_fees (
    id               SERIAL PRIMARY KEY,
    site_id          INTEGER NOT NULL UNIQUE,
    site_code        VARCHAR(10) NOT NULL,
    reservation_fee  NUMERIC(12, 2) NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by       VARCHAR(255),
    updated_by       VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS ix_reservation_fees_site_code ON reservation_fees (site_code);
