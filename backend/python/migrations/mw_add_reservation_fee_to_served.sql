-- Migration: add reservation_fee columns to mw_recommendations_served
-- Run against esa_middleware DB (the mw_* tables live there, not esa_backend).
-- Safe to run on a live app — all columns are nullable, no default required.

ALTER TABLE mw_recommendations_served
    ADD COLUMN IF NOT EXISTS slot1_reservation_fee        NUMERIC(12,2),
    ADD COLUMN IF NOT EXISTS slot1_reservation_fee_source VARCHAR(20),
    ADD COLUMN IF NOT EXISTS slot2_reservation_fee        NUMERIC(12,2),
    ADD COLUMN IF NOT EXISTS slot2_reservation_fee_source VARCHAR(20),
    ADD COLUMN IF NOT EXISTS slot3_reservation_fee        NUMERIC(12,2),
    ADD COLUMN IF NOT EXISTS slot3_reservation_fee_source VARCHAR(20);
