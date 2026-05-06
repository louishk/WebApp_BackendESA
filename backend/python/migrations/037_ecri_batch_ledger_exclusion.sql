-- Migration 037: Add per-row exclusion fields to ecri_batch_ledgers
-- Target DB: esa_pbi

ALTER TABLE ecri_batch_ledgers
    ADD COLUMN IF NOT EXISTS exclusion_status      VARCHAR(12) DEFAULT 'none'
        CHECK (exclusion_status IN ('none','requested','approved','rejected')),
    ADD COLUMN IF NOT EXISTS exclusion_reason_code VARCHAR(40),
    ADD COLUMN IF NOT EXISTS exclusion_notes       TEXT,
    ADD COLUMN IF NOT EXISTS exclusion_requested_by     INTEGER,
    ADD COLUMN IF NOT EXISTS exclusion_requested_at     TIMESTAMP,
    ADD COLUMN IF NOT EXISTS exclusion_decided_by       INTEGER,
    ADD COLUMN IF NOT EXISTS exclusion_decided_at       TIMESTAMP,
    ADD COLUMN IF NOT EXISTS exclusion_decision_notes   TEXT;

CREATE INDEX IF NOT EXISTS idx_ecri_bl_exclusion_status
    ON ecri_batch_ledgers (batch_id, exclusion_status);
