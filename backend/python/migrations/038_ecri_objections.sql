-- Migration 038: Create ecri_objections table
-- Target DB: esa_pbi

CREATE TABLE IF NOT EXISTS ecri_objections (
    id                    BIGSERIAL PRIMARY KEY,
    batch_ledger_id       BIGINT NOT NULL REFERENCES ecri_batch_ledgers(id) ON DELETE CASCADE,
    batch_id              UUID NOT NULL,
    site_id               INTEGER NOT NULL,
    ledger_id             INTEGER NOT NULL,
    original_increase_pct NUMERIC(5,2) NOT NULL,
    original_new_rent     NUMERIC(14,4) NOT NULL,
    currency              VARCHAR(3) NOT NULL DEFAULT 'SGD',
    new_increase_pct      NUMERIC(5,2),
    new_new_rent          NUMERIC(14,4),
    reason_code           VARCHAR(40) NOT NULL,
    reason_notes          TEXT,
    status                VARCHAR(20) NOT NULL DEFAULT 'pending_approval'
                          CHECK (status IN ('pending_approval','approved','rejected','applied','cancelled')),
    requires_approval     BOOLEAN NOT NULL DEFAULT TRUE,
    raised_by_user_id     INTEGER NOT NULL,
    raised_by_username    VARCHAR(100) NOT NULL,
    raised_at             TIMESTAMP NOT NULL DEFAULT NOW(),
    approver_user_id      INTEGER,
    approver_username     VARCHAR(100),
    approved_at           TIMESTAMP,
    approval_notes        TEXT,
    applied_at            TIMESTAMP,
    applied_ret_code      VARCHAR(20),
    applied_ret_msg       TEXT
);

CREATE INDEX IF NOT EXISTS idx_ecri_obj_batch_ledger ON ecri_objections (batch_ledger_id);
CREATE INDEX IF NOT EXISTS idx_ecri_obj_batch_id ON ecri_objections (batch_id);
CREATE INDEX IF NOT EXISTS idx_ecri_obj_site_ledger ON ecri_objections (site_id, ledger_id);
CREATE INDEX IF NOT EXISTS idx_ecri_obj_status ON ecri_objections (status);
CREATE INDEX IF NOT EXISTS idx_ecri_obj_raised_by ON ecri_objections (raised_by_user_id);
