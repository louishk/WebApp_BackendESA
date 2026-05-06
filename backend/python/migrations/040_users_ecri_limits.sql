-- Migration 040: Add per-user ECRI approval limits
-- Target DB: esa_backend (db: backend)

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS ecri_entitled           BOOLEAN        DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS ecri_max_pct_reduction  NUMERIC(5,2)   DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ecri_max_abs_reduction  NUMERIC(14,4)  DEFAULT 0;

COMMENT ON COLUMN users.ecri_entitled IS
    'User is allowed to participate in ECRI workflow (request exclusions / objections).';
COMMENT ON COLUMN users.ecri_max_pct_reduction IS
    'Max percentage-point reduction the user can auto-approve/apply on an objection.';
COMMENT ON COLUMN users.ecri_max_abs_reduction IS
    'Max absolute rent reduction (native currency) the user can auto-approve/apply on an objection.';
