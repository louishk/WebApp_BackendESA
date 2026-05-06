-- Migration 042: Add ECRI workflow permission columns to roles
-- Target DB: esa_backend (db: backend)

ALTER TABLE roles
    ADD COLUMN IF NOT EXISTS can_request_ecri_exclusion  BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS can_create_ecri_objection   BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS can_approve_ecri_objection  BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS can_finalize_ecri_batch     BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS can_execute_ecri_batch      BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS can_manage_ecri_reasons     BOOLEAN DEFAULT FALSE;

-- Roles that already have can_manage_ecri get all ECRI workflow permissions
UPDATE roles
SET
    can_request_ecri_exclusion  = TRUE,
    can_create_ecri_objection   = TRUE,
    can_approve_ecri_objection  = TRUE,
    can_finalize_ecri_batch     = TRUE,
    can_execute_ecri_batch      = TRUE,
    can_manage_ecri_reasons     = FALSE
WHERE can_manage_ecri = TRUE;

-- Admin additionally gets can_manage_ecri_reasons
UPDATE roles
SET can_manage_ecri_reasons = TRUE
WHERE name = 'admin';
