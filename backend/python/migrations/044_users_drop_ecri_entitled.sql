-- Migration 044: Drop redundant users.ecri_entitled flag
-- Target database: esa_backend
--
-- Context: the ecri_entitled boolean was a master-switch per user to gate
-- ECRI access. In practice it's redundant with the role permissions
-- (can_request_ecri_exclusion, can_create_ecri_objection, etc.). Removing
-- it simplifies the permission model.

BEGIN;

ALTER TABLE users DROP COLUMN IF EXISTS ecri_entitled;

COMMIT;
