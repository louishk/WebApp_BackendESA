-- Migration 029: Add 'dry_run' to ecri_batch_ledgers.api_status CHECK constraint
-- Target database: esa_pbi
--
-- Context: adds an LSETUP dry-run mode to the ECRI execute endpoint so the
-- team can rehearse pushes on the test site without calling SMD SOAP. The
-- route writes api_status='dry_run' plus the would-be payload in api_response.

BEGIN;

ALTER TABLE ecri_batch_ledgers
    DROP CONSTRAINT IF EXISTS ecri_batch_ledgers_api_status_check;

ALTER TABLE ecri_batch_ledgers
    ADD CONSTRAINT ecri_batch_ledgers_api_status_check
    CHECK (api_status IN ('pending', 'success', 'failed', 'skipped', 'dry_run'));

COMMIT;
