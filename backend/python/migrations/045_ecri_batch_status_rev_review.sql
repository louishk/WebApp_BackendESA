-- Migration 045: Add 'rev_review' status to ecri_batches
-- Target database: esa_pbi
--
-- Context: the original workflow combined "ops flags exclusions" and
-- "Revenue approves/rejects" inside a single 'site_review' status. We now
-- split them so Approve/Reject only appears once ops has closed their
-- review. New flow:
--   draft → site_review → rev_review → rev_approved → executing → executed

BEGIN;

ALTER TABLE ecri_batches DROP CONSTRAINT IF EXISTS ecri_batches_status_check;
ALTER TABLE ecri_batches ADD CONSTRAINT ecri_batches_status_check
    CHECK (status IN ('draft', 'site_review', 'rev_review', 'rev_approved', 'executing', 'executed', 'cancelled'));

COMMIT;
