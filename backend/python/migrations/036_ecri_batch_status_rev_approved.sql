-- Migration 036: Expand ecri_batches status CHECK + add site_review_deadline
-- Target DB: esa_pbi

-- Drop the old status constraint (name may vary; use pg_constraint lookup)
DO $$
DECLARE
    v_constraint_name TEXT;
BEGIN
    SELECT conname INTO v_constraint_name
    FROM pg_constraint
    WHERE conrelid = 'ecri_batches'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%status%';

    IF v_constraint_name IS NOT NULL THEN
        EXECUTE 'ALTER TABLE ecri_batches DROP CONSTRAINT ' || quote_ident(v_constraint_name);
    END IF;
END$$;

-- Re-add with full status set including rev_approved and site_review
ALTER TABLE ecri_batches
    ADD CONSTRAINT ecri_batches_status_check
        CHECK (status IN ('draft','site_review','rev_approved','executing','executed','cancelled'));

-- Add site_review_deadline column
ALTER TABLE ecri_batches
    ADD COLUMN IF NOT EXISTS site_review_deadline DATE;

-- Add submitted_for_review_at for audit trail
ALTER TABLE ecri_batches
    ADD COLUMN IF NOT EXISTS submitted_for_review_at TIMESTAMP;

ALTER TABLE ecri_batches
    ADD COLUMN IF NOT EXISTS submitted_for_review_by VARCHAR(255);
