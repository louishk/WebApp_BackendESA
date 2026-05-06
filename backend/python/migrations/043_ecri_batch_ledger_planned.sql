-- Migration 043: Snapshot planned values on ecri_batch_ledgers
-- Target database: esa_pbi
--
-- Context: once an objection is applied, `new_rent` / `increase_pct` /
-- `increase_amt` are overwritten with the delivered values. We need the
-- original planned values preserved for performance analytics
-- (plan-vs-delivered, revenue leakage per reason, etc.).

BEGIN;

ALTER TABLE ecri_batch_ledgers
    ADD COLUMN IF NOT EXISTS planned_new_rent     NUMERIC(14, 4),
    ADD COLUMN IF NOT EXISTS planned_increase_pct NUMERIC(5, 2),
    ADD COLUMN IF NOT EXISTS planned_increase_amt NUMERIC(14, 4);

-- Backfill: for every existing row, set planned_* = current values.
-- These rows predate the objection mechanism so current = planned.
UPDATE ecri_batch_ledgers
   SET planned_new_rent     = new_rent,
       planned_increase_pct = increase_pct,
       planned_increase_amt = increase_amt
 WHERE planned_new_rent IS NULL;

COMMIT;
