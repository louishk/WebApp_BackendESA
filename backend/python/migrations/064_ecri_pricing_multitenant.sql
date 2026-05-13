-- 064_ecri_pricing_multitenant.sql
-- Add multitenant concentration-risk factor.
--
-- Empirical: 10-15% of unique tenants rent 2+ active ledgers but they
-- account for 22-32% of all ledger-lines. Their churn impacts multiple
-- units at once, so we soften proposed increases for this cohort.

UPDATE ecri_pricing_config
SET config = jsonb_set(
    config,
    '{factors,multitenant}',
    jsonb_build_object('enabled', true, 'weight', -0.3)
),
updated_by = 'migration_064'
WHERE id = 1;
