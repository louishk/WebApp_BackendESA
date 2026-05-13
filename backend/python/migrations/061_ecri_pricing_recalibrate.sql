-- 061_ecri_pricing_recalibrate.sql
-- Data-driven recalibration of ECRI pricing weights + add tenure_under_18mo /
-- tenure_36mo_plus factors. Disables tenure_under_24mo (kept for back-compat).
-- Also flips high_unit_risk to a mutually-exclusive band so it no longer
-- stacks with very_high_unit_risk.
--
-- Firing-rate analysis (17,190 active eligible ledgers) showed the v1 weights
-- were over-evaluated, e.g. above_market firing on 85% with weight -0.6.
-- See conversation log + spec for rationale.

UPDATE ecri_pricing_config
SET config = jsonb_build_object(
    'gradient_min_pct', (config->>'gradient_min_pct')::numeric,
    'gradient_max_pct', (config->>'gradient_max_pct')::numeric,
    'factors', jsonb_build_object(
        'below_market',          jsonb_build_object('enabled', true,  'weight',  0.9),
        'below_site_median',     jsonb_build_object('enabled', true,  'weight',  0.6),
        'below_country_median',  jsonb_build_object('enabled', true,  'weight',  0.5),
        'below_top3',            jsonb_build_object('enabled', true,  'weight',  0.4),
        'below_top1',            jsonb_build_object('enabled', true,  'weight',  0.3),
        'above_market',          jsonb_build_object('enabled', true,  'weight', -0.4),
        'above_site_median',     jsonb_build_object('enabled', true,  'weight', -0.5),
        'above_country_median',  jsonb_build_object('enabled', true,  'weight', -0.4),
        'above_top3',            jsonb_build_object('enabled', true,  'weight', -0.6),
        'above_top1',            jsonb_build_object('enabled', true,  'weight', -0.9),
        'high_unit_risk',        jsonb_build_object('enabled', true,  'weight', -0.5),
        'very_high_unit_risk',   jsonb_build_object('enabled', true,  'weight', -0.8),
        'tenure_under_18mo',     jsonb_build_object('enabled', true,  'weight', -0.5),
        'tenure_under_24mo',     jsonb_build_object('enabled', false, 'weight', -0.3),
        'tenure_36mo_plus',      jsonb_build_object('enabled', true,  'weight',  0.4),
        'red_bucket',            jsonb_build_object('enabled', true,  'weight', -0.4)
    )
),
updated_by = 'migration_061'
WHERE id = 1;
