-- 062_ecri_pricing_country_overrides.sql
-- Extend the pricing config with optional per-country gradient overrides.
-- Required because budget targets and churn rates differ materially by
-- market: SG needs ~8% mean ECRI to hit budget after 16.5% churn, MY ~7.5%,
-- KR less than 1% to avoid overshooting a near-flat budget.
--
-- Seeds the four operating countries with calibrated gradients. Global
-- gradient remains the fallback for any country not listed.

UPDATE ecri_pricing_config
SET config = config || jsonb_build_object(
    'country_overrides', jsonb_build_object(
        'SG', jsonb_build_object('gradient_min_pct',  7, 'gradient_max_pct', 22),
        'MY', jsonb_build_object('gradient_min_pct',  6, 'gradient_max_pct', 20),
        'HK', jsonb_build_object('gradient_min_pct',  5, 'gradient_max_pct', 18),
        'KR', jsonb_build_object('gradient_min_pct',  1, 'gradient_max_pct',  5)
    )
),
updated_by = 'migration_062'
WHERE id = 1;
