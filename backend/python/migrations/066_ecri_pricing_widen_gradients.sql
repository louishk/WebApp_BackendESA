-- 066_ecri_pricing_widen_gradients.sql
-- Adding low_unit_risk/very_low_unit_risk positive factors expanded max_pos
-- from 3.6 → 5.2, diluting the per-factor contribution of the existing
-- positives. The most-impacted country was HK (-1.6 pts) because no HK
-- unit fires the new low-risk factors (all HK units sit at risk=1.0).
--
-- Widening SG/MY/HK gradient maxes compensates the dilution:
--   SG: 7-22 → 7-25 (recovers +0.25 pts rev-wgt avg)
--   MY: 6-20 → 6-22 (recovers +0.21 pts)
--   HK: 5-18 → 5-22 (recovers +1.06 pts)
-- KR untouched — already over budget by design (1% floor exceeds 0.75% need).

UPDATE ecri_pricing_config
SET config = jsonb_set(
    jsonb_set(
        jsonb_set(
            config,
            '{country_overrides,SG}',
            jsonb_build_object('gradient_min_pct', 7, 'gradient_max_pct', 25)
        ),
        '{country_overrides,MY}',
        jsonb_build_object('gradient_min_pct', 6, 'gradient_max_pct', 22)
    ),
    '{country_overrides,HK}',
    jsonb_build_object('gradient_min_pct', 5, 'gradient_max_pct', 22)
),
updated_by = 'migration_066'
WHERE id = 1;
