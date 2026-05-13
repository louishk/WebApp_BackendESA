-- 063_ecri_pricing_bump_factors.sql
-- Add bump-history-aware factors to the pricing config.
--
-- Distribution drove the design:
--   - 92%+ of tenants are "under-bumped" by the 1/year rule (portfolio
--     has been ECRI-starved historically). Modest +0.3 weight gives a
--     small tailwind without dominating max_pos.
--   - 12-23% of tenants in SG/MY/KR have annualized cumulative bumps
--     >= 7%/y (already aggressively pumped). -0.4 weight gives a real
--     headwind so we don't keep over-bumping the same cohort.

UPDATE ecri_pricing_config
SET config = jsonb_set(
    jsonb_set(
        config,
        '{factors,under_bumped}',
        jsonb_build_object('enabled', true, 'weight', 0.3)
    ),
    '{factors,cumul_bump_aggressive_annual}',
    jsonb_build_object('enabled', true, 'weight', -0.4)
),
updated_by = 'migration_063'
WHERE id = 1;
