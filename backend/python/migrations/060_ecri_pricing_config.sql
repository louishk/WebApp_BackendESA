-- 060_ecri_pricing_config.sql
-- Singleton config row for the ECRI Pricing tool: gradient bounds + per-factor
-- enabled/weight definitions used to compute a per-ledger Proposed Increase %.
--
-- Only id=1 is ever read/written. Pattern keeps the schema trivial while
-- supporting audit (updated_by, updated_at) and atomic full-config replacement.

CREATE TABLE IF NOT EXISTS ecri_pricing_config (
    id          INTEGER PRIMARY KEY,
    config      JSONB     NOT NULL,
    updated_by  VARCHAR(255),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  ecri_pricing_config IS 'Singleton (id=1) row holding the active ECRI proposal algorithm config (gradient + factor weights). See docs/superpowers/specs/2026-05-13-ecri-pricing-proposal-algo-design.md.';
COMMENT ON COLUMN ecri_pricing_config.config IS 'Full config blob: {gradient_min_pct, gradient_max_pct, factors:{name:{enabled, weight}}}.';

INSERT INTO ecri_pricing_config (id, config, updated_by)
VALUES (
    1,
    jsonb_build_object(
        'gradient_min_pct', 3,
        'gradient_max_pct', 18,
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
            'red_bucket',            jsonb_build_object('enabled', true,  'weight', -0.4),
            'under_bumped',          jsonb_build_object('enabled', true,  'weight',  0.3),
            'cumul_bump_aggressive_annual', jsonb_build_object('enabled', true,  'weight', -0.4)
        ),
        'country_overrides', jsonb_build_object(
            'SG', jsonb_build_object('gradient_min_pct',  7, 'gradient_max_pct', 22),
            'MY', jsonb_build_object('gradient_min_pct',  6, 'gradient_max_pct', 20),
            'HK', jsonb_build_object('gradient_min_pct',  5, 'gradient_max_pct', 18),
            'KR', jsonb_build_object('gradient_min_pct',  1, 'gradient_max_pct',  5)
        )
    ),
    'system'
)
ON CONFLICT (id) DO NOTHING;
