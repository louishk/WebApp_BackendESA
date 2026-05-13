-- 065_ecri_pricing_low_risk_factors.sql
-- Add symmetric positive low-risk factors. The current config penalised
-- high/very-high risk but had no upside for low/very-low risk units —
-- yet 33-58% of MY/SG/KR units sit below risk=0.9 (good categories that
-- should support a fuller bump).
--
-- Mutually exclusive bands matching the existing high/very_high split:
--   very_low: risk < 0.70   →  +0.8 (mirrors very_high -0.8)
--   low:      0.70 ≤ risk < 0.90  →  +0.5 (mirrors high -0.5)

UPDATE ecri_pricing_config
SET config = jsonb_set(
    jsonb_set(
        config,
        '{factors,low_unit_risk}',
        jsonb_build_object('enabled', true, 'weight', 0.5)
    ),
    '{factors,very_low_unit_risk}',
    jsonb_build_object('enabled', true, 'weight', 0.8)
),
updated_by = 'migration_065'
WHERE id = 1;
