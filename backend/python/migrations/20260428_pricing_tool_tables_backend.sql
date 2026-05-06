-- Migration: Pricing Tool — Core Tables
-- Target database: esa_backend
-- Purpose: Creates all nine tables for the rule-based pricing engine and AI
--          review layer (plan sections 5.4 and 6.5).
-- Safe to re-run: all CREATE TABLE statements use IF NOT EXISTS.
-- site_id / unit_id are BIGINT to align with esa_pbi units/siteinfo integer keys
-- (both tables use plain INTEGER in esa_pbi; BIGINT is a safe superset and
-- avoids a migration if those tables grow beyond INT range).
--
-- NOTE: No cross-DB foreign keys are defined. site_id and unit_id reference
-- esa_pbi tables (units_info, siteinfo) which live in a separate PostgreSQL
-- database and cannot be enforced via FK constraints.

BEGIN;

-- =============================================================================
-- Shared trigger function: auto-stamp updated_at on any UPDATE
-- Pattern follows migrations 041 and 047.
-- =============================================================================
CREATE OR REPLACE FUNCTION pricing_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 1. pricing_site_config
--    One row per site. Stores the per-site pricing engine parameters and AI
--    tuning knobs. NULL rows fall back to corp defaults (not stored here).
-- =============================================================================
CREATE TABLE IF NOT EXISTS pricing_site_config (
    -- NOTE: site_id matches SiteID (INTEGER) in esa_pbi.siteinfo. Using BIGINT
    --       as a safe superset; no FK can be declared across DBs.
    site_id                     BIGINT          PRIMARY KEY,

    -- Rule engine parameters
    rate_per_sqft               NUMERIC(12,4)   NOT NULL,
    -- NOTE: weekly_ratio default 1.25 per plan §4.2.1. Stored as NUMERIC(6,3)
    --       (e.g. 1.250) — consistent with percent-precision fields.
    weekly_ratio                NUMERIC(6,3)    NOT NULL DEFAULT 1.250,
    rounding_step               NUMERIC(12,4)   NOT NULL DEFAULT 5,

    -- AI adjustment limits
    -- NOTE: ai_max_adjustment_pct is a signed percent cap (e.g. 10 = ±10%).
    ai_max_adjustment_pct       NUMERIC(6,3)    NOT NULL DEFAULT 10.000,
    -- Auto-apply: skip human review when confidence >= threshold AND |adj| <= max_pct
    ai_autoapply_threshold      NUMERIC(6,3),   -- NULL = auto-apply disabled
    ai_autoapply_max_pct        NUMERIC(6,3),   -- NULL = auto-apply disabled

    -- Composite confidence weights (must sum to 1.0; validated at app layer)
    -- Keys: signal_strength, data_quality, historical_accuracy, model_self_report
    ai_confidence_weights       JSONB           NOT NULL
                                    DEFAULT '{"signal_strength":0.40,"data_quality":0.30,"historical_accuracy":0.20,"model_self_report":0.10}'::jsonb,

    -- Which Claude deployment tier to use for this site's nightly reviews
    ai_model_tier               VARCHAR(10)     NOT NULL DEFAULT 'primary'
                                    CHECK (ai_model_tier IN ('primary', 'fast')),

    -- Confirmation modal threshold: changes beyond this % trigger extra confirm
    confirmation_threshold_pct  NUMERIC(6,3)    NOT NULL DEFAULT 5.000,

    -- Audit
    updated_by                  VARCHAR(255),
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_pricing_site_config_updated_at ON pricing_site_config;
CREATE TRIGGER trg_pricing_site_config_updated_at
    BEFORE UPDATE ON pricing_site_config
    FOR EACH ROW EXECUTE FUNCTION pricing_set_updated_at();

-- =============================================================================
-- 2. pricing_range_config
--    One row per (site_id, range_code). NULL site_id = corp-level default.
--    range_code corresponds to the COM01 Unit Size Range code (e.g. '90-110').
-- =============================================================================
CREATE TABLE IF NOT EXISTS pricing_range_config (
    id                          SERIAL          PRIMARY KEY,

    -- NULL site_id = corp default; NOT NULL = site-level override
    site_id                     BIGINT,
    range_code                  VARCHAR(50)     NOT NULL,

    -- ref_sqft resolution strategy
    ref_sqft_strategy           VARCHAR(10)     NOT NULL DEFAULT 'lowest'
                                    CHECK (ref_sqft_strategy IN ('lowest', 'highest', 'midpoint', 'manual')),
    -- Used only when ref_sqft_strategy = 'manual'
    ref_sqft_value              NUMERIC(12,4),

    -- Optional: split range into sub-bands of this many sqft
    -- NULL = no sub-banding (treat whole range as one anchor)
    -- NOTE: NUMERIC(12,4) for sqft; integer semantics but NUMERIC avoids cast issues.
    floor_step                  NUMERIC(12,4),
    min_floor_sqft              NUMERIC(12,4),

    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_pricing_range_config UNIQUE (site_id, range_code)
);

DROP TRIGGER IF EXISTS trg_pricing_range_config_updated_at ON pricing_range_config;
CREATE TRIGGER trg_pricing_range_config_updated_at
    BEFORE UPDATE ON pricing_range_config
    FOR EACH ROW EXECUTE FUNCTION pricing_set_updated_at();

CREATE INDEX IF NOT EXISTS idx_pricing_range_config_site
    ON pricing_range_config (site_id, range_code);

-- =============================================================================
-- 3. pricing_modifier_config
--    One row per (site_id, component, code).
--    NULL site_id = corp default. component ∈ {climate, type, shape, pillar,
--    size_cat, case_count}. code is the COM01 shorthand (e.g. 'NC', 'A').
-- =============================================================================
CREATE TABLE IF NOT EXISTS pricing_modifier_config (
    id                          SERIAL          PRIMARY KEY,

    site_id                     BIGINT,
    component                   VARCHAR(20)     NOT NULL
                                    CHECK (component IN ('climate', 'type', 'shape', 'pillar', 'size_cat', 'case_count')),
    code                        VARCHAR(20)     NOT NULL,

    -- Signed percentage modifier applied additively to the type multiplier.
    -- Allowed range: -100 to +100 (enforced; wider values make no product sense).
    pct_modifier                NUMERIC(6,3)    NOT NULL
                                    CHECK (pct_modifier BETWEEN -100 AND 100),

    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_pricing_modifier_config UNIQUE (site_id, component, code)
);

DROP TRIGGER IF EXISTS trg_pricing_modifier_config_updated_at ON pricing_modifier_config;
CREATE TRIGGER trg_pricing_modifier_config_updated_at
    BEFORE UPDATE ON pricing_modifier_config
    FOR EACH ROW EXECUTE FUNCTION pricing_set_updated_at();

CREATE INDEX IF NOT EXISTS idx_pricing_modifier_config_site
    ON pricing_modifier_config (site_id, component);

-- =============================================================================
-- 4. pricing_recommendations
--    Output of the nightly rule-based engine. One row per (site, type, run).
--    unit_id is nullable: NULL = type-level rollup row (one per sTypeName);
--    NOT NULL = per-unit row (used by the override / bulk paths).
-- =============================================================================
CREATE TABLE IF NOT EXISTS pricing_recommendations (
    id                          BIGSERIAL       PRIMARY KEY,

    site_id                     BIGINT          NOT NULL,
    s_type_name                 VARCHAR(100)    NOT NULL,
    unit_id                     BIGINT,         -- NULL = type-level rollup

    recommended_monthly         NUMERIC(12,4),
    recommended_weekly          NUMERIC(12,4),
    recommended_web             NUMERIC(12,4),

    -- Full breakdown of inputs used to produce this recommendation (JSON blob)
    inputs_json                 JSONB,

    -- Pricing engine version tag for auditability (e.g. 'v1.0', 'v1.1-floor-step')
    algo_version                VARCHAR(50)     NOT NULL DEFAULT 'v1.0',

    generated_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Lifecycle: pending -> accepted / rejected / superseded
    status                      VARCHAR(20)     NOT NULL DEFAULT 'pending'
                                    CHECK (status IN ('pending', 'accepted', 'rejected', 'superseded')),

    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_pricing_recommendations_updated_at ON pricing_recommendations;
CREATE TRIGGER trg_pricing_recommendations_updated_at
    BEFORE UPDATE ON pricing_recommendations
    FOR EACH ROW EXECUTE FUNCTION pricing_set_updated_at();

CREATE INDEX IF NOT EXISTS idx_pricing_recs_site_type
    ON pricing_recommendations (site_id, s_type_name);
CREATE INDEX IF NOT EXISTS idx_pricing_recs_site_generated
    ON pricing_recommendations (site_id, generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_pricing_recs_status
    ON pricing_recommendations (status) WHERE status = 'pending';

-- =============================================================================
-- 5. units_pricing_override
--    Per-unit manual overrides. The nightly recomputer skips rows where
--    is_manual = TRUE so human-set prices are preserved across engine runs.
-- =============================================================================
CREATE TABLE IF NOT EXISTS units_pricing_override (
    -- NOTE: unit_id matches UnitID (INTEGER) in esa_pbi.units_info.
    unit_id                     BIGINT          PRIMARY KEY,

    std                         NUMERIC(12,4),
    web                         NUMERIC(12,4),
    monthly                     NUMERIC(12,4),
    weekly                      NUMERIC(12,4),

    -- When TRUE the recompute engine skips this unit entirely
    is_manual                   BOOLEAN         NOT NULL DEFAULT FALSE,

    set_by                      VARCHAR(255),
    set_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_units_pricing_override_updated_at ON units_pricing_override;
CREATE TRIGGER trg_units_pricing_override_updated_at
    BEFORE UPDATE ON units_pricing_override
    FOR EACH ROW EXECUTE FUNCTION pricing_set_updated_at();

-- Partial index: fast lookup of manual-only overrides (used by recompute skip logic)
CREATE INDEX IF NOT EXISTS idx_units_pricing_override_manual
    ON units_pricing_override (unit_id) WHERE is_manual = TRUE;

-- =============================================================================
-- 6. pricing_ai_reviews
--    One row per AI call. Generated by the nightly AI review job.
--    Links to a pricing_recommendations row (the baseline the AI was given).
-- =============================================================================
CREATE TABLE IF NOT EXISTS pricing_ai_reviews (
    id                          BIGSERIAL       PRIMARY KEY,

    -- The baseline recommendation this AI review was generated against
    recommendation_id           BIGINT          NOT NULL
                                    REFERENCES pricing_recommendations (id) ON DELETE CASCADE,

    -- Convenience denormalization to avoid joining recommendations for common filters
    site_id                     BIGINT          NOT NULL,
    s_type_name                 VARCHAR(100)    NOT NULL,

    -- AI output fields
    ai_action                   VARCHAR(30)     NOT NULL
                                    CHECK (ai_action IN ('hold', 'nudge_up', 'push_up_strong', 'nudge_down', 'push_down_strong')),
    ai_raw_adjustment_pct       NUMERIC(6,3)    NOT NULL,   -- raw value from LLM output
    ai_clamped                  BOOLEAN         NOT NULL DEFAULT FALSE, -- TRUE if raw was clamped to site limit
    ai_adjustment_pct           NUMERIC(6,3)    NOT NULL,   -- after clamping to ±ai_max_adjustment_pct

    -- Composite confidence sub-scores (each 0–1)
    confidence_signal           NUMERIC(6,4)    NOT NULL,   -- signal_strength sub-score
    confidence_data             NUMERIC(6,4)    NOT NULL,   -- data_quality sub-score
    confidence_history          NUMERIC(6,4)    NOT NULL,   -- historical_accuracy sub-score
    confidence_model            NUMERIC(6,4)    NOT NULL,   -- model self-reported confidence
    confidence_composite        NUMERIC(6,4)    NOT NULL,   -- weighted composite (0–1)

    ai_reasoning_text           TEXT,

    -- Full prompt inputs snapshotted for audit / backtest replay
    inputs_snapshot_json        JSONB,

    -- Model deployment name used (e.g. 'claude-sonnet-4-6')
    model_version               VARCHAR(100),

    generated_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_pricing_ai_reviews_updated_at ON pricing_ai_reviews;
CREATE TRIGGER trg_pricing_ai_reviews_updated_at
    BEFORE UPDATE ON pricing_ai_reviews
    FOR EACH ROW EXECUTE FUNCTION pricing_set_updated_at();

CREATE INDEX IF NOT EXISTS idx_pricing_ai_reviews_site_type
    ON pricing_ai_reviews (site_id, s_type_name);
CREATE INDEX IF NOT EXISTS idx_pricing_ai_reviews_site_generated
    ON pricing_ai_reviews (site_id, generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_pricing_ai_reviews_rec
    ON pricing_ai_reviews (recommendation_id);

-- =============================================================================
-- 7. pricing_ai_outcomes
--    T+7 and T+30 realized outcome measurements per AI review.
--    Each review gets up to two rows (as_of_days IN {7, 30}).
--    T+30 is authoritative when it conflicts with T+7.
-- =============================================================================
CREATE TABLE IF NOT EXISTS pricing_ai_outcomes (
    id                          BIGSERIAL       PRIMARY KEY,

    review_id                   BIGINT          NOT NULL
                                    REFERENCES pricing_ai_reviews (id) ON DELETE CASCADE,

    -- Which measurement window this row represents
    -- NOTE: SMALLINT used; only valid values are 7 and 30 (enforced by CHECK).
    as_of_days                  SMALLINT        NOT NULL
                                    CHECK (as_of_days IN (7, 30)),

    -- Realized metrics at T+N
    occ_pct_realized            NUMERIC(6,3),   -- occupancy % at measurement date
    net_movein_realized         INTEGER,        -- net move-ins in the window
    revenue_actual              NUMERIC(12,4),  -- actual revenue in the period

    -- Derived label
    outcome_label               VARCHAR(10)     NOT NULL
                                    CHECK (outcome_label IN ('positive', 'neutral', 'negative')),

    measured_at                 TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_pricing_ai_outcomes UNIQUE (review_id, as_of_days)
);

DROP TRIGGER IF EXISTS trg_pricing_ai_outcomes_updated_at ON pricing_ai_outcomes;
CREATE TRIGGER trg_pricing_ai_outcomes_updated_at
    BEFORE UPDATE ON pricing_ai_outcomes
    FOR EACH ROW EXECUTE FUNCTION pricing_set_updated_at();

-- Primary access pattern: fetch outcomes for a review + time window
CREATE INDEX IF NOT EXISTS idx_pricing_ai_outcomes_review
    ON pricing_ai_outcomes (review_id, as_of_days);

-- =============================================================================
-- 8. pricing_decisions
--    Human accept / reject decisions on AI reviews. One row per review_id.
--    Populated by POST /api/pricing/ai-review/{review_id}/decide.
-- =============================================================================
CREATE TABLE IF NOT EXISTS pricing_decisions (
    id                          BIGSERIAL       PRIMARY KEY,

    review_id                   BIGINT          NOT NULL
                                    REFERENCES pricing_ai_reviews (id) ON DELETE CASCADE,

    -- Convenience denormalization
    site_id                     BIGINT          NOT NULL,
    s_type_name                 VARCHAR(100)    NOT NULL,

    decision                    VARCHAR(20)     NOT NULL
                                    CHECK (decision IN ('accept_ai', 'accept_baseline', 'reject_both', 'manual_override')),

    -- The monthly rate that was actually applied (NULL if reject_both)
    applied_monthly             NUMERIC(12,4),

    decided_by                  VARCHAR(255)    NOT NULL,
    decided_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- One decision per review (can be updated if the user changes their mind
    -- before the rate write is executed — the route should UPSERT on review_id)
    CONSTRAINT uq_pricing_decisions_review UNIQUE (review_id)
);

DROP TRIGGER IF EXISTS trg_pricing_decisions_updated_at ON pricing_decisions;
CREATE TRIGGER trg_pricing_decisions_updated_at
    BEFORE UPDATE ON pricing_decisions
    FOR EACH ROW EXECUTE FUNCTION pricing_set_updated_at();

CREATE INDEX IF NOT EXISTS idx_pricing_decisions_site_decided
    ON pricing_decisions (site_id, decided_at DESC);

-- =============================================================================
-- 9. pricing_ai_mute
--    Per-site / per-type opt-out of AI overlay. The nightly AI review job
--    skips (site_id, s_type_name) rows where is_muted = TRUE.
-- =============================================================================
CREATE TABLE IF NOT EXISTS pricing_ai_mute (
    id                          SERIAL          PRIMARY KEY,

    site_id                     BIGINT          NOT NULL,
    s_type_name                 VARCHAR(100)    NOT NULL,

    is_muted                    BOOLEAN         NOT NULL DEFAULT TRUE,
    muted_by                    VARCHAR(255),
    muted_at                    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_pricing_ai_mute UNIQUE (site_id, s_type_name)
);

DROP TRIGGER IF EXISTS trg_pricing_ai_mute_updated_at ON pricing_ai_mute;
CREATE TRIGGER trg_pricing_ai_mute_updated_at
    BEFORE UPDATE ON pricing_ai_mute
    FOR EACH ROW EXECUTE FUNCTION pricing_set_updated_at();

CREATE INDEX IF NOT EXISTS idx_pricing_ai_mute_site
    ON pricing_ai_mute (site_id) WHERE is_muted = TRUE;

COMMIT;
