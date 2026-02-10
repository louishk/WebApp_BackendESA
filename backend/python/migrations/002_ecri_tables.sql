-- Migration 002: ECRI (Existing Customer Rate Increase) Tables
-- Target database: esa_pbi (PostgreSQL on Azure)
-- Creates: ecri_batches, ecri_batch_ledgers, ecri_outcomes

BEGIN;

-- ============================================================================
-- Table 1: ecri_batches - Batch metadata for each ECRI run
-- ============================================================================
CREATE TABLE IF NOT EXISTS ecri_batches (
    batch_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255),
    site_ids        INTEGER[] NOT NULL,
    target_increase_pct NUMERIC(5, 2),
    control_group_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    group_config    JSONB,
    total_ledgers   INTEGER NOT NULL DEFAULT 0,
    status          VARCHAR(20) NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft', 'review', 'executed', 'cancelled')),
    created_by      VARCHAR(255),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    executed_at     TIMESTAMP,
    cancelled_at    TIMESTAMP,

    -- Configuration snapshot at batch creation time
    min_tenure_months    INTEGER NOT NULL DEFAULT 12,
    notice_period_days   INTEGER NOT NULL DEFAULT 14,
    discount_reference_pct NUMERIC(5, 2) NOT NULL DEFAULT 40.00,
    attribution_window_days INTEGER NOT NULL DEFAULT 90,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_ecri_batches_status ON ecri_batches(status);
CREATE INDEX IF NOT EXISTS idx_ecri_batches_created ON ecri_batches(created_at DESC);

-- ============================================================================
-- Table 2: ecri_batch_ledgers - Per-ledger details within a batch
-- ============================================================================
CREATE TABLE IF NOT EXISTS ecri_batch_ledgers (
    id              BIGSERIAL PRIMARY KEY,
    batch_id        UUID NOT NULL REFERENCES ecri_batches(batch_id) ON DELETE CASCADE,
    site_id         INTEGER NOT NULL,
    ledger_id       INTEGER NOT NULL,
    tenant_id       INTEGER,
    unit_id         INTEGER,
    unit_name       VARCHAR(100),
    tenant_name     VARCHAR(255),

    -- Control group assignment
    control_group   INTEGER NOT NULL DEFAULT 0,

    -- Rent details
    old_rent        NUMERIC(14, 4) NOT NULL,
    new_rent        NUMERIC(14, 4) NOT NULL,
    increase_pct    NUMERIC(5, 2) NOT NULL,
    increase_amt    NUMERIC(14, 4) NOT NULL,

    -- Dates
    notice_date     DATE,
    effective_date  DATE,

    -- Benchmarking data
    in_place_median_site    NUMERIC(14, 4),
    in_place_median_country NUMERIC(14, 4),
    market_rate             NUMERIC(14, 4),
    std_rate                NUMERIC(14, 4),
    variance_vs_site        NUMERIC(5, 2),
    variance_vs_market      NUMERIC(5, 2),

    -- Tenure info
    moved_in_date   DATE,
    last_increase_date DATE,
    tenure_months   INTEGER,

    -- API execution status
    api_status      VARCHAR(20) NOT NULL DEFAULT 'pending'
                    CHECK (api_status IN ('pending', 'success', 'failed', 'skipped')),
    api_response    JSONB,
    api_executed_at TIMESTAMP,

    UNIQUE(batch_id, site_id, ledger_id)
);

CREATE INDEX IF NOT EXISTS idx_ecri_bl_batch ON ecri_batch_ledgers(batch_id);
CREATE INDEX IF NOT EXISTS idx_ecri_bl_site_ledger ON ecri_batch_ledgers(site_id, ledger_id);
CREATE INDEX IF NOT EXISTS idx_ecri_bl_api_status ON ecri_batch_ledgers(api_status);
CREATE INDEX IF NOT EXISTS idx_ecri_bl_control_group ON ecri_batch_ledgers(batch_id, control_group);

-- ============================================================================
-- Table 3: ecri_outcomes - Churn/stay tracking post-ECRI
-- ============================================================================
CREATE TABLE IF NOT EXISTS ecri_outcomes (
    id              BIGSERIAL PRIMARY KEY,
    batch_id        UUID NOT NULL REFERENCES ecri_batches(batch_id) ON DELETE CASCADE,
    site_id         INTEGER NOT NULL,
    ledger_id       INTEGER NOT NULL,
    outcome_date    DATE NOT NULL,
    outcome_type    VARCHAR(20) NOT NULL
                    CHECK (outcome_type IN ('stayed', 'moved_out', 'scheduled_out')),
    days_after_notice   INTEGER,
    months_at_new_rent  INTEGER,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),

    UNIQUE(batch_id, site_id, ledger_id, outcome_type)
);

CREATE INDEX IF NOT EXISTS idx_ecri_outcomes_batch ON ecri_outcomes(batch_id);
CREATE INDEX IF NOT EXISTS idx_ecri_outcomes_ledger ON ecri_outcomes(site_id, ledger_id);
CREATE INDEX IF NOT EXISTS idx_ecri_outcomes_type ON ecri_outcomes(outcome_type);

COMMIT;
