-- Migration 011: Discount Plans Management
-- Target database: backend (PostgreSQL)
-- Replaces Excel-based discount plan tracking with a proper backend table.
-- Supports both basic rate plan fields and promotion brief fields.

BEGIN;

-- ============================================================================
-- Table: discount_plans
-- ============================================================================
CREATE TABLE IF NOT EXISTS discount_plans (
    id                  SERIAL PRIMARY KEY,

    -- Plan identification
    plan_type           VARCHAR(50)  NOT NULL,
    plan_name           VARCHAR(255) NOT NULL UNIQUE,
    sitelink_discount_name VARCHAR(255),

    -- Description
    notes               TEXT,
    objective           TEXT,

    -- Availability / Scheduling
    period_range        VARCHAR(255),
    period_start        DATE,
    period_end          DATE,
    move_in_range       VARCHAR(255),
    applicable_sites    JSONB,

    -- Discount details
    discount_value      VARCHAR(255),
    discount_type       VARCHAR(50),
    discount_numeric    NUMERIC(10, 2),
    discount_segmentation VARCHAR(100),
    clawback_condition  TEXT,
    offers              JSONB,

    -- Terms & Conditions
    deposit             VARCHAR(255),
    payment_terms       VARCHAR(100),
    termination_notice  VARCHAR(100),
    extra_offer         VARCHAR(255),
    terms_conditions    JSONB,
    terms_conditions_cn JSONB,

    -- Promotion brief: eligibility & channel
    hidden_rate         BOOLEAN DEFAULT FALSE,
    available_for_chatbot BOOLEAN DEFAULT FALSE,
    chatbot_notes       VARCHAR(255),
    sales_extra_discount VARCHAR(50) DEFAULT 'Not Eligible',
    switch_to_us        VARCHAR(50) DEFAULT 'Not Eligible',
    referral_program    VARCHAR(50) DEFAULT 'Not Eligible',
    distribution_channel VARCHAR(255),

    -- Departmental info
    rate_rules          TEXT,
    rate_rules_sites    VARCHAR(500),
    promotion_codes     JSONB,
    collateral_url      TEXT,
    registration_flow   TEXT,
    department_notes    JSONB,

    -- Extensible custom fields (arbitrary key-value pairs from UI)
    custom_fields       JSONB DEFAULT '{}',

    -- Status
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order          INTEGER DEFAULT 0,

    -- Audit
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by          VARCHAR(255),
    updated_by          VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_discount_plans_type ON discount_plans(plan_type);
CREATE INDEX IF NOT EXISTS idx_discount_plans_active ON discount_plans(is_active);
CREATE INDEX IF NOT EXISTS idx_discount_plans_sort ON discount_plans(sort_order);

COMMIT;
