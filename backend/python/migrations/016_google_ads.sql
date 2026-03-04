-- Migration 016: Google Ads tables for PBI reporting
-- Source: BigQuery esa_google_ads dataset (Google Ads Data Transfer)
-- Target: esa_pbi PostgreSQL
--
-- 3 child accounts:
--   4605031997 (SG, SGD), 5469799452 (MY, SGD), 4855318963 (KR, KRW)

-- ============================================================================
-- 1. Account mapping (reference table)
-- ============================================================================
CREATE TABLE IF NOT EXISTS gads_account_map (
    customer_id BIGINT PRIMARY KEY,
    country VARCHAR(2) NOT NULL,
    currency VARCHAR(3) NOT NULL
);

INSERT INTO gads_account_map (customer_id, country, currency) VALUES
    (4605031997, 'SG', 'SGD'),
    (5469799452, 'MY', 'SGD'),
    (4855318963, 'KR', 'KRW')
ON CONFLICT (customer_id) DO UPDATE SET
    country = EXCLUDED.country,
    currency = EXCLUDED.currency;

-- ============================================================================
-- 2. Dimension: Campaigns (daily snapshot, full overwrite)
-- ============================================================================
CREATE TABLE IF NOT EXISTS gads_campaigns (
    campaign_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    campaign_name VARCHAR(255),
    campaign_status VARCHAR(20),
    channel_type VARCHAR(30),
    channel_sub_type VARCHAR(30),
    bidding_strategy_type VARCHAR(50),
    budget_amount_micros BIGINT,
    start_date DATE,
    end_date DATE,
    _data_date DATE,
    synced_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gads_campaigns_customer ON gads_campaigns (customer_id);
CREATE INDEX IF NOT EXISTS idx_gads_campaigns_status ON gads_campaigns (campaign_status);

-- ============================================================================
-- 3. Dimension: Ad Groups (daily snapshot, full overwrite)
-- ============================================================================
CREATE TABLE IF NOT EXISTS gads_ad_groups (
    ad_group_id BIGINT PRIMARY KEY,
    campaign_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    ad_group_name VARCHAR(255),
    ad_group_status VARCHAR(20),
    ad_group_type VARCHAR(30),
    _data_date DATE,
    synced_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gads_ad_groups_campaign ON gads_ad_groups (campaign_id);
CREATE INDEX IF NOT EXISTS idx_gads_ad_groups_customer ON gads_ad_groups (customer_id);

-- ============================================================================
-- 4. Dimension: Keywords (latest snapshot)
-- ============================================================================
CREATE TABLE IF NOT EXISTS gads_keywords (
    criterion_id BIGINT NOT NULL,
    ad_group_id BIGINT NOT NULL,
    campaign_id BIGINT NOT NULL,
    keyword_text VARCHAR(500),
    match_type VARCHAR(20),
    is_negative BOOLEAN DEFAULT FALSE,
    status VARCHAR(20),
    quality_score INTEGER,
    _data_date DATE,
    synced_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (criterion_id, ad_group_id)
);

CREATE INDEX IF NOT EXISTS idx_gads_keywords_campaign ON gads_keywords (campaign_id);

-- ============================================================================
-- 5. Fact: Campaign daily stats
-- ============================================================================
CREATE TABLE IF NOT EXISTS gads_campaign_daily (
    campaign_id BIGINT NOT NULL,
    segments_date DATE NOT NULL,
    device VARCHAR(20) NOT NULL,
    ad_network_type VARCHAR(30) NOT NULL,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost_micros BIGINT DEFAULT 0,
    conversions NUMERIC(12,6) DEFAULT 0,
    conversions_value NUMERIC(14,2) DEFAULT 0,
    interactions INTEGER DEFAULT 0,

    synced_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (campaign_id, segments_date, device, ad_network_type)
);

CREATE INDEX IF NOT EXISTS idx_gads_campaign_daily_date ON gads_campaign_daily (segments_date);
CREATE INDEX IF NOT EXISTS idx_gads_campaign_daily_campaign ON gads_campaign_daily (campaign_id);

-- ============================================================================
-- 6. Fact: Campaign conversions by action
-- ============================================================================
CREATE TABLE IF NOT EXISTS gads_campaign_conversions (
    campaign_id BIGINT NOT NULL,
    segments_date DATE NOT NULL,
    conversion_action_name VARCHAR(255) NOT NULL,
    ad_network_type VARCHAR(30) NOT NULL,
    conversion_action_category VARCHAR(50),
    conversions NUMERIC(12,6) DEFAULT 0,
    conversions_value NUMERIC(14,2) DEFAULT 0,
    value_per_conversion NUMERIC(14,2) DEFAULT 0,
    synced_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (campaign_id, segments_date, conversion_action_name, ad_network_type)
);

CREATE INDEX IF NOT EXISTS idx_gads_conv_date ON gads_campaign_conversions (segments_date);

-- ============================================================================
-- 7. Fact: Ad group daily stats
-- ============================================================================
CREATE TABLE IF NOT EXISTS gads_ad_group_daily (
    ad_group_id BIGINT NOT NULL,
    campaign_id BIGINT NOT NULL,
    segments_date DATE NOT NULL,
    device VARCHAR(20) NOT NULL,
    ad_network_type VARCHAR(30) NOT NULL,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost_micros BIGINT DEFAULT 0,
    conversions NUMERIC(12,6) DEFAULT 0,
    conversions_value NUMERIC(14,2) DEFAULT 0,
    interactions INTEGER DEFAULT 0,

    synced_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ad_group_id, segments_date, device, ad_network_type)
);

CREATE INDEX IF NOT EXISTS idx_gads_ag_daily_date ON gads_ad_group_daily (segments_date);
CREATE INDEX IF NOT EXISTS idx_gads_ag_daily_campaign ON gads_ad_group_daily (campaign_id);

-- ============================================================================
-- 8. Fact: Keyword daily stats
-- ============================================================================
CREATE TABLE IF NOT EXISTS gads_keyword_daily (
    criterion_id BIGINT NOT NULL,
    ad_group_id BIGINT NOT NULL,
    campaign_id BIGINT NOT NULL,
    segments_date DATE NOT NULL,
    device VARCHAR(20) NOT NULL,
    ad_network_type VARCHAR(30) NOT NULL,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost_micros BIGINT DEFAULT 0,
    conversions NUMERIC(12,6) DEFAULT 0,
    conversions_value NUMERIC(14,2) DEFAULT 0,
    interactions INTEGER DEFAULT 0,

    synced_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (criterion_id, ad_group_id, segments_date, device, ad_network_type)
);

CREATE INDEX IF NOT EXISTS idx_gads_kw_daily_date ON gads_keyword_daily (segments_date);
CREATE INDEX IF NOT EXISTS idx_gads_kw_daily_campaign ON gads_keyword_daily (campaign_id);
CREATE INDEX IF NOT EXISTS idx_gads_kw_daily_ad_group ON gads_keyword_daily (ad_group_id);

-- ============================================================================
-- 9. PBI View: pre-joined overview with SGD conversion
-- ============================================================================
CREATE OR REPLACE VIEW vw_gads_overview AS
SELECT
    d.segments_date,
    c.campaign_id,
    c.campaign_name,
    c.campaign_status,
    c.channel_type,
    am.country,
    am.currency,
    d.device,
    d.ad_network_type,
    d.impressions,
    d.clicks,
    d.cost_micros / 1e6 AS cost_local,
    d.conversions,
    d.conversions_value,
    d.interactions,
    -- Cost in SGD for cross-country comparison
    CASE am.currency
        WHEN 'SGD' THEN d.cost_micros / 1e6
        WHEN 'KRW' THEN (d.cost_micros / 1e6) / COALESCE(fx.avg_rate, 1)
        ELSE d.cost_micros / 1e6
    END AS cost_sgd
FROM gads_campaign_daily d
JOIN gads_campaigns c ON d.campaign_id = c.campaign_id
JOIN gads_account_map am ON c.customer_id = am.customer_id
LEFT JOIN fx_rates_monthly fx
    ON fx.target_currency = am.currency
    AND fx.year = EXTRACT(YEAR FROM d.segments_date)::integer
    AND fx.month = EXTRACT(MONTH FROM d.segments_date)::integer
    AND am.currency != 'SGD';
