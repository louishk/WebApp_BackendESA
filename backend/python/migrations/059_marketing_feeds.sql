-- Migration 059: Marketing feed configurations (esa_middleware DB)
--
-- One row per published feed (Facebook Catalog / Google Ads remarketing).
-- Selection criteria + render options are stored as JSONB for flexibility.

CREATE TABLE IF NOT EXISTS mw_marketing_feeds (
    id                 SERIAL PRIMARY KEY,
    name               VARCHAR(120) NOT NULL,
    slug               VARCHAR(80)  NOT NULL UNIQUE,
    channel            VARCHAR(20)  NOT NULL,   -- 'facebook' | 'google_ads'
    enabled            BOOLEAN      NOT NULL DEFAULT true,

    -- Selection
    site_ids           INTEGER[]    NOT NULL DEFAULT '{}',  -- empty = all sites
    countries          TEXT[]       NOT NULL DEFAULT '{}',  -- empty = all countries (filter on mw_siteinfo.Country)
    category_includes  TEXT[]       NOT NULL DEFAULT '{}',  -- empty = all categories (stype_name ILIKE ANY)
    category_excludes  TEXT[]       NOT NULL DEFAULT '{}',
    unit_type_excludes TEXT[]       NOT NULL DEFAULT '{}',  -- e.g. MB, BZ, PR

    -- Pricing
    list_price_source     VARCHAR(20) NOT NULL DEFAULT 'std_rate',     -- column on mw_unit_discount_candidates
    sale_price_source     VARCHAR(20) NOT NULL DEFAULT 'preferred_rate',
    include_sale_price    BOOLEAN     NOT NULL DEFAULT true,
    currency_override     VARCHAR(3),  -- NULL = derive from country

    -- Render
    title_template       TEXT NOT NULL DEFAULT '{size_range} {climate_label} Storage at {site_name}',
    description_template TEXT NOT NULL DEFAULT 'Self-storage unit ({size_range}) at Extra Space Asia {site_name}. {climate_label}.',
    brand                VARCHAR(80) NOT NULL DEFAULT 'Extra Space Asia',
    landing_url_template TEXT NOT NULL DEFAULT 'https://www.extraspaceasia.com/?site={site_code}',
    image_url_template   TEXT,        -- optional, can be null

    -- Public access
    public_token VARCHAR(64) NOT NULL UNIQUE,

    -- Bookkeeping
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by   VARCHAR(120),
    last_built_at TIMESTAMP,
    last_row_count INTEGER
);

CREATE INDEX IF NOT EXISTS ix_mw_marketing_feeds_enabled ON mw_marketing_feeds (enabled);
CREATE INDEX IF NOT EXISTS ix_mw_marketing_feeds_channel ON mw_marketing_feeds (channel);
