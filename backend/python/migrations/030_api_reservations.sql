-- 030: Reservation tracking table for distribution/attribution analysis
-- Database: esa_pbi
-- Run: PGPASSWORD=<pw> psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d esa_pbi -f backend/python/migrations/030_api_reservations.sql

CREATE TABLE IF NOT EXISTS api_reservations (
    id                  SERIAL PRIMARY KEY,

    -- SiteLink response data
    waiting_id          INTEGER,
    global_waiting_num  BIGINT,
    tenant_id           INTEGER,

    -- Reservation details
    site_code           VARCHAR(10) NOT NULL,
    unit_id             INTEGER NOT NULL,
    first_name          VARCHAR(100) NOT NULL DEFAULT '',
    last_name           VARCHAR(100) NOT NULL DEFAULT '',
    email               VARCHAR(100),
    phone               VARCHAR(20),
    mobile              VARCHAR(20),
    quoted_rate         NUMERIC(10,2) DEFAULT 0,
    concession_id       INTEGER DEFAULT 0,
    needed_date         DATE,
    expires_date        DATE,
    source_name         VARCHAR(64) DEFAULT 'ESA Backend',
    comment             VARCHAR(500),

    -- Source / distribution tracking
    source              VARCHAR(50) NOT NULL DEFAULT 'api',
    gclid               VARCHAR(255),
    gid                 VARCHAR(255),
    botid               VARCHAR(255),

    -- API caller identity
    api_key_id          VARCHAR(20),
    api_user            VARCHAR(100),

    -- Status
    status              VARCHAR(20) NOT NULL DEFAULT 'created',

    -- Timestamps
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Query indexes
CREATE INDEX IF NOT EXISTS idx_api_res_site ON api_reservations (site_code);
CREATE INDEX IF NOT EXISTS idx_api_res_waiting ON api_reservations (waiting_id);
CREATE INDEX IF NOT EXISTS idx_api_res_tenant ON api_reservations (tenant_id);
CREATE INDEX IF NOT EXISTS idx_api_res_source ON api_reservations (source);
CREATE INDEX IF NOT EXISTS idx_api_res_created ON api_reservations (created_at);

-- Partial indexes for tracking fields (most rows NULL)
CREATE INDEX IF NOT EXISTS idx_api_res_gclid ON api_reservations (gclid) WHERE gclid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_api_res_gid ON api_reservations (gid) WHERE gid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_api_res_botid ON api_reservations (botid) WHERE botid IS NOT NULL;
