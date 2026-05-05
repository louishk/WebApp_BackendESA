-- sql/2026-05-05-unit-category-risk.sql
-- Tables for unit-category risk scoring (esa_pbi)

CREATE TABLE IF NOT EXISTS unit_category_risk_baseline (
    country_code        VARCHAR(2)    PRIMARY KEY,
    window_start        DATE          NOT NULL,
    window_end          DATE          NOT NULL,
    moveout_count       INTEGER       NOT NULL DEFAULT 0,
    unit_months_occupied NUMERIC(14,2) NOT NULL DEFAULT 0,
    baseline_rate       NUMERIC(8,6)  NOT NULL DEFAULT 0,
    computed_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS unit_category_risk_factor (
    id                   SERIAL        PRIMARY KEY,
    country_code         VARCHAR(2)    NOT NULL,
    dimension            VARCHAR(16)   NOT NULL,
    value                VARCHAR(16)   NOT NULL,
    sample_size          INTEGER       NOT NULL DEFAULT 0,
    unit_months_occupied NUMERIC(14,2) NOT NULL DEFAULT 0,
    empirical_factor     NUMERIC(8,6),
    override_factor      NUMERIC(8,6),
    effective_factor     NUMERIC(8,6)  NOT NULL DEFAULT 1.0,
    is_thin_data         BOOLEAN       NOT NULL DEFAULT TRUE,
    override_reason      TEXT,
    override_by          VARCHAR(64),
    override_at          TIMESTAMPTZ,
    computed_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_risk_factor UNIQUE (country_code, dimension, value),
    CONSTRAINT ck_risk_factor_dim CHECK (dimension IN
        ('size','range','type','climate','shape','pillar')),
    CONSTRAINT ck_risk_factor_override_range CHECK
        (override_factor IS NULL OR (override_factor >= 0.1 AND override_factor <= 5.0))
);
CREATE INDEX IF NOT EXISTS idx_risk_factor_country
    ON unit_category_risk_factor(country_code);

CREATE TABLE IF NOT EXISTS unit_category_risk_history (
    id               SERIAL        PRIMARY KEY,
    snapshot_month   DATE          NOT NULL,
    country_code     VARCHAR(2)    NOT NULL,
    dimension        VARCHAR(16)   NOT NULL,
    value            VARCHAR(16)   NOT NULL,
    empirical_factor NUMERIC(8,6),
    sample_size      INTEGER       NOT NULL DEFAULT 0,
    baseline_rate    NUMERIC(8,6)  NOT NULL DEFAULT 0,
    CONSTRAINT uq_risk_history UNIQUE
        (country_code, dimension, value, snapshot_month)
);
CREATE INDEX IF NOT EXISTS idx_risk_history_lookup
    ON unit_category_risk_history(country_code, dimension, value);
