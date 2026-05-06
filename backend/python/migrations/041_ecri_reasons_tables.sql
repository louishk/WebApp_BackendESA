-- Migration 041: Create ecri_exclusion_reasons + ecri_objection_reasons tables
-- Target DB: esa_backend (db: backend)

CREATE TABLE IF NOT EXISTS ecri_exclusion_reasons (
    id         SERIAL PRIMARY KEY,
    code       VARCHAR(40)  NOT NULL UNIQUE,
    label      VARCHAR(200) NOT NULL,
    active     BOOLEAN      NOT NULL DEFAULT TRUE,
    sort_order INTEGER      NOT NULL DEFAULT 100,
    created_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ecri_objection_reasons (
    id         SERIAL PRIMARY KEY,
    code       VARCHAR(40)  NOT NULL UNIQUE,
    label      VARCHAR(200) NOT NULL,
    active     BOOLEAN      NOT NULL DEFAULT TRUE,
    sort_order INTEGER      NOT NULL DEFAULT 100,
    created_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Seed exclusion reasons
INSERT INTO ecri_exclusion_reasons (code, label, sort_order) VALUES
    ('scheduled_moveout',   'Tenant scheduled to move out',              10),
    ('recent_signup',       'Recent sign-up / recent rate change',       20),
    ('hardship',            'Financial hardship',                        30),
    ('vip_longterm',        'VIP / long-term retention',                 40),
    ('dispute',             'Active dispute / complaint',                50),
    ('unit_issue',          'Unit quality / access issue',               60),
    ('commercial_contract', 'Commercial contract / special agreement',   70),
    ('other',               'Other (specify in notes)',                  80)
ON CONFLICT (code) DO NOTHING;

-- Seed objection reasons
INSERT INTO ecri_objection_reasons (code, label, sort_order) VALUES
    ('tenant_hardship',      'Tenant hardship / financial difficulty',   10),
    ('competitor_match',     'Matched competitor rate',                  20),
    ('long_term_retention',  'Retention — long-term tenant',             30),
    ('unit_issue',           'Unit quality / access issue',              40),
    ('billing_error',        'Billing or calculation error',             50),
    ('negotiated_discount',  'Negotiated discount',                      60),
    ('other',                'Other (specify in notes)',                 70)
ON CONFLICT (code) DO NOTHING;
