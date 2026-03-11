-- Migration 021: Gravity Forms tables for PBI reporting
-- Source: Kinsta WordPress MySQL (extraspaceasiasg) via SSH tunnel
-- Target: esa_pbi PostgreSQL
--
-- Archival extraction of all Gravity Forms submissions from ESA Singapore
-- WordPress site before Kinsta decommission. Multi-instance design supports
-- future extraction of MY/KR instances into same schema.

-- ============================================================================
-- 1. Dimension: Facilities (WP post_type = 'facility')
-- ============================================================================
CREATE TABLE IF NOT EXISTS gf_dim_facility (
    post_id INTEGER NOT NULL,
    facility_name VARCHAR(255) NOT NULL,
    source_instance VARCHAR(50) NOT NULL DEFAULT 'kinsta_sg',
    PRIMARY KEY (source_instance, post_id)
);

-- ============================================================================
-- 2. Dimension: Storage Sizes (WP post_type = 'storage-size-aqr' + 'storage-size')
-- ============================================================================
CREATE TABLE IF NOT EXISTS gf_dim_storage_size (
    size_id INTEGER NOT NULL,
    size_label VARCHAR(255) NOT NULL,
    size_type VARCHAR(20) NOT NULL DEFAULT 'aqr',  -- 'aqr' or 'legacy'
    source_instance VARCHAR(50) NOT NULL DEFAULT 'kinsta_sg',
    PRIMARY KEY (source_instance, size_id)
);

-- ============================================================================
-- 3. Dimension: Storage Durations (WP taxonomy = 'storage-duration')
-- ============================================================================
CREATE TABLE IF NOT EXISTS gf_dim_storage_duration (
    duration_id INTEGER NOT NULL,
    duration_label VARCHAR(100) NOT NULL,
    source_instance VARCHAR(50) NOT NULL DEFAULT 'kinsta_sg',
    PRIMARY KEY (source_instance, duration_id)
);

-- ============================================================================
-- 4. Fact: Entries (one row per form submission, wide flat table)
-- ============================================================================
CREATE TABLE IF NOT EXISTS gf_entries (
    entry_id INTEGER NOT NULL,
    form_id INTEGER NOT NULL,
    form_name TEXT,
    status TEXT,
    date_created_utc TIMESTAMP,
    date_created_sgt TIMESTAMP,
    date_updated TIMESTAMP,
    source_url TEXT,
    ip TEXT,
    user_agent TEXT,

    -- Contact fields
    salutation TEXT,
    name TEXT,
    email TEXT,
    phone TEXT,
    company TEXT,

    -- Quote fields (forms 6, 7, 8, 9)
    facility_id INTEGER,
    facility_name TEXT,
    storage_size_id INTEGER,
    storage_size_label TEXT,
    storage_duration_id INTEGER,
    storage_duration_label TEXT,
    source_channel TEXT,
    promo_code TEXT,
    unit_type TEXT,

    -- UTM fields (form 8 only, ~10% populated)
    utm_source TEXT,
    utm_campaign TEXT,
    utm_medium TEXT,
    utm_additional TEXT,

    -- Payment fields (form 3)
    invoice_number TEXT,
    payment_amount DOUBLE PRECISION,
    payment_due_date DATE,
    registered_name TEXT,
    payment_status TEXT,
    payment_date TIMESTAMP,
    payment_method TEXT,
    transaction_id TEXT,

    -- Contact/Inquiry fields (forms 2, 10)
    message TEXT,
    preferred_contact TEXT,
    existing_customer TEXT,
    package_selection TEXT,

    -- Moving fields (form 12)
    service_type TEXT,
    moving_from_postal TEXT,
    moving_to_postal TEXT,
    move_date TEXT,

    -- Express/Bizplus fields (forms 14, 15, 16)
    storage_purpose TEXT,
    seats_required TEXT,
    coworking_solution TEXT,
    tour_date TEXT,

    -- Overflow
    extra_fields JSONB,

    -- ETL metadata
    source_instance VARCHAR(50) NOT NULL DEFAULT 'kinsta_sg',
    synced_at TIMESTAMP NOT NULL DEFAULT NOW(),

    UNIQUE (source_instance, entry_id)
);

CREATE INDEX IF NOT EXISTS idx_gf_entries_form ON gf_entries (form_id);
CREATE INDEX IF NOT EXISTS idx_gf_entries_email ON gf_entries (email);
CREATE INDEX IF NOT EXISTS idx_gf_entries_date_sgt ON gf_entries (date_created_sgt);
CREATE INDEX IF NOT EXISTS idx_gf_entries_status ON gf_entries (status);
CREATE INDEX IF NOT EXISTS idx_gf_entries_instance ON gf_entries (source_instance);
CREATE INDEX IF NOT EXISTS idx_gf_entries_facility ON gf_entries (facility_id);

-- ============================================================================
-- 5. Raw EAV backup (insurance — drop after count verification)
-- ============================================================================
CREATE TABLE IF NOT EXISTS gf_entry_meta_raw (
    entry_id INTEGER NOT NULL,
    form_id INTEGER NOT NULL,
    meta_key VARCHAR(255) NOT NULL,
    meta_value TEXT,
    source_instance VARCHAR(50) NOT NULL DEFAULT 'kinsta_sg',
    synced_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gf_meta_raw_entry ON gf_entry_meta_raw (entry_id);
CREATE INDEX IF NOT EXISTS idx_gf_meta_raw_form ON gf_entry_meta_raw (form_id);
CREATE INDEX IF NOT EXISTS idx_gf_meta_raw_instance ON gf_entry_meta_raw (source_instance);
