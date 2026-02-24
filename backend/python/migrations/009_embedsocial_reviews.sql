-- Migration 009: EmbedSocial reviews table for tracking Google review scores over time
-- Date: 2026-02-23

CREATE TABLE IF NOT EXISTS embedsocial_reviews (
    id SERIAL PRIMARY KEY,
    review_id VARCHAR(50) NOT NULL UNIQUE,
    source_id VARCHAR(50) NOT NULL,
    source_name VARCHAR(255),
    source_address VARCHAR(500),
    author_name VARCHAR(255),
    rating INTEGER NOT NULL,
    caption_text TEXT,
    review_link VARCHAR(500),
    original_created_on TIMESTAMP NOT NULL,
    reply_text TEXT,
    reply_created_on TIMESTAMP,
    synced_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_es_reviews_source_id ON embedsocial_reviews (source_id);
CREATE INDEX IF NOT EXISTS ix_es_reviews_created_on ON embedsocial_reviews (original_created_on);
CREATE INDEX IF NOT EXISTS ix_es_reviews_source_date ON embedsocial_reviews (source_id, original_created_on);
CREATE INDEX IF NOT EXISTS ix_es_reviews_rating ON embedsocial_reviews (rating);
