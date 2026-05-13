CREATE TABLE IF NOT EXISTS smart_lock_refresh_cooldowns (
    site_id          INTEGER PRIMARY KEY,
    last_refresh_at  TIMESTAMP NOT NULL,
    last_refresh_by  INTEGER REFERENCES users(id),
    last_chain_id    VARCHAR(64),
    updated_at       TIMESTAMP NOT NULL DEFAULT NOW()
);
