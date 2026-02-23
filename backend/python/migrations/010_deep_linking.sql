-- Migration 010: Deep linking / Smart app redirects for URL shortener
-- Date: 2026-02-23
-- Adds app link rules for mapping URL domains to native app URI schemes,
-- and deep_link_enabled flag on short_links for per-link control.

-- App link rules: maps domain patterns to native app schemes
CREATE TABLE IF NOT EXISTS app_link_rules (
    id SERIAL PRIMARY KEY,
    domain_pattern VARCHAR(255) NOT NULL,              -- e.g. "youtube.com", "*.instagram.com"
    name VARCHAR(100) NOT NULL,                        -- human-readable name, e.g. "YouTube"
    ios_scheme VARCHAR(500),                           -- e.g. "youtube://", "vnd.youtube://"
    ios_app_store_url VARCHAR(500),                    -- fallback App Store link
    android_scheme VARCHAR(500),                       -- e.g. "intent://...#Intent;scheme=https;package=com.google.android.youtube;end"
    android_play_store_url VARCHAR(500),               -- fallback Play Store link
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 0,               -- higher = matched first when multiple rules match
    created_by VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_app_link_rules_domain ON app_link_rules (domain_pattern);
CREATE INDEX IF NOT EXISTS ix_app_link_rules_active ON app_link_rules (is_active);
CREATE UNIQUE INDEX IF NOT EXISTS ix_app_link_rules_domain_unique ON app_link_rules (domain_pattern);

-- Add deep linking toggle to short_links
ALTER TABLE short_links ADD COLUMN IF NOT EXISTS deep_link_enabled BOOLEAN NOT NULL DEFAULT FALSE;

-- Seed popular app link rules
INSERT INTO app_link_rules (domain_pattern, name, ios_scheme, ios_app_store_url, android_scheme, android_play_store_url, priority)
VALUES
    ('youtube.com', 'YouTube', 'youtube://', 'https://apps.apple.com/app/youtube/id544007664', 'intent://www.youtube.com#Intent;scheme=https;package=com.google.android.youtube;end', 'https://play.google.com/store/apps/details?id=com.google.android.youtube', 100),
    ('youtu.be', 'YouTube (Short)', 'youtube://', 'https://apps.apple.com/app/youtube/id544007664', 'intent://youtu.be#Intent;scheme=https;package=com.google.android.youtube;end', 'https://play.google.com/store/apps/details?id=com.google.android.youtube', 99),
    ('instagram.com', 'Instagram', 'instagram://', 'https://apps.apple.com/app/instagram/id389801252', 'intent://www.instagram.com#Intent;scheme=https;package=com.instagram.android;end', 'https://play.google.com/store/apps/details?id=com.instagram.android', 100),
    ('twitter.com', 'X (Twitter)', 'twitter://', 'https://apps.apple.com/app/x/id333903271', 'intent://twitter.com#Intent;scheme=https;package=com.twitter.android;end', 'https://play.google.com/store/apps/details?id=com.twitter.android', 100),
    ('x.com', 'X', 'twitter://', 'https://apps.apple.com/app/x/id333903271', 'intent://x.com#Intent;scheme=https;package=com.twitter.android;end', 'https://play.google.com/store/apps/details?id=com.twitter.android', 99),
    ('facebook.com', 'Facebook', 'fb://', 'https://apps.apple.com/app/facebook/id284882215', 'intent://www.facebook.com#Intent;scheme=https;package=com.facebook.katana;end', 'https://play.google.com/store/apps/details?id=com.facebook.katana', 100),
    ('linkedin.com', 'LinkedIn', 'linkedin://', 'https://apps.apple.com/app/linkedin/id288429040', 'intent://www.linkedin.com#Intent;scheme=https;package=com.linkedin.android;end', 'https://play.google.com/store/apps/details?id=com.linkedin.android', 100),
    ('tiktok.com', 'TikTok', 'snssdk1233://', 'https://apps.apple.com/app/tiktok/id835599320', 'intent://www.tiktok.com#Intent;scheme=https;package=com.zhiliaoapp.musically;end', 'https://play.google.com/store/apps/details?id=com.zhiliaoapp.musically', 100),
    ('spotify.com', 'Spotify', 'spotify://', 'https://apps.apple.com/app/spotify/id324684580', 'intent://open.spotify.com#Intent;scheme=https;package=com.spotify.music;end', 'https://play.google.com/store/apps/details?id=com.spotify.music', 100),
    ('open.spotify.com', 'Spotify (Open)', 'spotify://', 'https://apps.apple.com/app/spotify/id324684580', 'intent://open.spotify.com#Intent;scheme=https;package=com.spotify.music;end', 'https://play.google.com/store/apps/details?id=com.spotify.music', 101),
    ('wa.me', 'WhatsApp', 'whatsapp://', 'https://apps.apple.com/app/whatsapp/id310633997', 'intent://wa.me#Intent;scheme=https;package=com.whatsapp;end', 'https://play.google.com/store/apps/details?id=com.whatsapp', 100),
    ('maps.google.com', 'Google Maps', 'comgooglemaps://', 'https://apps.apple.com/app/google-maps/id585027354', 'intent://maps.google.com#Intent;scheme=https;package=com.google.android.apps.maps;end', 'https://play.google.com/store/apps/details?id=com.google.android.apps.maps', 100),
    ('zoom.us', 'Zoom', 'zoomus://', 'https://apps.apple.com/app/zoom/id546505307', 'intent://zoom.us#Intent;scheme=https;package=us.zoom.videomeetings;end', 'https://play.google.com/store/apps/details?id=us.zoom.videomeetings', 100),
    ('pinterest.com', 'Pinterest', 'pinterest://', 'https://apps.apple.com/app/pinterest/id429047995', 'intent://www.pinterest.com#Intent;scheme=https;package=com.pinterest;end', 'https://play.google.com/store/apps/details?id=com.pinterest', 100),
    ('reddit.com', 'Reddit', 'reddit://', 'https://apps.apple.com/app/reddit/id1064216828', 'intent://www.reddit.com#Intent;scheme=https;package=com.reddit.frontpage;end', 'https://play.google.com/store/apps/details?id=com.reddit.frontpage', 100)
ON CONFLICT (domain_pattern) DO NOTHING;
