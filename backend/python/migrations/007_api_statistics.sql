-- Migration 007: API Statistics table for endpoint consumption monitoring
-- Date: 2026-02-13

CREATE TABLE IF NOT EXISTS api_statistics (
    id SERIAL PRIMARY KEY,
    endpoint VARCHAR(255) NOT NULL,
    method VARCHAR(10) NOT NULL,
    status_code INTEGER NOT NULL,
    response_time_ms FLOAT NOT NULL,
    client_ip VARCHAR(45),
    user_agent VARCHAR(255),
    request_size INTEGER,
    response_size INTEGER,
    called_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_api_stats_called_at ON api_statistics (called_at);
CREATE INDEX IF NOT EXISTS ix_api_stats_endpoint ON api_statistics (endpoint);
CREATE INDEX IF NOT EXISTS ix_api_stats_endpoint_called ON api_statistics (endpoint, called_at);
