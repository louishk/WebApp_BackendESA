-- Migration 008: External API statistics table for tracking outbound API calls
-- Date: 2026-02-13

CREATE TABLE IF NOT EXISTS external_api_statistics (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(50) NOT NULL,
    endpoint VARCHAR(500) NOT NULL,
    method VARCHAR(10) NOT NULL,
    status_code INTEGER,
    response_time_ms FLOAT NOT NULL,
    request_size INTEGER,
    response_size INTEGER,
    success BOOLEAN NOT NULL DEFAULT TRUE,
    error_message VARCHAR(500),
    caller VARCHAR(100),
    called_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_ext_api_called_at ON external_api_statistics (called_at);
CREATE INDEX IF NOT EXISTS ix_ext_api_service ON external_api_statistics (service_name);
CREATE INDEX IF NOT EXISTS ix_ext_api_service_called ON external_api_statistics (service_name, called_at);
