-- Migration 028: Add mcp_db_presets column to api_keys
-- Restricts which DB presets each API key can connect to via MCP
-- Empty array = all presets allowed (default)

ALTER TABLE api_keys
ADD COLUMN IF NOT EXISTS mcp_db_presets JSONB NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN api_keys.mcp_db_presets IS 'Allowed DB preset names (empty list = all presets)';
