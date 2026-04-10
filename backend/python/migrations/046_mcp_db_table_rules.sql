-- 046: Add table-level access control for MCP database presets
-- mcp_db_table_rules: {"preset_name": ["table1", "table2"], ...}
-- Empty object ({}) = no table restrictions (all tables allowed)
-- Preset key with list = only those tables are accessible

ALTER TABLE api_keys
ADD COLUMN IF NOT EXISTS mcp_db_table_rules JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN api_keys.mcp_db_table_rules IS 'Per-preset table allow-lists for MCP DB tools. Empty = no restrictions.';
