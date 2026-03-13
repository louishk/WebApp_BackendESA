-- Migration 026: Add MCP access control columns to api_keys
-- Run against esa_backend database

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS mcp_enabled BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS mcp_tools JSONB NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN api_keys.mcp_enabled IS 'Whether this key can access the MCP server';
COMMENT ON COLUMN api_keys.mcp_tools IS 'Allowed MCP tool names (empty array = all tools)';
