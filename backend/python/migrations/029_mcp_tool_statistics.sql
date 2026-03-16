-- Migration 029: MCP tool usage statistics table
-- Tracks individual MCP tool calls for consumption monitoring

CREATE TABLE IF NOT EXISTS mcp_tool_statistics (
    id SERIAL PRIMARY KEY,
    tool_name VARCHAR(100) NOT NULL,
    username VARCHAR(255),
    key_id VARCHAR(16),
    client_ip VARCHAR(45),
    response_time_ms FLOAT NOT NULL DEFAULT 0,
    is_error BOOLEAN NOT NULL DEFAULT false,
    called_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_mcp_stats_called_at ON mcp_tool_statistics (called_at);
CREATE INDEX IF NOT EXISTS ix_mcp_stats_tool ON mcp_tool_statistics (tool_name);
CREATE INDEX IF NOT EXISTS ix_mcp_stats_tool_called ON mcp_tool_statistics (tool_name, called_at);
CREATE INDEX IF NOT EXISTS ix_mcp_stats_key_id ON mcp_tool_statistics (key_id);

COMMENT ON TABLE mcp_tool_statistics IS 'MCP tool call tracking for usage monitoring';
