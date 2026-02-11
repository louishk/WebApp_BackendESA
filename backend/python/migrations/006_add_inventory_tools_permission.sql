-- Migration 006: Add can_access_inventory_tools permission to roles
ALTER TABLE roles ADD COLUMN IF NOT EXISTS can_access_inventory_tools BOOLEAN DEFAULT FALSE;

-- Grant inventory tools access to admin role
UPDATE roles SET can_access_inventory_tools = TRUE WHERE name = 'admin';
