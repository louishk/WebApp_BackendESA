-- 035: Add revenue tools permission to roles table
-- Target database: esa_backend

ALTER TABLE roles ADD COLUMN IF NOT EXISTS can_access_revenue_tools BOOLEAN DEFAULT false;
UPDATE roles SET can_access_revenue_tools = true WHERE name = 'admin';
