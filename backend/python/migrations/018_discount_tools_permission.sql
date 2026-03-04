-- Add separate permission for Discount Plan Changer tool
ALTER TABLE roles ADD COLUMN IF NOT EXISTS can_access_discount_tools BOOLEAN DEFAULT FALSE;

-- Grant to admin role by default
UPDATE roles SET can_access_discount_tools = TRUE WHERE name = 'admin';
