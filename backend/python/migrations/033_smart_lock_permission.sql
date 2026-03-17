-- Add smart lock permission to roles table
-- Run against: esa_backend DB
BEGIN;

ALTER TABLE roles ADD COLUMN IF NOT EXISTS can_access_smart_lock BOOLEAN DEFAULT FALSE;
UPDATE roles SET can_access_smart_lock = TRUE WHERE name = 'admin';

COMMIT;
