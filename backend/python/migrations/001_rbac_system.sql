-- Migration: Role-Based Access Control System
-- Date: 2026-02-03
-- Description: Adds roles table, updates users table with role_id FK, updates pages table with new access control columns

-- ============================================================================
-- STEP 1: Create roles table
-- ============================================================================

CREATE TABLE IF NOT EXISTS roles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(50) UNIQUE NOT NULL,
    description VARCHAR(255) DEFAULT '',
    can_access_scheduler BOOLEAN DEFAULT 0,
    can_manage_users BOOLEAN DEFAULT 0,
    can_manage_pages BOOLEAN DEFAULT 0,
    can_manage_roles BOOLEAN DEFAULT 0,
    can_manage_configs BOOLEAN DEFAULT 0,
    is_system BOOLEAN DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- STEP 2: Seed default roles
-- ============================================================================

INSERT OR IGNORE INTO roles (name, description, can_access_scheduler, can_manage_users, can_manage_pages, can_manage_roles, can_manage_configs, is_system)
VALUES
    ('admin', 'Full system access', 1, 1, 1, 1, 1, 1),
    ('scheduler_admin', 'Scheduler management', 1, 0, 0, 0, 0, 1),
    ('editor', 'Page management', 0, 0, 1, 0, 0, 1),
    ('viewer', 'Read-only access', 0, 0, 0, 0, 0, 1);

-- ============================================================================
-- STEP 3: Add role_id column to users table
-- ============================================================================

-- Check if column exists before adding (SQLite doesn't support IF NOT EXISTS for columns)
-- This will fail silently if column already exists
ALTER TABLE users ADD COLUMN role_id INTEGER REFERENCES roles(id);

-- ============================================================================
-- STEP 4: Migrate existing user roles to role_id
-- ============================================================================

-- Map 'admin' role string to admin role ID
UPDATE users
SET role_id = (SELECT id FROM roles WHERE name = 'admin')
WHERE role = 'admin' AND role_id IS NULL;

-- Map 'scheduler_admin' role string to scheduler_admin role ID
UPDATE users
SET role_id = (SELECT id FROM roles WHERE name = 'scheduler_admin')
WHERE role = 'scheduler_admin' AND role_id IS NULL;

-- Map 'editor' role string to editor role ID
UPDATE users
SET role_id = (SELECT id FROM roles WHERE name = 'editor')
WHERE role = 'editor' AND role_id IS NULL;

-- Map 'viewer' role string to viewer role ID (default)
UPDATE users
SET role_id = (SELECT id FROM roles WHERE name = 'viewer')
WHERE (role = 'viewer' OR role IS NULL) AND role_id IS NULL;

-- Set any remaining users without a role_id to viewer
UPDATE users
SET role_id = (SELECT id FROM roles WHERE name = 'viewer')
WHERE role_id IS NULL;

-- ============================================================================
-- STEP 5: Add new page access control columns
-- ============================================================================

-- Add is_public column (replaces is_secure with inverted logic)
ALTER TABLE pages ADD COLUMN is_public BOOLEAN DEFAULT 0;

-- Add view access control columns
ALTER TABLE pages ADD COLUMN view_roles VARCHAR(255) DEFAULT '';
ALTER TABLE pages ADD COLUMN view_users TEXT DEFAULT '';

-- Add edit access control columns
ALTER TABLE pages ADD COLUMN edit_roles VARCHAR(255) DEFAULT '';
ALTER TABLE pages ADD COLUMN edit_users TEXT DEFAULT '';

-- ============================================================================
-- STEP 6: Migrate existing page data
-- ============================================================================

-- Convert is_secure to is_public (inverted logic)
-- Pages that were NOT secure become public
UPDATE pages SET is_public = 1 WHERE is_secure = 0 OR is_secure IS NULL;
UPDATE pages SET is_public = 0 WHERE is_secure = 1;

-- Migrate edit_restricted to edit_roles (admin only)
UPDATE pages
SET edit_roles = (SELECT CAST(id AS TEXT) FROM roles WHERE name = 'admin')
WHERE edit_restricted = 1;

-- ============================================================================
-- VERIFICATION QUERIES (run manually to verify migration)
-- ============================================================================

-- Verify roles were created:
-- SELECT * FROM roles;

-- Verify users have role_id set:
-- SELECT id, username, role, role_id FROM users;

-- Verify pages have new columns:
-- SELECT id, slug, is_secure, is_public, view_roles, edit_roles FROM pages;

-- ============================================================================
-- CLEANUP (run manually after verification - OPTIONAL)
-- ============================================================================

-- WARNING: Only run these after confirming the migration worked correctly!
-- SQLite doesn't support DROP COLUMN easily, so these are commented out.
-- For production, you may need to recreate the tables without these columns.

-- ALTER TABLE users DROP COLUMN role;
-- ALTER TABLE pages DROP COLUMN is_secure;
-- ALTER TABLE pages DROP COLUMN edit_restricted;
