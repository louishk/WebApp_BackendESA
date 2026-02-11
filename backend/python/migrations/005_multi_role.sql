-- Migration 005: Multi-role support for users
-- Creates user_roles join table for many-to-many relationship between users and roles.
-- Migrates existing role_id data into the join table.
-- The users.role_id column is kept for backward compatibility but no longer used by the app.

CREATE TABLE IF NOT EXISTS user_roles (
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);

-- Migrate existing single-role assignments into the join table
INSERT INTO user_roles (user_id, role_id)
SELECT id, role_id FROM users WHERE role_id IS NOT NULL
ON CONFLICT DO NOTHING;
