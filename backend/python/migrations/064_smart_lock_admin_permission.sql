-- 064_smart_lock_admin_permission.sql
-- Split smart-lock permission into "access" (ops) and "admin" (config / bridges
-- / keypads / padlocks). Ops staff need to link keypad/padlock on the unit
-- assignment page and trigger refresh — but should NOT manage the bridge/
-- keypad/padlock inventory or change site config.
--
-- Target DB: esa_backend

ALTER TABLE roles
    ADD COLUMN IF NOT EXISTS can_admin_smart_lock BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN roles.can_admin_smart_lock IS
    'Manage bridges/keypads/padlocks inventory + site config. can_access_smart_lock is still required for the assignment + refresh workflow.';

-- Existing admin-tier roles retain their previous level of access.
UPDATE roles SET can_admin_smart_lock = TRUE
WHERE name IN ('admin', 'smartlock_admin');

-- New ops-tier role: assignment + refresh only, no inventory management.
INSERT INTO roles (name, can_access_smart_lock, can_admin_smart_lock)
SELECT 'smartlock_ops', TRUE, FALSE
WHERE NOT EXISTS (SELECT 1 FROM roles WHERE name = 'smartlock_ops');

-- Auto-grant smartlock_ops to every user who already has ecri_ops
-- (same site-ops cohort — they need to link keypad/padlock during ECRI work).
INSERT INTO user_roles (user_id, role_id)
SELECT ur.user_id, (SELECT id FROM roles WHERE name = 'smartlock_ops')
FROM user_roles ur
JOIN roles r ON r.id = ur.role_id
WHERE r.name = 'ecri_ops'
ON CONFLICT (user_id, role_id) DO NOTHING;
