-- Rollback for 067_rename_scheduler_perm_to_sync.sql
-- DO NOT RUN unless you're explicitly reverting the scheduler decommissioning.
-- Target DB: esa_backend

BEGIN;

ALTER TABLE roles RENAME COLUMN can_access_sync TO can_access_scheduler;

UPDATE roles
   SET name = 'scheduler_admin',
       description = 'Scheduler management'
 WHERE name = 'sync_admin';

COMMIT;
