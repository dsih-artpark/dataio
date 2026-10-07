-- Rollback Migration 009: Remove user suspension fields

BEGIN;

DROP INDEX IF EXISTS idx_users_suspended_at;

ALTER TABLE users
DROP COLUMN IF EXISTS suspended_at,
DROP COLUMN IF EXISTS suspended_by;

DELETE FROM db_migration_history WHERE migration_number = 9;

COMMIT;
