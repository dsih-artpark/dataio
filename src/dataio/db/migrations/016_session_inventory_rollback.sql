BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 16;

DROP INDEX IF EXISTS idx_sessions_user_active;

ALTER TABLE sessions
    DROP COLUMN IF EXISTS last_seen_at;

COMMIT;
