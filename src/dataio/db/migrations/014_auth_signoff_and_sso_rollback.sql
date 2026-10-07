BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 14;

DROP TABLE IF EXISTS oauth_identities;
DROP INDEX IF EXISTS idx_sessions_refresh_token_jti_hash;
ALTER TABLE sessions DROP COLUMN IF EXISTS refresh_token_jti_hash;

COMMIT;
