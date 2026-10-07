BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 13;

DROP TABLE IF EXISTS auth_audit_logs;
DROP TABLE IF EXISTS auth_rate_limits;

COMMIT;
