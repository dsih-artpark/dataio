-- Rollback Migration 032: dataset changelog entries
-- Drops dataset_changes: every recorded changelog entry is lost.

BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 32;

DROP TABLE IF EXISTS dataset_changes;

COMMIT;
