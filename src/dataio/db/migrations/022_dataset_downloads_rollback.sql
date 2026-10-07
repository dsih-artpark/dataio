-- Rollback Migration 022: Dataset Download Audit & Metrics Logging

BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 22;

DROP TABLE IF EXISTS dataset_downloads;

COMMIT;
