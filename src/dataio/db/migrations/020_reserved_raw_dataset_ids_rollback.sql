BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 20;

DROP TABLE IF EXISTS reserved_raw_dataset_ids;

COMMIT;
