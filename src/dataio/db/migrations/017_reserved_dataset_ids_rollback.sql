BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 17;

DROP TABLE IF EXISTS reserved_dataset_ids;

COMMIT;
