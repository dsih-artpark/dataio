BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 19;

ALTER TABLE dataset_manifest_drafts DROP COLUMN IF EXISTS raw_dataset_id;

COMMIT;
