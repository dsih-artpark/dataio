BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 18;

DROP TABLE IF EXISTS dataset_manifest_drafts;
DROP TYPE IF EXISTS dataset_manifest_draft_status;

COMMIT;
