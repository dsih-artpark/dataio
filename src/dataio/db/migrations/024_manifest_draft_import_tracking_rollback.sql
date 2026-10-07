-- Rollback Migration 024: drop the draft import tracking columns.
-- Datasets already imported from drafts are untouched; only the record of
-- which draft imported them is lost.

BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 24;

ALTER TABLE dataset_manifest_drafts DROP COLUMN IF EXISTS import_result;
ALTER TABLE dataset_manifest_drafts DROP COLUMN IF EXISTS imported_by;
ALTER TABLE dataset_manifest_drafts DROP COLUMN IF EXISTS imported_at;
ALTER TABLE dataset_manifest_drafts DROP COLUMN IF EXISTS import_started_at;

COMMIT;
