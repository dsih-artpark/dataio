-- Rollback Migration 023: restore the original NO ACTION foreign keys.
-- SET NOT NULL fails if rows lost their author while 023 was applied;
-- backfill those rows (or delete them) before running this.

BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 23;

ALTER TABLE dataset_manifest_drafts DROP CONSTRAINT IF EXISTS dataset_manifest_drafts_created_by_fkey;
ALTER TABLE dataset_manifest_drafts ADD CONSTRAINT dataset_manifest_drafts_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(email);
ALTER TABLE dataset_manifest_drafts DROP CONSTRAINT IF EXISTS dataset_manifest_drafts_reviewed_by_fkey;
ALTER TABLE dataset_manifest_drafts ADD CONSTRAINT dataset_manifest_drafts_reviewed_by_fkey
    FOREIGN KEY (reviewed_by) REFERENCES users(email);
ALTER TABLE reserved_dataset_ids DROP CONSTRAINT IF EXISTS reserved_dataset_ids_reserved_by_fkey;
ALTER TABLE reserved_dataset_ids ADD CONSTRAINT reserved_dataset_ids_reserved_by_fkey
    FOREIGN KEY (reserved_by) REFERENCES users(email);
ALTER TABLE reserved_raw_dataset_ids DROP CONSTRAINT IF EXISTS reserved_raw_dataset_ids_reserved_by_fkey;
ALTER TABLE reserved_raw_dataset_ids ADD CONSTRAINT reserved_raw_dataset_ids_reserved_by_fkey
    FOREIGN KEY (reserved_by) REFERENCES users(email);

ALTER TABLE dataset_manifest_drafts ALTER COLUMN created_by SET NOT NULL;
ALTER TABLE reserved_dataset_ids ALTER COLUMN reserved_by SET NOT NULL;
ALTER TABLE reserved_raw_dataset_ids ALTER COLUMN reserved_by SET NOT NULL;

COMMIT;
