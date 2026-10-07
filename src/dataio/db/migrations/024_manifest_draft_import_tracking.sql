-- Migration 024: Track "Upload dataset now" imports on manifest drafts
-- Before this, nothing recorded that a draft had been imported: the UI
-- guessed from whether the dataset existed, so a half-finished import showed
-- as "Already uploaded" and could never be retried. These columns record the
-- upload itself:
--   import_started_at  set when an upload claims the draft (cleared again if
--                      it fails), so two uploads of one draft can't run at once
--   imported_at/by     set once the dataset is fully published
--   import_result      outcome of the latest attempt (dataset, tables or error)
-- Timestamps are naive UTC, like the table's other timestamps.

BEGIN;

ALTER TABLE dataset_manifest_drafts ADD COLUMN IF NOT EXISTS import_started_at TIMESTAMP NULL;
ALTER TABLE dataset_manifest_drafts ADD COLUMN IF NOT EXISTS imported_at TIMESTAMP NULL;
ALTER TABLE dataset_manifest_drafts ADD COLUMN IF NOT EXISTS imported_by TEXT NULL
    REFERENCES users(email) ON DELETE SET NULL;
ALTER TABLE dataset_manifest_drafts ADD COLUMN IF NOT EXISTS import_result JSONB NULL;

SELECT add_migration(24, '024_manifest_draft_import_tracking');

COMMIT;
