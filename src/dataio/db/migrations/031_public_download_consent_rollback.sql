-- Rollback Migration 031: signed-out download email and visitor ID
-- Drops the columns: the emails and visitor IDs recorded for signed-out
-- downloads are lost (the download rows themselves stay).

BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 31;

DROP INDEX IF EXISTS idx_dataset_downloads_visitor_email;
DROP INDEX IF EXISTS idx_dataset_downloads_visitor_id;

ALTER TABLE dataset_downloads
    DROP COLUMN IF EXISTS terms_accepted_at,
    DROP COLUMN IF EXISTS visitor_id,
    DROP COLUMN IF EXISTS visitor_email;

COMMIT;
