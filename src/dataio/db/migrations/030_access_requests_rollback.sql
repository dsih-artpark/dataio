-- Rollback Migration 030: access requests and signed-out downloads
-- Drops access_requests (submitted requests are lost) and deletes signed-out
-- download log rows so dataset_downloads.user_email can be NOT NULL again.

BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 30;

DROP TABLE IF EXISTS access_requests;

DELETE FROM dataset_downloads WHERE user_email IS NULL;
ALTER TABLE dataset_downloads ALTER COLUMN user_email SET NOT NULL;

COMMIT;
