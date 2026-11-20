-- Rollback Migration 029: dataset licence
-- Drops datasets.licence. Any licences recorded since 029 are lost.

BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 29;

ALTER TABLE datasets DROP COLUMN IF EXISTS licence;

COMMIT;
