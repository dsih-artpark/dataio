-- Rollback Migration 026: Versioned shapefiles
-- Only roll this back together with the app code, and after 027's rollback.
-- The S3 files are not touched: legacy files stay at their old keys, but files
-- of newer versions (shapefiles/<version>/...) are no longer served by the old code.

BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 26;

DROP TABLE IF EXISTS shapefiles;
DROP TABLE IF EXISTS shapefile_versions;

COMMIT;
