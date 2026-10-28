-- Migration 026: Versioned shapefiles
-- Shapefiles used to live only in S3 at shapefiles/<parent_id>/<region_id>.geojson.gz,
-- one file per region, found by walking the bucket on every list request.
-- Each upload batch (e.g. a new KGIS release) is now a version, and every file
-- is indexed here so list/download don't walk S3 and old versions are kept.
--   shapefile_versions: one row per batch; released_at orders them, and a
--     download without a version gets the newest version that has the region.
--   shapefiles: one row per (region, version), pointing at its S3 key.
-- New versions are stored at shapefiles/<version>/<parent_id>/<region_id>.geojson.gz.
-- The pre-versioning files stay where they are as version 'legacy'; run
-- `shapefile-backfill` once after this migration to index them.

BEGIN;

CREATE TABLE IF NOT EXISTS shapefile_versions (
    version TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    dataset_id TEXT,  -- catalogue ds_id these boundaries are published as, if any
    description TEXT,
    released_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by TEXT REFERENCES users(email) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS shapefiles (
    region_id TEXT NOT NULL REFERENCES regions(region_id),
    version TEXT NOT NULL REFERENCES shapefile_versions(version),
    s3_key TEXT NOT NULL UNIQUE,
    size_bytes BIGINT,
    uploaded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    uploaded_by TEXT REFERENCES users(email) ON DELETE SET NULL,
    PRIMARY KEY (region_id, version)
);

CREATE INDEX IF NOT EXISTS idx_shapefiles_version ON shapefiles(version);

-- The files uploaded before versioning (Shapefiles India, GS0012DS0051).
-- Dated to the prod upload so every later version sorts after it.
INSERT INTO shapefile_versions (version, source, dataset_id, description, released_at)
VALUES (
    'legacy',
    'Shapefiles uploaded before versioning',
    'GS0012DS0051',
    'Files at shapefiles/<parent_id>/<region_id>.geojson.gz, indexed by shapefile-backfill',
    '2025-09-16T00:00:00Z'
)
ON CONFLICT (version) DO NOTHING;

SELECT add_migration(26, '026_shapefile_versions');

COMMIT;
