-- Migration 008: Add documentation caching fields to datasets table
-- Caches README.md and metadata.yaml (data dictionary) content from file server

BEGIN;

-- Add documentation columns to datasets table
ALTER TABLE datasets
ADD COLUMN IF NOT EXISTS readme_md TEXT,
ADD COLUMN IF NOT EXISTS data_dictionary_yaml TEXT,
ADD COLUMN IF NOT EXISTS documentation_synced_at TIMESTAMP WITH TIME ZONE;

-- Add comment for documentation columns
COMMENT ON COLUMN datasets.readme_md IS 'Cached README.md content from file server';
COMMENT ON COLUMN datasets.data_dictionary_yaml IS 'Cached metadata.yaml (data dictionary) content from file server';
COMMENT ON COLUMN datasets.documentation_synced_at IS 'Timestamp of last documentation sync from file server';

-- Update the datasets_full_view to include documentation fields
DROP VIEW IF EXISTS datasets_full_view;

CREATE VIEW datasets_full_view AS
SELECT d.id, array_agg(DISTINCT rd.rds_id) AS rds_ids, d.ds_id, d.title, c.collection_id, c.collection_name, c.category_id, c.category_name,
do2."name" AS data_owner_name, do2.contact_person AS data_owner_contact_person, do2.contact_person_email AS data_owner_contact_person_email,
d.description, array_agg(DISTINCT t.tag_name) AS tags,
r.region_name AS spatial_coverage, d.spatial_resolution, d.temporal_coverage_start_date, d.temporal_coverage_end_date, d.temporal_resolution,
d.access_level, d.additional_metadata,
d.readme_md, d.data_dictionary_yaml, d.documentation_synced_at
FROM datasets d
LEFT JOIN collections c ON d.collection_id = c.id
LEFT JOIN data_owners do2 ON d.data_owner_id = do2.id
LEFT JOIN dataset_raw_datasets drd ON d.id = drd.dataset_id
LEFT JOIN raw_datasets rd ON rd.id = drd.raw_dataset_id
LEFT JOIN dataset_tags dt ON dt.dataset_id = d.id
LEFT JOIN tags t ON t.id = dt.tag_id
LEFT JOIN regions r ON r.region_id = d.spatial_coverage_region_id
GROUP BY d.id, d.ds_id, d.title, c.collection_id, c.collection_name, c.category_id, c.category_name,
data_owner_name, data_owner_contact_person, data_owner_contact_person_email,
d.description, spatial_coverage, d.spatial_resolution, d.temporal_coverage_start_date, d.temporal_coverage_end_date, d.temporal_resolution,
d.access_level, d.additional_metadata, d.readme_md, d.data_dictionary_yaml, d.documentation_synced_at;

SELECT add_migration(8, '008_dataset_documentation');

COMMIT;
