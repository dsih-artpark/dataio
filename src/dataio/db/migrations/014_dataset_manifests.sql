-- Migration 014: Add dataset manifest cache fields and include them in datasets_full_view

BEGIN;

ALTER TABLE datasets
ADD COLUMN IF NOT EXISTS manifest_yaml TEXT,
ADD COLUMN IF NOT EXISTS manifest_json JSONB,
ADD COLUMN IF NOT EXISTS manifest_updated_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS manifest_updated_by TEXT;

COMMENT ON COLUMN datasets.manifest_yaml IS 'Cached canonical manifest.yaml content from filestore';
COMMENT ON COLUMN datasets.manifest_json IS 'Cached normalized manifest JSON derived from manifest.yaml';
COMMENT ON COLUMN datasets.manifest_updated_at IS 'Timestamp of last direct manifest update';
COMMENT ON COLUMN datasets.manifest_updated_by IS 'Actor email that last updated the manifest cache';

DROP VIEW IF EXISTS datasets_full_view;

CREATE VIEW datasets_full_view AS
SELECT d.id, array_agg(DISTINCT rd.rds_id) AS rds_ids, d.ds_id, d.title, c.collection_id, c.collection_name, c.category_id, c.category_name,
do2."name" AS data_owner_name, do2.contact_person AS data_owner_contact_person, do2.contact_person_email AS data_owner_contact_person_email,
d.description, array_agg(DISTINCT t.tag_name) AS tags,
r.region_name AS spatial_coverage, d.spatial_resolution, d.temporal_coverage_start_date, d.temporal_coverage_end_date, d.temporal_resolution,
d.access_level, d.additional_metadata,
d.readme_md, d.data_dictionary_json, d.manifest_yaml, d.manifest_json, d.manifest_updated_at, d.manifest_updated_by, d.documentation_synced_at
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
d.access_level, d.additional_metadata,
d.readme_md, d.data_dictionary_json, d.manifest_yaml, d.manifest_json, d.manifest_updated_at, d.manifest_updated_by, d.documentation_synced_at;

SELECT add_migration(14, '014_dataset_manifests');

COMMIT;
