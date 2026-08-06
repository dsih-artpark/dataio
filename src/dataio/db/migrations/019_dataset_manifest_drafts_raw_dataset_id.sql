-- Migration 019: track the resolved raw_dataset_id on the draft row itself,
-- not inside draft_json/draft_yaml. metadata.yaml never contains a
-- raw_dataset/rds_id field in real production files - that belongs only in
-- info.yml, generated later. Storing it as a column here (rather than
-- polluting the manifest) keeps the drafted YAML schema-pure while still
-- letting a curator see it during review and carry it into info.yml when
-- they manually import the approved draft (approve_draft itself does not
-- create a RawDataset row - see DatasetManifestDraft's docstring).

BEGIN;

ALTER TABLE dataset_manifest_drafts ADD COLUMN IF NOT EXISTS raw_dataset_id TEXT NULL;

SELECT add_migration(19, '019_dataset_manifest_drafts_raw_dataset_id');

COMMIT;
