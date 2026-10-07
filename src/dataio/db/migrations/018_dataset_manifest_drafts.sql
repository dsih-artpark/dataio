-- Migration 018: Dataset Manifest Drafts
-- Staging area for LLM-drafted metadata.yaml, reviewed by a curator before
-- ever reaching the real store (datasets.manifest_yaml / S3 via
-- filestore_service). Deliberately separate from both of those: VersionType
-- (PREPROCESSED/STANDARDISED) represents pipeline stage, not review state,
-- and the datasets table only ever holds the live approved manifest.

BEGIN;

CREATE TYPE dataset_manifest_draft_status AS ENUM ('pending', 'approved', 'rejected', 'flagged');

CREATE TABLE IF NOT EXISTS dataset_manifest_drafts (
    draft_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id TEXT NULL,              -- nullable: brand-new datasets don't have a ds_id yet
    collection_id TEXT NOT NULL,
    category_id TEXT NOT NULL,         -- used for suggest_next_raw_dataset_id_for_category + audit
    source_csv_path TEXT NOT NULL,     -- what the drafter read, for reproducibility
    digitization_log_path TEXT NULL,
    status dataset_manifest_draft_status NOT NULL DEFAULT 'pending',
    draft_yaml TEXT NOT NULL,
    draft_json JSONB NOT NULL,
    flagged_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
    reviewer_notes JSONB NOT NULL DEFAULT '[]'::jsonb,
    validation_result JSONB NULL,
    llm_model_id TEXT NOT NULL,
    llm_prompt_tokens INTEGER NULL,
    llm_completion_tokens INTEGER NULL,
    created_by TEXT NOT NULL REFERENCES users(email),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reviewed_by TEXT NULL REFERENCES users(email),
    reviewed_at TIMESTAMP NULL,
    superseded_by_draft_id UUID NULL REFERENCES dataset_manifest_drafts(draft_id)
);

CREATE INDEX IF NOT EXISTS idx_dataset_manifest_drafts_status ON dataset_manifest_drafts(status);
CREATE INDEX IF NOT EXISTS idx_dataset_manifest_drafts_dataset_id ON dataset_manifest_drafts(dataset_id);

SELECT add_migration(18, '018_dataset_manifest_drafts');

COMMIT;
