-- Migration 020: Reserved Raw Dataset IDs
-- Mirrors reserved_dataset_ids (017) but for rds_id, which is scoped by
-- category rather than collection. Needed so two concurrent LLM-drafted
-- manifests in the same category can't be handed the same suggested
-- rds_id: generate_draft reserves the id it resolves immediately, the same
-- way it already reserves a fresh ds_id via reserved_dataset_ids.

BEGIN;

CREATE TABLE IF NOT EXISTS reserved_raw_dataset_ids (
    id SERIAL PRIMARY KEY,
    rds_id TEXT NOT NULL UNIQUE,
    category_id TEXT NULL,
    note TEXT NULL,
    reserved_by TEXT NOT NULL REFERENCES users(email),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_reserved_raw_dataset_ids_category_id
    ON reserved_raw_dataset_ids(category_id);

SELECT add_migration(20, '020_reserved_raw_dataset_ids');

COMMIT;
