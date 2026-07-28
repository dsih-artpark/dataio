-- Rollback 021: restores NOT NULL on llm_model_id. Only safe if no
-- deterministic (llm_model_id IS NULL) drafts exist yet - back-fill or
-- delete them first, otherwise this ALTER will fail with a constraint
-- violation, the same way the original bug surfaced.
ALTER TABLE dataset_manifest_drafts ALTER COLUMN llm_model_id SET NOT NULL;
