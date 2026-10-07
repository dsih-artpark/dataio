-- Migration 021: Allow NULL llm_model_id on dataset_manifest_drafts
-- llm_model_id was originally NOT NULL (018) since every draft came from
-- the LLM drafter. The deterministic (no-LLM) draft path added this
-- session sets llm_model_id=None deliberately - that's how the app tells
-- a deterministic draft apart from an LLM one (see
-- draft_review_service.regenerate_draft's dispatch, DraftDetail's "Model:
-- Deterministic (rule-based)" display) - so the column must allow NULL.

BEGIN;

ALTER TABLE dataset_manifest_drafts ALTER COLUMN llm_model_id DROP NOT NULL;

SELECT add_migration(21, '021_manifest_draft_llm_model_id_nullable');

COMMIT;
