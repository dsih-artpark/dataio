-- Rollback Migration 011: Invitation Magic Links

-- Drop the index
DROP INDEX IF EXISTS idx_magic_link_tokens_invited_by;

-- Remove the invited_by column
ALTER TABLE magic_link_tokens DROP COLUMN IF EXISTS invited_by;
