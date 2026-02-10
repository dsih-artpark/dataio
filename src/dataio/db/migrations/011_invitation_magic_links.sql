-- Migration 011: Invitation Magic Links
-- Adds support for magic link invitations with 48-hour expiry and admin revocation

-- Add invited_by column to track which admin sent the invitation
ALTER TABLE magic_link_tokens ADD COLUMN IF NOT EXISTS invited_by TEXT;

-- Comment on the column
COMMENT ON COLUMN magic_link_tokens.invited_by IS 'Email of admin who sent the invitation (for purpose=invitation)';

-- Create index for finding pending invitations by admin
CREATE INDEX IF NOT EXISTS idx_magic_link_tokens_invited_by ON magic_link_tokens(invited_by) WHERE invited_by IS NOT NULL;
