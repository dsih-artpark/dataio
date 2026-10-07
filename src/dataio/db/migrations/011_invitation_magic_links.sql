-- Migration 011: Invitation Magic Links
-- Adds support for magic link invitations with 48-hour expiry and admin revocation

BEGIN;

-- Schema from the archived archive/008_registration_and_verification.sql,
-- which the runner never applies (its number clashes with
-- 008_dataset_documentation). Without it a fresh database has no
-- magic_link_tokens table and no users verification columns, so this
-- migration fails. Everything here is IF NOT EXISTS / OR REPLACE, so it is
-- a no-op on databases that already have the archived 008 applied. Its two
-- UPDATE users backfills are left out on purpose: they would change
-- existing rows on databases where this migration re-runs. The rollback
-- deliberately leaves these objects in place.
CREATE TABLE IF NOT EXISTS magic_link_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    purpose TEXT NOT NULL,  -- 'registration', 'account_deletion', 'invitation'
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    used_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_magic_link_tokens_token ON magic_link_tokens(token);
CREATE INDEX IF NOT EXISTS idx_magic_link_tokens_email ON magic_link_tokens(email);
CREATE INDEX IF NOT EXISTS idx_magic_link_tokens_expires ON magic_link_tokens(expires_at);

ALTER TABLE users ADD COLUMN IF NOT EXISTS verification_status TEXT DEFAULT 'verified';
ALTER TABLE users ADD COLUMN IF NOT EXISTS registered_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS verified_by TEXT;

CREATE INDEX IF NOT EXISTS idx_users_verification_status ON users(verification_status);

CREATE OR REPLACE FUNCTION cleanup_expired_magic_links()
RETURNS void AS $$
BEGIN
    DELETE FROM magic_link_tokens
    WHERE expires_at < NOW() - INTERVAL '1 day';
END;
$$ LANGUAGE plpgsql;

-- Add invited_by column to track which admin sent the invitation
ALTER TABLE magic_link_tokens ADD COLUMN IF NOT EXISTS invited_by TEXT;

-- Comment on the column
COMMENT ON COLUMN magic_link_tokens.invited_by IS 'Email of admin who sent the invitation (for purpose=invitation)';

-- Create index for finding pending invitations by admin
CREATE INDEX IF NOT EXISTS idx_magic_link_tokens_invited_by ON magic_link_tokens(invited_by) WHERE invited_by IS NOT NULL;

SELECT add_migration(11, '011_invitation_magic_links');

COMMIT;
