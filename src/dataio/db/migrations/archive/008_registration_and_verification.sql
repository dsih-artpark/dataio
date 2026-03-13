-- Legacy untracked migration candidate kept for reference only.
-- Migration 008: Registration and User Verification
-- Adds support for self-registration with email verification and admin approval

-- Magic link tokens for registration and account deletion
CREATE TABLE IF NOT EXISTS magic_link_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    purpose TEXT NOT NULL,  -- 'registration', 'account_deletion'
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    used_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_magic_link_tokens_token ON magic_link_tokens(token);
CREATE INDEX IF NOT EXISTS idx_magic_link_tokens_email ON magic_link_tokens(email);
CREATE INDEX IF NOT EXISTS idx_magic_link_tokens_expires ON magic_link_tokens(expires_at);

-- Add verification columns to users table
ALTER TABLE users ADD COLUMN IF NOT EXISTS verification_status TEXT DEFAULT 'verified';
ALTER TABLE users ADD COLUMN IF NOT EXISTS registered_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS verified_by TEXT;

-- Set existing users as verified (they were created by admins)
UPDATE users SET verification_status = 'verified' WHERE verification_status IS NULL;

-- For existing users, set registered_at to created_at if available
UPDATE users SET registered_at = created_at WHERE registered_at IS NULL AND created_at IS NOT NULL;

-- Create index for finding pending users
CREATE INDEX IF NOT EXISTS idx_users_verification_status ON users(verification_status);

-- Cleanup function for expired magic link tokens
CREATE OR REPLACE FUNCTION cleanup_expired_magic_links()
RETURNS void AS $$
BEGIN
    DELETE FROM magic_link_tokens
    WHERE expires_at < NOW() - INTERVAL '1 day';
END;
$$ LANGUAGE plpgsql;

-- Comment on new columns
COMMENT ON COLUMN users.verification_status IS 'User verification status: verified, pending, rejected';
COMMENT ON COLUMN users.registered_at IS 'When the user registered (self-registration)';
COMMENT ON COLUMN users.verified_at IS 'When the user was verified by admin';
COMMENT ON COLUMN users.verified_by IS 'Email of admin who verified the user';
COMMENT ON TABLE magic_link_tokens IS 'Tokens for magic link email verification';
