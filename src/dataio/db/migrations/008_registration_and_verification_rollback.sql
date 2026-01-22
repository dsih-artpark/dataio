-- Rollback Migration 008: Registration and User Verification

-- Drop magic link tokens table
DROP TABLE IF EXISTS magic_link_tokens;

-- Remove verification columns from users table
ALTER TABLE users DROP COLUMN IF EXISTS verification_status;
ALTER TABLE users DROP COLUMN IF EXISTS registered_at;
ALTER TABLE users DROP COLUMN IF EXISTS verified_at;
ALTER TABLE users DROP COLUMN IF EXISTS verified_by;

-- Drop cleanup function
DROP FUNCTION IF EXISTS cleanup_expired_magic_links();

-- Drop index
DROP INDEX IF EXISTS idx_users_verification_status;
