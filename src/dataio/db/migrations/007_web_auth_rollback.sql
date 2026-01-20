-- ROLLBACK SCRIPT for 007_web_auth.sql
-- Run this to undo the web auth migration if needed

BEGIN;

-- Drop new tables (in reverse dependency order)
DROP TABLE IF EXISTS user_api_keys CASCADE;
DROP TABLE IF EXISTS webauthn_challenges CASCADE;
DROP TABLE IF EXISTS webauthn_credentials CASCADE;
DROP TABLE IF EXISTS otp_tokens CASCADE;
DROP TABLE IF EXISTS sessions CASCADE;

-- Remove added columns from users table
ALTER TABLE users DROP COLUMN IF EXISTS email_verified;
ALTER TABLE users DROP COLUMN IF EXISTS last_login;
ALTER TABLE users DROP COLUMN IF EXISTS created_at;
ALTER TABLE users DROP COLUMN IF EXISTS display_name;

-- Restore original constraint (requires API key for non-group users)
ALTER TABLE users DROP CONSTRAINT IF EXISTS valid_user_group;
ALTER TABLE users ADD CONSTRAINT valid_user_group CHECK (
    (is_group = TRUE AND key IS NULL) OR
    (is_group = FALSE AND key IS NOT NULL)
);

-- Remove cleanup function
DROP FUNCTION IF EXISTS cleanup_expired_auth_data();

-- Remove migration record
DELETE FROM migrations WHERE migration_number = 7;

COMMIT;
