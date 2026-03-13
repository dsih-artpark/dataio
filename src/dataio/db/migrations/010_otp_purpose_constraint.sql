-- Migration 010: Update OTP tokens purpose constraint
-- Adds 'registration' and 'account_deletion' to allowed purposes

BEGIN;

-- Drop the existing constraint
ALTER TABLE otp_tokens DROP CONSTRAINT IF EXISTS otp_tokens_purpose_check;

-- Add the new constraint with additional purposes
ALTER TABLE otp_tokens ADD CONSTRAINT otp_tokens_purpose_check
    CHECK (purpose IN ('login', 'verify_email', 'invite', 'registration', 'account_deletion'));

SELECT add_migration(10, '010_otp_purpose_constraint');

COMMIT;
