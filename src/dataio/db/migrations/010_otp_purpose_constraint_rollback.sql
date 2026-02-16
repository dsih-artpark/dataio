-- Rollback Migration 010: Revert OTP tokens purpose constraint
-- WARNING: This will fail if there are rows with 'registration' or 'account_deletion' purposes

BEGIN;

-- Drop the expanded constraint
ALTER TABLE otp_tokens DROP CONSTRAINT IF EXISTS otp_tokens_purpose_check;

-- Restore the original constraint
ALTER TABLE otp_tokens ADD CONSTRAINT otp_tokens_purpose_check
    CHECK (purpose IN ('login', 'verify_email', 'invite'));

COMMIT;
