-- Rollback Migration 025: back to 010's fixed list of OTP purposes.
-- Only roll this back together with the app code: any version that has
-- admin dataset deletion stores 'dataset_deletion:<id>' codes, which the
-- narrower constraint rejects, so deletion would fail with a server error.
-- Dataset-deletion codes are short-lived; any still stored are removed so
-- the narrower constraint can be added (pending deletions must be restarted).

BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 25;

DELETE FROM otp_tokens WHERE purpose LIKE 'dataset!_deletion:%' ESCAPE '!';

ALTER TABLE otp_tokens DROP CONSTRAINT IF EXISTS otp_tokens_purpose_check;
ALTER TABLE otp_tokens ADD CONSTRAINT otp_tokens_purpose_check
    CHECK (purpose IN ('login', 'verify_email', 'invite', 'registration', 'account_deletion'));

COMMIT;
