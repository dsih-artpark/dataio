-- Rollback Migration 033: back to 025's OTP purposes (no 'download').
-- Only roll this back together with the app code: any version that asks
-- signed-out visitors for an email code stores 'download' codes, which the
-- narrower constraint rejects, so sending a code would fail with a server error.
-- Download codes are short-lived; any still stored are removed so the
-- narrower constraint can be added (visitors waiting for one ask again).

BEGIN;

DELETE FROM db_migration_history WHERE migration_number = 33;

DELETE FROM otp_tokens WHERE purpose = 'download';

ALTER TABLE otp_tokens DROP CONSTRAINT IF EXISTS otp_tokens_purpose_check;
ALTER TABLE otp_tokens ADD CONSTRAINT otp_tokens_purpose_check
    CHECK (
        purpose IN ('login', 'verify_email', 'invite', 'registration', 'account_deletion')
        OR purpose LIKE 'dataset!_deletion:_%' ESCAPE '!'
    );

COMMIT;
