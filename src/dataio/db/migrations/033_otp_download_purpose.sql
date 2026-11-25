-- Migration 033: Allow email codes for signed-out downloads
-- A visitor who downloads an Open dataset without an account now confirms
-- their email with a code (public_access_service send_download_code), stored
-- with purpose 'download'. 025's otp_tokens_purpose_check only allows a fixed
-- list plus the dataset-deletion prefix, so add 'download' to the list.
-- test_otp_purposes.py checks every purpose the code uses against this.

BEGIN;

ALTER TABLE otp_tokens DROP CONSTRAINT IF EXISTS otp_tokens_purpose_check;
ALTER TABLE otp_tokens ADD CONSTRAINT otp_tokens_purpose_check
    CHECK (
        purpose IN ('login', 'verify_email', 'invite', 'registration', 'account_deletion', 'download')
        OR purpose LIKE 'dataset!_deletion:_%' ESCAPE '!'
    );

SELECT add_migration(33, '033_otp_download_purpose');

COMMIT;
