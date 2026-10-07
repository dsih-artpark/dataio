-- Migration 025: Allow dataset-deletion OTP codes
-- An admin deleting a dataset is emailed a confirmation code stored with
-- purpose 'dataset_deletion:<dataset id>' (web_admin_service
-- initiate_dataset_deletion), but 010's otp_tokens_purpose_check only allows
-- a fixed list, so inserting that code failed and dataset deletion returned
-- a server error. Keep the fixed list and also allow that prefix.

BEGIN;

ALTER TABLE otp_tokens DROP CONSTRAINT IF EXISTS otp_tokens_purpose_check;
ALTER TABLE otp_tokens ADD CONSTRAINT otp_tokens_purpose_check
    CHECK (
        purpose IN ('login', 'verify_email', 'invite', 'registration', 'account_deletion')
        OR purpose LIKE 'dataset\_deletion:_%'
    );

SELECT add_migration(25, '025_otp_dataset_deletion_purpose');

COMMIT;
