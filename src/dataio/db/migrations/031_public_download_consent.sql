-- Migration 031: who downloaded an Open dataset without signing in
-- The signed-out download pop-up asks for an email and agreement to the Terms
-- of Use and the dataset's licence, then remembers the visitor with a random
-- ID in a cookie (dataio_visitor) so later downloads don't ask again.
-- Only filled for access_channel 'PUBLIC'; signed-in downloads use user_email.

BEGIN;

ALTER TABLE dataset_downloads
    ADD COLUMN IF NOT EXISTS visitor_email TEXT,
    ADD COLUMN IF NOT EXISTS visitor_id UUID,
    ADD COLUMN IF NOT EXISTS terms_accepted_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX IF NOT EXISTS idx_dataset_downloads_visitor_id ON dataset_downloads(visitor_id);
CREATE INDEX IF NOT EXISTS idx_dataset_downloads_visitor_email ON dataset_downloads(lower(visitor_email));

COMMENT ON COLUMN dataset_downloads.visitor_email IS
    'Email given in the signed-out download pop-up (access_channel PUBLIC), lower-cased.';
COMMENT ON COLUMN dataset_downloads.visitor_id IS
    'Random ID from the dataio_visitor cookie, set once the visitor agreed to the terms.';
COMMENT ON COLUMN dataset_downloads.terms_accepted_at IS
    'When this visitor ticked the Terms of Use and licence box (carried over to later downloads).';

SELECT add_migration(31, '031_public_download_consent');

COMMIT;
