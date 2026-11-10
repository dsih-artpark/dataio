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

COMMENT ON COLUMN dataset_downloads.visitor_email IS
    'Email given in the signed-out download pop-up (access_channel PUBLIC), lower-cased.';
COMMENT ON COLUMN dataset_downloads.visitor_id IS
    'Random ID from the dataio_visitor cookie, set once the visitor agreed to the terms.';
COMMENT ON COLUMN dataset_downloads.terms_accepted_at IS
    'When this visitor ticked the Terms of Use and licence box (carried over to later downloads).';

COMMIT;

-- CONCURRENTLY so downloads keep being logged while the indexes build. It can't
-- run inside a transaction, so these come after COMMIT (the runner applies this
-- file with psql -f, without --single-transaction). The migration is recorded
-- only after both indexes exist, so a failed build is retried on the next run;
-- if a build fails, drop the INVALID index it leaves before re-running.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dataset_downloads_visitor_id ON dataset_downloads(visitor_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dataset_downloads_visitor_email ON dataset_downloads(lower(visitor_email));

SELECT add_migration(31, '031_public_download_consent');
