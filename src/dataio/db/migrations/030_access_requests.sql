-- Migration 030: access requests and signed-out downloads
-- 1. access_requests: "Request access" form submissions from the public site
--    (View only and Restricted datasets). Admins review them in the admin panel;
--    approving adds the requester to the dataset's access list.
-- 2. dataset_downloads.user_email becomes nullable so Open download datasets
--    can be downloaded without signing in (access_channel 'PUBLIC', IP kept).

BEGIN;

CREATE TABLE IF NOT EXISTS access_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id TEXT NOT NULL,
    full_name TEXT NOT NULL,
    email TEXT NOT NULL,
    organisation TEXT NOT NULL,
    organisation_type TEXT NOT NULL,
    organisation_type_other TEXT,
    purpose TEXT NOT NULL,
    consent BOOLEAN NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    ip_address TEXT,
    user_agent TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    reviewed_at TIMESTAMP WITH TIME ZONE,
    reviewed_by TEXT REFERENCES users(email) ON DELETE SET NULL,
    review_note TEXT,
    CONSTRAINT access_requests_status_check CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED')),
    CONSTRAINT access_requests_consent_check CHECK (consent)
);

CREATE INDEX IF NOT EXISTS idx_access_requests_status ON access_requests(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_access_requests_dataset_id ON access_requests(dataset_id);
CREATE INDEX IF NOT EXISTS idx_access_requests_email ON access_requests(lower(email));

ALTER TABLE dataset_downloads ALTER COLUMN user_email DROP NOT NULL;

COMMENT ON COLUMN dataset_downloads.user_email IS
    'Downloading user; NULL for signed-out downloads of Open datasets (access_channel PUBLIC).';

SELECT add_migration(30, '030_access_requests');

COMMIT;
