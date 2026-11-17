-- Migration 032: dataset changelog entries (DIO-53)
-- One row per update to a published dataset's data or metadata. Admins record
-- entries in the admin panel (as part of Replace, or by hand), save them as
-- DRAFT and publish them; published entries show on the public site.
-- dataset_id has no foreign key so the history survives a dataset deletion
-- (the public site hides entries whose dataset no longer exists).
-- archive_prefix links an entry to the archive copy a Replace made; it is
-- internal and never served publicly.

BEGIN;

CREATE TABLE IF NOT EXISTS dataset_changes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id TEXT NOT NULL,
    change_date DATE NOT NULL,
    change_type TEXT NOT NULL,
    breaking BOOLEAN NOT NULL DEFAULT FALSE,
    summary TEXT NOT NULL,
    reason TEXT NOT NULL,
    changes JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL DEFAULT 'DRAFT',
    archive_prefix TEXT,
    created_by TEXT REFERENCES users(email) ON DELETE SET NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_by TEXT REFERENCES users(email) ON DELETE SET NULL,
    updated_at TIMESTAMP WITH TIME ZONE,
    published_by TEXT REFERENCES users(email) ON DELETE SET NULL,
    published_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT dataset_changes_status_check CHECK (status IN ('DRAFT', 'PUBLISHED')),
    CONSTRAINT dataset_changes_type_check CHECK (change_type IN ('data', 'metadata')),
    CONSTRAINT dataset_changes_summary_check CHECK (btrim(summary) <> ''),
    CONSTRAINT dataset_changes_reason_check CHECK (btrim(reason) <> '')
);

CREATE INDEX IF NOT EXISTS idx_dataset_changes_dataset ON dataset_changes(dataset_id, change_date DESC);
CREATE INDEX IF NOT EXISTS idx_dataset_changes_status ON dataset_changes(status, change_date DESC);

SELECT add_migration(32, '032_dataset_changes');

COMMIT;
