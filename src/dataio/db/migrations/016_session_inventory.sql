BEGIN;

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP;

UPDATE sessions
SET last_seen_at = COALESCE(last_seen_at, created_at, NOW())
WHERE last_seen_at IS NULL;

ALTER TABLE sessions
    ALTER COLUMN last_seen_at SET DEFAULT NOW();

ALTER TABLE sessions
    ALTER COLUMN last_seen_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sessions_user_active
    ON sessions(user_email, revoked_at, expires_at);

SELECT add_migration(16, '016_session_inventory');

COMMIT;
