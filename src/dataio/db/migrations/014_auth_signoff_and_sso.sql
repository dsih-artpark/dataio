BEGIN;

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS refresh_token_jti_hash TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_refresh_token_jti_hash
    ON sessions(refresh_token_jti_hash)
    WHERE refresh_token_jti_hash IS NOT NULL;

ALTER TABLE sessions
    ALTER COLUMN refresh_token DROP NOT NULL;

CREATE TABLE IF NOT EXISTS oauth_identities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider TEXT NOT NULL,
    provider_user_id TEXT NOT NULL,
    user_email TEXT NOT NULL REFERENCES users(email) ON DELETE CASCADE,
    provider_email TEXT,
    provider_email_verified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_oauth_identities_provider_user
    ON oauth_identities(provider, provider_user_id);

CREATE INDEX IF NOT EXISTS idx_oauth_identities_user_email
    ON oauth_identities(user_email);

SELECT add_migration(14, '014_auth_signoff_and_sso');

COMMIT;
