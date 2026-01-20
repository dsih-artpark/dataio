BEGIN;

-- =============================================================================
-- Web Authentication Tables for Email OTP and Passkey (WebAuthn) Support
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Sessions table for JWT/refresh token tracking
-- -----------------------------------------------------------------------------
CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_email TEXT NOT NULL REFERENCES users(email) ON DELETE CASCADE,
    refresh_token TEXT NOT NULL UNIQUE,
    user_agent TEXT,
    ip_address TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    revoked_at TIMESTAMP
);

CREATE INDEX idx_sessions_user_email ON sessions(user_email);
CREATE INDEX idx_sessions_refresh_token ON sessions(refresh_token);
CREATE INDEX idx_sessions_expires_at ON sessions(expires_at);

-- -----------------------------------------------------------------------------
-- OTP tokens for email verification and login
-- -----------------------------------------------------------------------------
CREATE TABLE otp_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL,
    code TEXT NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose IN ('login', 'verify_email', 'invite')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    used_at TIMESTAMP,
    attempts INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_otp_tokens_email ON otp_tokens(email);
CREATE INDEX idx_otp_tokens_expires_at ON otp_tokens(expires_at);

-- -----------------------------------------------------------------------------
-- WebAuthn credentials for passkey storage
-- -----------------------------------------------------------------------------
CREATE TABLE webauthn_credentials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_email TEXT NOT NULL REFERENCES users(email) ON DELETE CASCADE,
    credential_id TEXT NOT NULL UNIQUE,
    public_key BYTEA NOT NULL,
    sign_count INTEGER NOT NULL DEFAULT 0,
    device_name TEXT,
    transports TEXT[], -- Array of transport types (usb, nfc, ble, internal, hybrid)
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMP
);

CREATE INDEX idx_webauthn_credentials_user_email ON webauthn_credentials(user_email);
CREATE INDEX idx_webauthn_credentials_credential_id ON webauthn_credentials(credential_id);

-- -----------------------------------------------------------------------------
-- WebAuthn challenges for registration/authentication (temporary storage)
-- -----------------------------------------------------------------------------
CREATE TABLE webauthn_challenges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_email TEXT NOT NULL,
    challenge TEXT NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose IN ('registration', 'authentication')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_webauthn_challenges_user_email ON webauthn_challenges(user_email);
CREATE INDEX idx_webauthn_challenges_expires_at ON webauthn_challenges(expires_at);

-- -----------------------------------------------------------------------------
-- User API keys table (for self-service API key management)
-- -----------------------------------------------------------------------------
CREATE TABLE user_api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_email TEXT NOT NULL REFERENCES users(email) ON DELETE CASCADE,
    key_hash TEXT NOT NULL,
    key_prefix TEXT NOT NULL, -- First 8 chars for identification
    name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMP,
    expires_at TIMESTAMP,
    revoked_at TIMESTAMP
);

CREATE INDEX idx_user_api_keys_user_email ON user_api_keys(user_email);
CREATE INDEX idx_user_api_keys_key_hash ON user_api_keys(key_hash);

-- -----------------------------------------------------------------------------
-- Extend users table with web auth fields
-- -----------------------------------------------------------------------------
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT;

-- -----------------------------------------------------------------------------
-- Relax constraint to allow web users without API keys (key can be null)
-- -----------------------------------------------------------------------------
ALTER TABLE users DROP CONSTRAINT IF EXISTS valid_user_group;
ALTER TABLE users ADD CONSTRAINT valid_user_group CHECK (
    (is_group = TRUE AND key IS NULL) OR
    (is_group = FALSE)
);

-- -----------------------------------------------------------------------------
-- Helper function to clean up expired tokens and sessions
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION cleanup_expired_auth_data()
RETURNS void AS $$
BEGIN
    -- Delete expired OTP tokens
    DELETE FROM otp_tokens WHERE expires_at < NOW();

    -- Delete expired WebAuthn challenges
    DELETE FROM webauthn_challenges WHERE expires_at < NOW();

    -- Delete expired sessions and revoked sessions older than 30 days
    DELETE FROM sessions
    WHERE expires_at < NOW()
       OR (revoked_at IS NOT NULL AND revoked_at < NOW() - INTERVAL '30 days');
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- Record migration
-- -----------------------------------------------------------------------------
SELECT add_migration(7, '007_web_auth');

COMMIT;
