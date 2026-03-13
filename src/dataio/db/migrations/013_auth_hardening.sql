BEGIN;

CREATE TABLE IF NOT EXISTS auth_rate_limits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action TEXT NOT NULL,
    subject TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    window_started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    blocked_until TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_rate_limits_action_subject
    ON auth_rate_limits(action, subject);

CREATE INDEX IF NOT EXISTS idx_auth_rate_limits_blocked_until
    ON auth_rate_limits(blocked_until);

CREATE TABLE IF NOT EXISTS auth_audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    outcome TEXT NOT NULL,
    actor_email TEXT,
    target_email TEXT,
    ip_address TEXT,
    user_agent TEXT,
    details JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_auth_audit_logs_created_at
    ON auth_audit_logs(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_auth_audit_logs_target_email
    ON auth_audit_logs(target_email);

CREATE INDEX IF NOT EXISTS idx_auth_audit_logs_event_type
    ON auth_audit_logs(event_type);

SELECT add_migration(13, '013_auth_hardening');

COMMIT;
