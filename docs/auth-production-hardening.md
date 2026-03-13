# Auth Production Hardening

This branch implements the production sign-off hardening pass for web authentication.

## Included in this pass

- Enforced account state checks for web authentication.
  - Suspended users can no longer authenticate.
  - `pending` and `rejected` users can no longer authenticate.
  - Existing access tokens for blocked accounts are rejected on protected routes.
- Added DB-backed auth rate limiting.
  - Login initiate and verify.
  - Registration initiate and verify.
  - Passkey login options and verify.
  - Refresh token rotation.
  - Account deletion initiate and verify.
- Added auth/security audit logging.
  - Login, registration, refresh, logout, logout-all.
  - Passkey add/remove and passkey login.
  - Invitation send/resend/accept/revoke.
  - Admin verify/reject/suspend/unsuspend.
  - API key create/revoke.
- Updated registration UX contract.
  - Sign-in and sign-up are now unified through the same email OTP entry point.
  - First-time users are auto-provisioned on successful verification.
  - Users are routed automatically to `verified` or `pending` based on domain policy.
  - Pending users no longer receive an authenticated session.
  - The frontend keeps pending users logged out until approval.
- Moved refresh-token handling to `HttpOnly` cookies with refresh-token rotation.
  - The browser no longer stores refresh tokens in JavaScript-accessible storage.
  - New session rows store a hashed refresh-token JTI instead of a raw refresh token.
- Hashed OTP codes at rest using `OTP_SECRET_KEY`.
- Added Google and GitHub SSO.
  - Google requires a verified provider email.
  - GitHub checks all available verified emails and matches any existing user before auto-provisioning.
- Improved passkey UX and privacy.
  - Passkey sign-in can start without entering an email using discoverable credentials.
  - Unauthenticated passkey endpoints use generic responses to avoid account enumeration.
- Reduced token exposure in the browser.
  - OAuth callbacks no longer place access tokens in the URL fragment.
  - Frontend session restoration now happens from the refresh cookie.
- Added proxy-aware IP extraction behind trusted load balancers via `TRUST_PROXY_HEADERS=true`.

## Migration

Apply:

```bash
/Users/saisneha/.local/bin/uv run all-migrations
psql -U postgres -d catalogue -f src/dataio/db/migrations/013_auth_hardening.sql
psql -U postgres -d catalogue -f src/dataio/db/migrations/014_auth_signoff_and_sso.sql
```

Rollback:

```bash
psql -U postgres -d catalogue -f src/dataio/db/migrations/014_auth_signoff_and_sso_rollback.sql
psql -U postgres -d catalogue -f src/dataio/db/migrations/013_auth_hardening_rollback.sql
```

## Recommended next steps

These are still recommended follow-ups after this branch:

1. Add session/device management UI.
   - Show active sessions, last seen IP, user agent, and revoke-by-session.
2. Add suspicious activity notifications.
   - New sign-in, passkey added, API key created, admin suspension, invitation accepted.
3. Add dedicated auth integration tests.
   - Pending/suspended/rejected users.
   - Rate limit behavior.
   - Refresh rotation.
   - OAuth callback and account-linking behavior.
   - Audit log writes.
   - Passkey login edge cases.
4. Add retention and archival rules for auth audit logs and auth rate-limit buckets.
5. Consider stronger step-up protection for destructive actions.
   - Account deletion.
   - API key creation.
   - Admin privilege changes.
6. Consider moving access tokens fully in-memory with a backend session bootstrap endpoint if you want to reduce XSS blast radius even further.

## Environment knobs

- `TRUST_PROXY_HEADERS`
- `AUTH_RATE_LIMIT_WINDOW_SECONDS`
- `AUTH_RATE_LIMIT_BLOCK_SECONDS`
- `OTP_SECRET_KEY`
- `COOKIE_SECURE`
- `COOKIE_SAMESITE`
- `REFRESH_COOKIE_NAME`
- `WEB_API_BASE_URL`
- `GOOGLE_OAUTH_CLIENT_ID`
- `GOOGLE_OAUTH_CLIENT_SECRET`
- `GOOGLE_OAUTH_REDIRECT_URI`
- `GITHUB_OAUTH_CLIENT_ID`
- `GITHUB_OAUTH_CLIENT_SECRET`
- `GITHUB_OAUTH_REDIRECT_URI`

The defaults are intentionally conservative enough for staging, but you should tune them per environment before production rollout.

## Automatic migration runner

Use:

```bash
/Users/saisneha/.local/bin/uv run all-migrations
```

Behavior:

- applies only forward migrations that register themselves with `SELECT add_migration(...)`
- ignores `*_rollback.sql` files
- skips legacy `.sql` files that do not self-register as tracked migrations
- only runs migrations that are not already present in `db_migration_history`
- can ignore older historical gaps via `MIGRATION_MIN_NUMBER`
