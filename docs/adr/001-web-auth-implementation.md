# ADR 001: Web Authentication Implementation

**Status:** In Progress
**Date:** 2025-01-20
**Decision Makers:** Sneha S, Akhil B

## Context

DataIO needs a web interface for users to browse datasets, manage their accounts, and perform administrative tasks. The existing system only supports API key authentication via CLI/SDK, which is unsuitable for browser-based access.

## Decision

### Authentication Methods

We chose **Email OTP + Passkeys (WebAuthn)** as the sole authentication methods, explicitly rejecting traditional passwords.

| Method | Decision | Rationale |
|--------|----------|-----------|
| Passwords | **Rejected** | Vulnerable to phishing, credential stuffing, and poor user practices. Password management is a support burden. |
| Email OTP | **Adopted** | Passwordless entry point. Email is already verified during user creation. Simple UX. |
| Passkeys/WebAuthn | **Adopted** | Phishing-resistant, device-bound credentials. Most secure option available. |
| Magic Links | **Considered, Rejected** | Similar to OTP but introduces URL handling complexity and deep-link issues on mobile. |
| OAuth/SSO | **Deferred** | May add later for enterprise customers. Not needed for MVP. |

### Authentication Flow

1. **First-time login**: User enters email → receives 6-digit OTP → verifies → prompted to register passkey
2. **Returning users**: Can use either OTP or passkey to authenticate
3. **Passkey-first**: UI encourages passkey login for returning users (faster, more secure)

### Email Provider

We chose **AWS SES** as the default email provider over SendGrid/Mailgun.

| Option | Decision | Rationale |
|--------|----------|-----------|
| AWS SES | **Default** | Already using AWS infrastructure. Unified billing. No additional vendor. boto3 already a dependency. |
| SendGrid | **Fallback** | Kept as SMTP fallback option via `EMAIL_PROVIDER=smtp` for flexibility. |
| Mailgun | **Fallback** | Same as SendGrid - available via SMTP. |

### Session Management

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| Token Type | JWT | Stateless verification, standard format, easy debugging. |
| Access Token TTL | 15 minutes | Short-lived to limit exposure if leaked. |
| Refresh Token TTL | 7 days | Balance between security and UX (weekly re-auth). |
| Storage | Access: memory, Refresh: httpOnly cookie or localStorage | Access token never persisted, refresh token protected from XSS. |

### Passkey Security Model

Passkeys use public-key cryptography where:
- **Private key**: Stored in device's secure hardware (TPM/Secure Enclave), never leaves device
- **Public key**: Stored in database (`webauthn_credentials.public_key`)
- **Sign count**: Incremented on each auth, prevents replay attacks

**Why database storage is secure:**
- Public keys are safe to expose (by definition)
- Even with full database access, attacker cannot forge authentication
- No password hashes to crack
- Attacker would need physical device + biometric/PIN

### Frontend Technology

| Option | Decision | Rationale |
|--------|----------|-----------|
| Framework | Astro 4.x | Fast, static-first, great for content-heavy dashboard. |
| Interactivity | Preact islands | Lightweight (3KB), React-compatible API, only hydrates where needed. |
| Styling | Tailwind CSS | Utility-first, consistent with rapid development. |
| WebAuthn Client | @simplewebauthn/browser | Well-maintained, TypeScript, handles browser quirks. |

### Deployment Architecture

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| Structure | Monorepo (`web/` folder) | Single repo, coordinated deployments, shared types. |
| Subdomain | Separate (e.g., app.dataio.artpark.ai) | Clear separation from API, independent caching/CDN. |
| CORS | Explicit allow-list | Security - only web subdomain can call API with credentials. |

### Database Schema

New tables added via migration `007_web_auth.sql`:
- `sessions` - JWT refresh token tracking
- `otp_tokens` - OTP codes with rate limiting (max 5 attempts)
- `webauthn_credentials` - Passkey storage
- `webauthn_challenges` - Temporary WebAuthn challenge storage
- `user_api_keys` - Self-service API key management (separate from legacy `users.key`)

### Environment Variables

```bash
# Email (AWS SES default)
EMAIL_PROVIDER=ses          # or "smtp" for SendGrid/Mailgun
EMAIL_FROM_ADDRESS=noreply@dataio.artpark.ai
EMAIL_FROM_NAME=DataIO
AWS_SES_REGION=ap-south-1   # SES region

# SMTP fallback (if EMAIL_PROVIDER=smtp)
SMTP_HOST=smtp.sendgrid.net
SMTP_PORT=587
SMTP_USER=apikey
SMTP_PASSWORD=<sendgrid-api-key>

# JWT
JWT_SECRET_KEY=<random-32-chars>
JWT_ALGORITHM=HS256

# WebAuthn
WEBAUTHN_RP_ID=app.dataio.artpark.ai
WEBAUTHN_RP_NAME=DataIO
WEBAUTHN_ORIGIN=https://app.dataio.artpark.ai

# Development
DEBUG_EMAIL=true            # Print OTPs to console instead of sending
```

## Consequences

### Positive
- No password reset flows to build/maintain
- No password breach liability
- Passkeys are phishing-resistant
- Unified AWS billing for email
- Existing users automatically have web access (same email)

### Negative
- Users without passkey-capable devices must use OTP every time
- Email deliverability issues could lock users out
- 7-day session requires periodic re-auth

### Risks & Mitigations
| Risk | Mitigation |
|------|------------|
| Email delivery failures | Fallback SMTP provider, retry logic, clear error messages |
| WebAuthn browser support | OTP always available as fallback |
| Lost device | OTP recovery path, admin can revoke passkeys |

## Related Documents
- `src/dataio/db/migrations/007_web_auth.sql` - Database schema
- `src/dataio/api/routers/web.py` - API endpoints
- `web/` - Frontend application

---

*This ADR will be frozen once the feature reaches production.*
