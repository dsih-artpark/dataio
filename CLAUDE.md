# DataIO Development Guidelines

## Git Workflow

**IMPORTANT: Claude should NOT commit or push directly.** Instead:
1. Make code changes as requested
2. Provide the commit message to the user
3. Provide the `gh pr create` command if a PR is needed
4. Let the user execute git commands themselves

### Branch Strategy
- Don't work on existing production branches
- Always checkout a branch from staging and work on feature branches that can be merged onto staging
- Feature branch naming: `<name>/<feature-description>` (e.g., `sneha/web-auth-layer`)

### Example Git Commands for User
```bash
# Stage and commit changes
git add -A
git commit -m "your commit message here"

# Push and create PR to staging
git push -u origin <branch-name>
gh pr create --base staging --title "PR Title" --body "Description"
```

## Project Structure

### Backend (Python/FastAPI)
- **Source**: `src/dataio/`
- **API**: `src/dataio/api/` - FastAPI application
- **Database**: PostgreSQL with SQLAlchemy 2.0+
- **Auth**: `src/dataio/api/auth/` - Authentication & authorization
- **Services**: `src/dataio/api/services/` - Business logic layer
- **Migrations**: `src/dataio/db/migrations/` - SQL migration files

### Frontend (Astro + Preact)
- **Source**: `web/` - Astro application with TypeScript
- **Components**: `web/src/components/` - Preact islands for interactivity
- **Pages**: `web/src/pages/` - Astro pages and layouts
- **Lib**: `web/src/lib/` - API client, auth helpers, utilities

## Web UI Feature

### Architecture
- **Frontend**: Astro 4.x with Preact islands, TypeScript
- **Backend**: FastAPI with new `/api/v1/web/` endpoints for session-based auth
- **Deployment**: Separate subdomain (e.g., app.dataio.example.com)
- **Email**: Third-party SMTP (SendGrid/Mailgun) for OTP delivery

### Authentication
- **No passwords** - Only Email OTP and Passkey (WebAuthn)
- **Session**: JWT with 7-day expiry + refresh tokens
- **Passkey**: Prompted after first OTP login, stored in database

### Key Features
1. Email OTP login (magic link style)
2. Passkey (WebAuthn) registration and authentication
3. Dataset dashboard with search/filters and download
4. User account management + API key generation
5. Admin dashboard for user/group/role management

### Database Tables (Migration 007)
- `sessions` - JWT/refresh token tracking
- `otp_tokens` - Email verification codes
- `webauthn_credentials` - Passkey storage
- `webauthn_challenges` - WebAuthn flow state
- `user_api_keys` - Self-service API key management
- Extended `users` table with `email_verified`, `last_login`, `created_at`, `display_name`

## Development Commands

```bash
# Backend
uv sync --group api                        # Install dependencies
uvicorn src.dataio.api.main:app --reload  # Run API server

# Frontend (web/)
cd web
npm install                                # Install dependencies
npm run dev                                # Run Astro dev server (port 3000)
npm run build                              # Build for production

# Database
psql -U postgres -d catalogue -f src/dataio/db/migrations/007_web_auth.sql  # Run migration
psql -U postgres -d catalogue -f src/dataio/db/migrations/007_web_auth_rollback.sql  # Rollback if needed
```

## Environment Variables

### Backend (.env)
```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=password
DB_NAME=catalogue

# AWS S3
AWS_ACCESS_KEY=...
AWS_SECRET_ACCESS_KEY=...
AWS_BUCKET_NAME=...

# JWT Authentication
JWT_SECRET_KEY=<generate-secure-random-key>
JWT_ALGORITHM=HS256
JWT_ACCESS_TOKEN_EXPIRE_MINUTES=15
JWT_REFRESH_TOKEN_EXPIRE_DAYS=7

# Email Configuration
DEBUG_EMAIL=true                           # Set to true for local dev (prints OTPs to console)
EMAIL_PROVIDER=ses                         # "ses" (default) or "smtp"
EMAIL_FROM_ADDRESS=noreply@dataio.artpark.ai
EMAIL_FROM_NAME=DataIO

# AWS SES (when EMAIL_PROVIDER=ses)
AWS_SES_REGION=ap-south-1

# SMTP (when EMAIL_PROVIDER=smtp)
SMTP_HOST=smtp.sendgrid.net
SMTP_PORT=587
SMTP_USER=apikey
SMTP_PASSWORD=<your-api-key>
SMTP_USE_TLS=true

# WebAuthn/Passkeys
WEBAUTHN_RP_ID=localhost                   # Domain for passkeys
WEBAUTHN_RP_NAME=DataIO
WEBAUTHN_ORIGIN=http://localhost:3000      # Frontend URL

# Magic Links (Registration & Invitations)
FRONTEND_URL=http://localhost:3000         # Base URL for magic links in emails
INVITATION_LINK_EXPIRY_HOURS=48            # How long invitation links are valid

# CORS
CORS_ORIGINS=http://localhost:3000,http://localhost:4321
```

### Frontend (web/.env)
```bash
PUBLIC_API_URL=http://localhost:8000/api/v1
PUBLIC_WEBAUTHN_RP_ID=localhost
```

## Code Style
- Python: Follow existing patterns in `src/dataio/api/`
- TypeScript: Strict mode, ESLint + Prettier
- Use service layer pattern for business logic
- All new endpoints require authentication checks

## Testing Notes

### Debug Email Mode
Set `DEBUG_EMAIL=true` in `.env` to print OTP codes to console instead of sending emails.
This allows testing the full auth flow without SMTP configuration.

### Migration Rollback
If the web auth migration needs to be reverted:
```bash
psql -U postgres -d catalogue -f src/dataio/db/migrations/007_web_auth_rollback.sql
```
