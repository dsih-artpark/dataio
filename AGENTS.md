# DataIO Development Guidelines

## Local Agent Notes

- Use `/Users/saisneha/.local/bin/uv` for Python environment and test commands in this repo.
- Do not use `pip`, `python -m pytest`, or other package/test runners when `uv` is needed.

## Git Workflow

**IMPORTANT: Codex should NOT commit or push directly.** Instead:
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
/Users/saisneha/.local/bin/uv sync --group api   # Install dependencies
uvicorn src.dataio.api.main:app --reload         # Run API server

# Frontend (web/)
cd web
npm install
npm run dev
npm run build

# Database
/Users/saisneha/.local/bin/uv run all-migrations   # Apply only new forward migrations
psql -U postgres -d catalogue -f src/dataio/db/migrations/007_web_auth.sql
psql -U postgres -d catalogue -f src/dataio/db/migrations/007_web_auth_rollback.sql
```

`all-migrations` respects `MIGRATION_MIN_NUMBER` if set, which is useful for environments with known historical gaps.

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
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_BUCKET_NAME=...

# JWT Authentication
JWT_SECRET_KEY=<generate-secure-random-key>
JWT_ALGORITHM=HS256
JWT_ACCESS_TOKEN_EXPIRE_MINUTES=15
JWT_REFRESH_TOKEN_EXPIRE_DAYS=7

# Email Configuration
DEBUG_EMAIL=true
EMAIL_PROVIDER=ses
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
WEBAUTHN_RP_ID=localhost
WEBAUTHN_RP_NAME=DataIO
WEBAUTHN_ORIGIN=http://localhost:3000

# Magic Links (Registration & Invitations)
FRONTEND_URL=http://localhost:3000
INVITATION_LINK_EXPIRY_HOURS=48

# AI Chat Assistant
CHAT_PROVIDER=bedrock
CHAT_MAX_TOOL_ITERATIONS=10

# AWS Bedrock (when CHAT_PROVIDER=bedrock)
AWS_BEDROCK_REGION=us-east-1
BEDROCK_MODEL_ID=anthropic.claude-3-5-sonnet-20241022-v2:0

# OpenRouter (when CHAT_PROVIDER=openrouter)
OPENROUTER_API_KEY=<your-api-key>
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
OPENROUTER_MODEL_ID=anthropic/claude-3.5-sonnet

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

## AI Chat Assistant (Multi-Provider)

### Architecture
The platform includes an AI-powered chat assistant that helps users discover and explore datasets.
Supports multiple AI providers: AWS Bedrock (default) and OpenRouter.

```
Frontend (DataChat component)
    │
    ▼ SSE Stream (+ optional provider param)
FastAPI Backend (/api/v1/web/chat/stream)
    │
    ▼
Chat Service (Orchestrator)
    │
    ├──▶ AI Provider (Bedrock or OpenRouter)
    │       ▲
    │       │ Tool calls
    │       ▼
    └──▶ MCP Server (DataIO tools)
            │
            ▼
        Database (datasets, permissions)
```

### AI Providers
The chat service supports multiple AI providers via an abstraction layer:

- **AWS Bedrock** (default): Uses boto3 with the Converse API
  - Requires AWS credentials with Bedrock access
  - Model: `anthropic.claude-3-5-sonnet-20241022-v2:0` (configurable)

- **OpenRouter**: OpenAI-compatible API supporting 100+ models
  - Requires API key from https://openrouter.ai/keys
  - Model: `anthropic/claude-3.5-sonnet` (configurable)
  - Supports models from OpenAI, Anthropic, Google, Meta, Mistral, etc.

### Components
- **MCP Server** (`src/dataio/mcp/`): Exposes DataIO capabilities as MCP tools
  - `search_datasets`: Search by query, category, tags
  - `get_dataset_details`: Get full dataset information
  - `list_categories`: List available categories
  - `list_data_owners`: List data providers
  - `get_download_info`: Get download instructions
  - `get_dataset_schema`: Get data dictionary

- **Chat Service** (`src/dataio/api/services/chat_service.py`): Orchestrates AI + MCP
  - Provider abstraction layer (`BedrockProvider`, `OpenRouterProvider`)
  - Handles the agentic loop (message → tools → response)
  - Streams responses via SSE
  - Respects user permissions for dataset access

- **Frontend** (`web/src/components/chat/`): Interactive chat UI
  - `DataChat.tsx`: Main chat component with streaming and provider selector
  - `ChatMessage.tsx`: Message display with markdown
  - `ToolIndicator.tsx`: Shows tool execution status

### Database Tables (Migration 012)
- `chat_sessions`: User chat session tracking
- `chat_messages`: Message history with tool calls

### Running the Chat Feature
1. Configure your chosen provider:
   - **Bedrock**: Ensure AWS credentials are configured with Bedrock access
   - **OpenRouter**: Set `OPENROUTER_API_KEY` environment variable
2. Set `CHAT_PROVIDER` to "bedrock" or "openrouter" (or omit for default)
3. Run migration: `psql -U postgres -d catalogue -f src/dataio/db/migrations/012_chat_sessions.sql`
4. Access via `/chat` in the web UI

### Frontend Provider Selection
The `DataChat` component accepts optional props for provider configuration:
```tsx
// Use server default
<DataChat />

// Force specific provider
<DataChat provider="openrouter" />

// Allow user to select provider
<DataChat showProviderSelector={true} />
```
