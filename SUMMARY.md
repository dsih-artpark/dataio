# DataIO - Dataset Management System

**Version:** 0.4.0b15
**Package:** `dataio-artpark` (PyPI)
**License:** AGPL-3.0
**Homepage:** https://dataio.artpark.ai

A production-grade dataset management platform for ARTPARK enabling centralized catalog, distribution, and access control of datasets via API, SDK, CLI, and web interface.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend (Astro)                        │
│  web/src/pages/     - Astro routes (login, datasets, admin)     │
│  web/src/components/ - Preact islands (forms, tables, panels)   │
│  web/src/lib/        - API client, auth helpers, types          │
└─────────────────────────────────────────────────────────────────┘
                                │ HTTP
┌─────────────────────────────────────────────────────────────────┐
│                     Backend (FastAPI)                           │
│  src/dataio/api/                                                │
│  ├── routers/       - web.py, user.py, admin.py                 │
│  ├── services/      - Business logic (11 service classes)       │
│  ├── auth/          - JWT, OTP, Passkey, permissions            │
│  └── database/      - SQLAlchemy models, enums                  │
└─────────────────────────────────────────────────────────────────┘
                                │
┌──────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PostgreSQL     │    │     AWS S3       │    │  AWS SES/SMTP   │
│   (catalogue)    │    │   (file store)   │    │    (email)      │
└──────────────────┘    └──────────────────┘    └─────────────────┘
```

---

## Tech Stack

| Layer | Stack |
|-------|-------|
| **Backend** | Python 3.12+, FastAPI 0.115+, SQLAlchemy 2.0+, PostgreSQL |
| **Frontend** | Astro 4.x, Preact, TypeScript, Tailwind CSS |
| **Auth** | PyJWT (sessions), webauthn 2.0 (passkeys), bcrypt |
| **Storage** | AWS S3 (boto3, s3fs), xarray, zarr, pandas |
| **Email** | AWS SES or SMTP (SendGrid/Mailgun) |

---

## Database Schema

### Core Tables
| Table | Purpose |
|-------|---------|
| `users` | Accounts (email PK, is_admin, is_group, verification_status, suspended_at) |
| `datasets` | Catalog (ds_id, title, collection_id, data_owner_id, spatial/temporal resolution, access_level) |
| `collections` | Dataset groupings (collection_id, collection_name, category) |
| `data_owners` | Sources (name, contact_person, contact_person_email) |
| `regions` | Geographic hierarchy (region_id, parent_region_id) |
| `tags` | Dataset categorization |

### Access Control
| Table | Purpose |
|-------|---------|
| `user_permissions` | User→Resource permissions (user_email, resource_type, resource_id, permission) |
| `user_groups` | Group membership (group_email, user_email) |
| `resource_groups` | Named permission bundles |
| `resource_group_members` | Resources in a resource group |

### Web Auth (Migration 007+)
| Table | Purpose |
|-------|---------|
| `sessions` | JWT tracking (user_email, refresh_token, expires_at, revoked_at, ip_address) |
| `otp_tokens` | 6-digit codes (email, code, purpose, expires_at, attempts) |
| `webauthn_credentials` | Passkeys (credential_id, public_key, sign_count, device_name) |
| `webauthn_challenges` | Temporary challenge storage |
| `user_api_keys` | Self-service keys (key_hash, key_prefix, name, expires_at, revoked_at) |
| `magic_link_tokens` | Registration/deletion verification |

### Enums
- **AccessLevel:** `NONE`, `VIEW`, `DOWNLOAD`
- **SpatialResolution:** `COUNTRY`, `STATE`, `DISTRICT`, `SUBDISTRICT`, `VILLAGE`, `LAT/LONG`, etc.
- **TemporalResolution:** `YEAR`, `MONTH`, `WEEK`, `DATE`, `HOUR`, `MINUTE`, `SECOND`, `NONE`
- **ResourceType:** `DATASET`, `GROUP`, `BUCKET`, `WEATHER_DATA_API`
- **VersionType:** `PREPROCESSED`, `STANDARDISED`

---

## API Endpoints

### Web Auth (`/api/v1/web/auth/`)
| Method | Endpoint | Auth | Purpose |
|--------|----------|------|---------|
| POST | `/auth/login/initiate` | None | Send OTP to email |
| POST | `/auth/login/verify` | None | Verify OTP, return JWT |
| POST | `/auth/refresh` | None | Refresh access token |
| POST | `/auth/logout` | None | Revoke session |
| POST | `/auth/logout-all` | JWT | Revoke all sessions |
| POST | `/auth/register/initiate` | None | Start registration |
| POST | `/auth/register/verify` | None | Complete registration |
| POST | `/auth/passkey/register/options` | JWT | WebAuthn registration challenge |
| POST | `/auth/passkey/register/verify` | JWT | Store passkey |
| POST | `/auth/passkey/login/options` | None | WebAuthn auth challenge |
| POST | `/auth/passkey/login/verify` | None | Passkey authentication |

### User Profile (`/api/v1/web/`)
| Method | Endpoint | Auth | Purpose |
|--------|----------|------|---------|
| GET | `/me` | JWT | Get profile |
| PUT | `/me` | JWT | Update display_name |
| GET | `/api-keys` | JWT | List API keys |
| POST | `/api-keys` | JWT | Create API key (returns full key once) |
| DELETE | `/api-keys/{id}` | JWT | Revoke API key |
| GET | `/passkeys` | JWT | List passkeys |
| DELETE | `/passkeys/{id}` | JWT | Delete passkey |

### Datasets (`/api/v1/web/`)
| Method | Endpoint | Auth | Purpose |
|--------|----------|------|---------|
| GET | `/public/datasets` | None | Browse public datasets |
| GET | `/public/datasets/{id}` | None | Public dataset detail |
| GET | `/datasets` | JWT | User's accessible datasets |
| GET | `/datasets/{id}` | JWT | Dataset detail |
| GET | `/datasets/{id}/download-urls` | JWT | Presigned S3 URLs |
| GET | `/collections` | JWT | Filter options |
| GET | `/data-owners` | JWT | Filter options |

### Admin (`/api/v1/web/admin/`)
| Method | Endpoint | Auth | Purpose |
|--------|----------|------|---------|
| GET | `/users` | Admin | List users |
| GET | `/users/pending` | Admin | Pending verification |
| POST | `/users` | Admin | Invite user |
| PUT | `/users/{email}` | Admin | Update user |
| POST | `/users/{email}/verify` | Admin | Approve user |
| POST | `/users/{email}/suspend` | Admin | Suspend user |
| DELETE | `/users/{email}` | Admin | Delete user |
| POST | `/users/bulk-invite` | Admin | Bulk invite |
| POST | `/users/{email}/permissions` | Admin | Set dataset permission |
| GET | `/groups` | Admin | List groups |
| POST | `/groups` | Admin | Create group |
| POST | `/groups/{email}/members` | Admin | Add member |
| DELETE | `/groups/{email}/members/{user}` | Admin | Remove member |
| POST | `/groups/{email}/permissions` | Admin | Set group permission |

### Legacy User API (`/api/v1/`)
| Method | Endpoint | Auth | Purpose |
|--------|----------|------|---------|
| GET | `/datasets` | API Key | List datasets |
| GET | `/datasets/{id}/{bucket}/tables` | API Key | Table listing |
| GET | `/shapefiles` | API Key | List shapefiles |
| GET | `/shapefiles/{region_id}` | API Key | Download shapefile (gzip) |
| GET | `/regions/{id}/children` | API Key | Child regions |
| GET | `/weather/datasets` | API Key | Weather dataset metadata |
| POST | `/weather/datasets/{name}/download` | API Key | Download NetCDF |

---

## Services Layer

| Service | Responsibility |
|---------|----------------|
| `WebAuthService` | OTP generation/verification, passkey flows, session management, registration |
| `WebUserService` | Profile CRUD, API key management, dataset access |
| `WebAdminService` | User/group CRUD, permission management, bulk operations |
| `UserService` | Dataset listing, table access, shapefile downloads |
| `WeatherService` | Weather data extraction (zarr/xarray), spatial filtering |
| `EmailService` | OTP delivery via SES or SMTP |
| `FilestoreService` | S3 operations (presigned URLs, file listing) |
| `AdminDatasetService` | Dataset CRUD operations |
| `AdminUserManagementService` | Legacy admin operations |

---

## Frontend Pages

| Page | Route | Purpose |
|------|-------|---------|
| `index.astro` | `/` | Landing/dashboard |
| `login.astro` | `/login` | OTP + passkey login |
| `register.astro` | `/register` | Self-registration |
| `verify-email.astro` | `/verify-email` | Magic link handler |
| `datasets/index.astro` | `/datasets` | Dataset browser |
| `account/index.astro` | `/account` | Profile, API keys, passkeys |
| `admin/index.astro` | `/admin` | Admin dashboard |
| `admin/users.astro` | `/admin/users` | User management |
| `admin/groups.astro` | `/admin/groups` | Group management |

### Key Components (Preact)
- **Auth:** `LoginForm`, `RegisterForm`, `OTPInput`, `PasskeyPrompt`, `VerifyEmailHandler`
- **Datasets:** `DatasetBrowser`, `DatasetTable`, `DatasetDetailPanel`, `FilterPanel`, `CodeSnippets`
- **Account:** `AccountSettings`, `APIKeyManager`, `DeleteAccountModal`
- **Admin:** `UserList`, `PendingUsers`, `GroupManager`, `AdminUsersTabs`
- **Common:** `Header`, `Sidebar`, `UnifiedNav`

---

## Authentication Flow

### Email OTP Login
```
1. POST /auth/login/initiate {email}
   → Send 6-digit OTP (10min expiry, max 5 attempts)
2. POST /auth/login/verify {email, code}
   → Return {access_token (15min), refresh_token (7 days), user, needs_passkey}
```

### Passkey Registration (after first login)
```
1. POST /auth/passkey/register/options
   → Return WebAuthn challenge
2. Browser: navigator.credentials.create()
3. POST /auth/passkey/register/verify {credential, device_name}
   → Store credential
```

### Passkey Login
```
1. POST /auth/passkey/login/options {email}
   → Return WebAuthn challenge with allowed credentials
2. Browser: navigator.credentials.get()
3. POST /auth/passkey/login/verify {email, credential}
   → Return session tokens
```

### API Key Auth (SDK/CLI)
```
Header: X-API-Key: dio_xxxxx
Legacy keys (no prefix) → Deprecation headers added
```

---

## Permission Model

```
User → UserPermission → Resource (DATASET, BUCKET, WEATHER_DATA_API)
         ↓
      permission: NONE | VIEW | DOWNLOAD

User → UserGroup → Group (is_group=true user)
                      ↓
                   Group's UserPermissions

Resolution: Highest permission wins (DOWNLOAD > VIEW > NONE)
Admin users: Bypass all permission checks
```

---

## CLI & SDK

### CLI (`dataio` command)
```bash
pip install dataio-artpark
dataio --help
```

### SDK
```python
from dataio.sdk.user import UserClient
client = UserClient(api_key="dio_xxx", base_url="https://api.dataio.artpark.ai")
datasets = client.get_datasets()
```

---

## Development

### Backend
```bash
uv sync --group api
uvicorn src.dataio.api.main:app --reload  # localhost:8000
```

### Frontend
```bash
cd web
npm install
npm run dev  # localhost:3000
```

### Database
```bash
# Run migrations
psql -U postgres -d catalogue -f src/dataio/db/migrations/001_init.sql
# ... through 010_otp_purpose_constraint.sql
```

### Environment Variables
```bash
# Backend (.env)
DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=xxx DB_NAME=catalogue
JWT_SECRET_KEY=xxx JWT_ACCESS_TOKEN_EXPIRE_MINUTES=15 JWT_REFRESH_TOKEN_EXPIRE_DAYS=7
AWS_ACCESS_KEY=xxx AWS_SECRET_ACCESS_KEY=xxx AWS_BUCKET_NAME=xxx
DEBUG_EMAIL=true  # Prints OTP to console
WEBAUTHN_RP_ID=localhost WEBAUTHN_ORIGIN=http://localhost:3000
CORS_ORIGINS=http://localhost:3000,http://localhost:4321

# Frontend (web/.env)
PUBLIC_API_URL=http://localhost:8000/api/v1
PUBLIC_WEBAUTHN_RP_ID=localhost
```

---

## Key Design Decisions

1. **Passwordless auth only** - No passwords stored; OTP + passkeys only
2. **Service layer pattern** - Routers thin, services handle business logic
3. **Island architecture** - Astro static pages + Preact hydration where needed
4. **Graceful deprecation** - Legacy API keys work with `Deprecation` headers (sunset: 2025-12-31)
5. **Group permissions** - Users inherit permissions from groups they belong to
6. **Self-service registration** - Email verification + admin approval workflow
7. **Async throughout** - All database and S3 operations non-blocking

---

## File Structure

```
dataio/
├── src/dataio/
│   ├── api/
│   │   ├── main.py              # FastAPI app, middleware, CORS
│   │   ├── routers/
│   │   │   ├── web.py           # 50+ web auth/user/admin endpoints
│   │   │   ├── user.py          # Legacy API-key endpoints
│   │   │   └── admin.py         # Legacy admin endpoints
│   │   ├── services/            # 11 service classes
│   │   ├── auth/
│   │   │   ├── jwt.py           # Token creation/validation
│   │   │   ├── otp.py           # OTP generation
│   │   │   ├── passkey.py       # WebAuthn implementation
│   │   │   ├── permissions.py   # Permission resolution
│   │   │   └── providers.py     # Auth dependency injection
│   │   └── database/
│   │       ├── models.py        # 17 SQLAlchemy models
│   │       ├── enums.py         # 6 enum classes
│   │       └── config.py        # DB connection
│   ├── cli/                     # Typer CLI app
│   ├── sdk/                     # Python SDK (user.py, admin.py)
│   └── db/migrations/           # 10 SQL migrations
├── web/
│   ├── src/
│   │   ├── pages/               # 9 Astro pages
│   │   ├── components/          # 24 Preact components
│   │   └── lib/                 # api.ts, auth.ts, types.ts, webauthn.ts
│   └── astro.config.mjs
├── pyproject.toml               # Package config (dataio-artpark)
└── CLAUDE.md                    # Dev guidelines
```
