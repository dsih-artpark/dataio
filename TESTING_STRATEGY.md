# DataIO Testing Strategy

## Overview

This document outlines the comprehensive end-to-end testing strategy for the DataIO project, covering the API backend, Python SDK, CLI, and web frontend.

## Testing Pyramid

```
                    ╱╲
                   ╱  ╲
                  ╱ E2E ╲        (Smoke tests, integration tests)
                 ╱────────╲
                ╱          ╲
               ╱ Integration ╲   (API endpoints, database, external services)
              ╱──────────────╲
             ╱                ╲
            ╱    Unit Tests    ╲  (Models, utilities, business logic)
           ╱────────────────────╲
```

## Test Categories

### 1. Unit Tests
- Fast, isolated tests for individual functions and classes
- No external dependencies (database, network, file system)
- Run on every commit

### 2. Integration Tests
- Test interactions between components
- May require database or mocked external services
- Run on every PR

### 3. Smoke Tests
- Quick sanity checks that the system is working
- Run before deployments

### 4. End-to-End Tests
- Full system tests simulating user workflows
- Run on staging before production releases

---

## Test Infrastructure

### Python Tests (pytest)

**Location**: `tests/` directory structure:
```
tests/
├── conftest.py              # Shared fixtures
├── unit/                    # Unit tests
│   ├── api/                 # API unit tests
│   │   ├── test_models.py   # Pydantic model validation
│   │   ├── test_auth/       # Auth module tests
│   │   │   ├── test_jwt.py
│   │   │   ├── test_otp.py
│   │   │   └── test_permissions.py
│   │   └── test_services/   # Service layer tests
│   ├── sdk/                 # SDK unit tests
│   │   └── test_sdk_user.py
│   └── cli/                 # CLI unit tests
│       └── test_cli_commands.py
├── integration/             # Integration tests
│   ├── api/                 # API integration tests
│   │   ├── test_user_endpoints.py
│   │   ├── test_admin_endpoints.py
│   │   └── test_web_auth_endpoints.py
│   └── sdk/                 # SDK integration tests
│       └── test_sdk_integration.py
└── smoke/                   # Smoke tests
    └── test_smoke.py        # Quick sanity checks
```

**Configuration**: `pyproject.toml`
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
addopts = "-v --tb=short"
markers = [
    "unit: Unit tests (fast, no dependencies)",
    "integration: Integration tests (may need database)",
    "smoke: Smoke tests (quick sanity checks)",
    "slow: Slow tests (run separately)",
]
```

**Run Commands**:
```bash
# Run all tests
uv run pytest

# Run only unit tests
uv run pytest -m unit

# Run only integration tests
uv run pytest -m integration

# Run smoke tests
uv run pytest -m smoke

# Run with coverage
uv run pytest --cov=src/dataio --cov-report=html
```

### Web Tests (Vitest)

**Location**: `web/` directory structure:
```
web/
├── vitest.config.ts         # Vitest configuration
├── src/
│   ├── lib/
│   │   └── __tests__/       # Lib tests
│   │       ├── api.test.ts
│   │       └── auth.test.ts
│   └── components/
│       └── __tests__/       # Component tests
│           ├── LoginForm.test.tsx
│           └── DatasetBrowser.test.tsx
└── tests/
    └── smoke.test.ts        # Web smoke tests
```

**Run Commands**:
```bash
cd web
npm run test           # Run all tests
npm run test:unit      # Run unit tests only
npm run test:coverage  # Run with coverage
```

---

## Test Coverage Targets

| Component | Unit | Integration | Total Target |
|-----------|------|-------------|--------------|
| API Models | 90%+ | N/A | 90%+ |
| API Auth | 80%+ | 70%+ | 80%+ |
| API Services | 70%+ | 60%+ | 70%+ |
| SDK | 80%+ | 60%+ | 75%+ |
| CLI | 70%+ | 50%+ | 65%+ |
| Web Lib | 80%+ | N/A | 80%+ |
| Web Components | 60%+ | N/A | 60%+ |

---

## CI/CD Integration

### GitHub Actions Workflows

#### 1. `test.yml` - Test Pipeline (runs on every PR)
```yaml
name: Tests
on:
  pull_request:
    branches: [staging, main, prod]
  push:
    branches: [staging]

jobs:
  python-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - run: uv sync --group api --group dev
      - run: uv run pytest -m "unit or smoke" --cov

  web-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
      - run: cd web && npm ci && npm run test
```

#### 2. `integration-tests.yml` - Integration Tests (runs on PRs to main/prod)
```yaml
name: Integration Tests
on:
  pull_request:
    branches: [main, prod]

jobs:
  integration:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: test
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - run: uv sync --group api
      - run: uv run pytest -m integration
```

#### 3. Pre-production Checks
```yaml
name: Pre-Production
on:
  pull_request:
    branches: [prod]

jobs:
  full-test-suite:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - run: uv sync --group api --group dev
      - run: uv run pytest --cov --cov-fail-under=70
```

---

## Test Types by Component

### API Tests

#### Models (`tests/unit/api/test_models.py`)
- Pydantic model validation
- Field constraints (min/max length, enums)
- Optional field handling
- Serialization/deserialization

#### Auth (`tests/unit/api/test_auth/`)
- JWT token creation and verification
- OTP generation and validation
- Permission checking logic
- Email validation

#### Services (`tests/unit/api/test_services/`)
- Business logic in isolation
- Mocked database operations
- Error handling

#### Endpoints (`tests/integration/api/`)
- HTTP status codes
- Response schemas
- Authentication requirements
- Authorization (admin vs user)

### SDK Tests

#### Unit (`tests/unit/sdk/`)
- Client initialization
- Request building
- Response parsing
- Error handling

#### Integration (`tests/integration/sdk/`)
- Real API calls (mocked or test server)
- End-to-end workflows

### CLI Tests

#### Unit (`tests/unit/cli/`)
- Command parsing
- Option handling
- Output formatting

### Web Tests

#### Unit (`web/src/lib/__tests__/`)
- API client functions
- Auth helpers
- Utility functions

#### Components (`web/src/components/__tests__/`)
- Component rendering
- User interactions
- State management

---

## Smoke Test Checklist

Run before every deployment:

### API Smoke Tests
- [ ] Health check endpoint responds
- [ ] Authentication works
- [ ] List datasets returns data
- [ ] Error responses are JSON

### SDK Smoke Tests
- [ ] Package imports successfully
- [ ] Can instantiate client
- [ ] API key validation works

### CLI Smoke Tests
- [ ] Help command works
- [ ] Version command works
- [ ] Init command runs

### Web Smoke Tests
- [ ] Homepage loads
- [ ] Login page renders
- [ ] API client initializes

---

## Test Data Management

### Fixtures
- Use pytest fixtures for reusable test data
- Define in `tests/conftest.py`

### Database
- Use separate test database or SQLite for unit tests
- Use transactions with rollback for integration tests

### Mocking
- Use `pytest-mock` for Python
- Use `vi.mock()` for TypeScript
- Mock external services (S3, email)

---

## Running Tests Locally

### Prerequisites
```bash
# Install dependencies
uv sync --group api --group dev

# For web tests
cd web && npm install
```

### Full Test Suite
```bash
# Python tests
uv run pytest

# Web tests
cd web && npm run test

# All tests with coverage
uv run pytest --cov=src/dataio --cov-report=term-missing
```

### Quick Smoke Tests
```bash
# Just smoke tests (fast)
uv run pytest -m smoke
cd web && npm run test -- --grep "smoke"
```

---

## Contributing Tests

1. **Write tests alongside code**: Every new feature should include tests
2. **Follow naming conventions**: `test_<function_name>_<scenario>`
3. **Use appropriate markers**: `@pytest.mark.unit`, `@pytest.mark.integration`
4. **Keep tests focused**: One assertion per test when possible
5. **Use descriptive names**: Tests should document behavior

---

## Test Maintenance

- Review test coverage weekly
- Remove flaky tests or fix them
- Update tests when requirements change
- Run full test suite before releases
