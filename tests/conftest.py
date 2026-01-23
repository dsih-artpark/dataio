"""
Shared pytest fixtures for DataIO tests.

This module provides common fixtures used across unit, integration, and smoke tests.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# ============================================================================
# Environment Fixtures
# ============================================================================


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment variables before any tests run."""
    # Set test-specific environment variables
    test_env = {
        "JWT_SECRET_KEY": "test-secret-key-for-testing-only-32chars",
        "JWT_ALGORITHM": "HS256",
        "JWT_ACCESS_TOKEN_EXPIRE_MINUTES": "15",
        "JWT_REFRESH_TOKEN_EXPIRE_DAYS": "7",
        "DEBUG_EMAIL": "true",
        "WEBAUTHN_RP_ID": "localhost",
        "WEBAUTHN_RP_NAME": "DataIO Test",
        "WEBAUTHN_ORIGIN": "http://localhost:3000",
        "CORS_ORIGINS": "http://localhost:3000,http://localhost:4321",
    }

    # Store original values
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    yield

    # Restore original values
    for key, original_value in original_env.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value


# ============================================================================
# API Test Fixtures
# ============================================================================


@pytest.fixture
def mock_db_session():
    """Mock database session for unit tests."""
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = None
    session.query.return_value.filter.return_value.all.return_value = []
    session.add = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    session.close = MagicMock()
    session.refresh = MagicMock()
    session.expunge = MagicMock()
    return session


@pytest.fixture
def sample_user_data():
    """Sample user data for testing."""
    return {
        "email": "test@example.com",
        "is_group": False,
        "is_admin": False,
        "email_verified": True,
        "display_name": "Test User",
        "created_at": datetime.now(timezone.utc),
        "last_login": datetime.now(timezone.utc),
    }


@pytest.fixture
def sample_admin_user_data():
    """Sample admin user data for testing."""
    return {
        "email": "admin@example.com",
        "is_group": False,
        "is_admin": True,
        "email_verified": True,
        "display_name": "Admin User",
        "created_at": datetime.now(timezone.utc),
        "last_login": datetime.now(timezone.utc),
    }


@pytest.fixture
def sample_dataset_data():
    """Sample dataset data for testing."""
    return {
        "ds_id": "GS0001DS0001",
        "title": "Test Dataset",
        "description": "A test dataset for unit testing",
        "collection": {
            "collection_id": "GS0001",
            "collection_name": "Test Collection",
            "category_id": "CAT001",
            "category_name": "Test Category",
        },
        "data_owner": {
            "name": "Test Owner",
            "contact_person": "Test Contact",
            "contact_person_email": "owner@example.com",
        },
        "access_level": "DOWNLOAD",
    }


@pytest.fixture
def sample_jwt_payload():
    """Sample JWT payload for testing."""
    return {
        "sub": "test@example.com",
        "exp": datetime.now(timezone.utc) + timedelta(minutes=15),
        "iat": datetime.now(timezone.utc),
        "type": "access",
    }


# ============================================================================
# SDK Test Fixtures
# ============================================================================


@pytest.fixture
def mock_api_response():
    """Mock API response for SDK tests."""
    def _create_response(status_code=200, json_data=None, headers=None):
        response = MagicMock()
        response.status_code = status_code
        response.json.return_value = json_data or {}
        response.headers = headers or {}
        response.raise_for_status = MagicMock()
        if status_code >= 400:
            from requests.exceptions import HTTPError
            response.raise_for_status.side_effect = HTTPError()
        return response
    return _create_response


@pytest.fixture
def mock_requests_session(mock_api_response):
    """Mock requests session for SDK tests."""
    with patch("requests.Session") as mock_session_class:
        session = MagicMock()
        mock_session_class.return_value = session
        session.headers = {}
        session.request.return_value = mock_api_response()
        yield session


@pytest.fixture
def sdk_env_vars():
    """Set up environment variables for SDK tests."""
    original_vars = {
        "DATAIO_API_BASE_URL": os.environ.get("DATAIO_API_BASE_URL"),
        "DATAIO_API_KEY": os.environ.get("DATAIO_API_KEY"),
        "DATAIO_DATA_DIR": os.environ.get("DATAIO_DATA_DIR"),
    }

    os.environ["DATAIO_API_BASE_URL"] = "https://test-api.example.com/api/v1"
    os.environ["DATAIO_API_KEY"] = "dio_test_api_key_12345"
    os.environ["DATAIO_DATA_DIR"] = "/tmp/dataio_test"

    yield

    for key, value in original_vars.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


# ============================================================================
# CLI Test Fixtures
# ============================================================================


@pytest.fixture
def cli_runner():
    """Typer CLI test runner."""
    from typer.testing import CliRunner
    return CliRunner()


@pytest.fixture
def temp_env_file(tmp_path):
    """Create a temporary .env file for CLI tests."""
    env_file = tmp_path / ".env"
    env_content = """
DATAIO_API_KEY=dio_test_key_12345
DATAIO_API_BASE_URL=https://test-api.example.com/api/v1
DATAIO_DATA_DIR=data
"""
    env_file.write_text(env_content.strip())
    return env_file


# ============================================================================
# HTTP Client Fixtures (for integration tests)
# ============================================================================


@pytest.fixture
def test_client():
    """FastAPI test client for integration tests."""
    # Defer import to avoid issues when API dependencies aren't installed
    try:
        from fastapi.testclient import TestClient
        from dataio.api.main import app

        client = TestClient(app)
        yield client
    except ImportError:
        pytest.skip("FastAPI or API dependencies not installed")


@pytest.fixture
def authenticated_client(test_client, sample_user_data):
    """Authenticated test client with valid JWT token."""
    # This would require setting up auth - for now, skip if not configured
    pytest.skip("Authenticated client requires database setup")


# ============================================================================
# Utility Functions
# ============================================================================


def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line("markers", "unit: Unit tests (fast, no dependencies)")
    config.addinivalue_line("markers", "integration: Integration tests (may need database)")
    config.addinivalue_line("markers", "smoke: Smoke tests (quick sanity checks)")
    config.addinivalue_line("markers", "slow: Slow tests (run separately)")
    config.addinivalue_line("markers", "api: API-related tests")
    config.addinivalue_line("markers", "sdk: SDK-related tests")
    config.addinivalue_line("markers", "cli: CLI-related tests")
    config.addinivalue_line("markers", "web: Web-related tests")


def pytest_collection_modifyitems(config, items):
    """Automatically add markers based on test location."""
    for item in items:
        # Add markers based on path
        if "/unit/" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "/integration/" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        elif "/smoke/" in str(item.fspath):
            item.add_marker(pytest.mark.smoke)

        # Add component markers
        if "/api/" in str(item.fspath):
            item.add_marker(pytest.mark.api)
        elif "/sdk/" in str(item.fspath):
            item.add_marker(pytest.mark.sdk)
        elif "/cli/" in str(item.fspath):
            item.add_marker(pytest.mark.cli)
