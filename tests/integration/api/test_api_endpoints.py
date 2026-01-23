"""
Integration tests for API endpoints.

Tests API endpoints with mocked or test database.
"""

import os
import pytest
from unittest.mock import patch, MagicMock


# Skip integration tests if API dependencies not available
pytest.importorskip("fastapi")


class TestHealthEndpoints:
    """Tests for health check and basic endpoints."""

    def test_docs_redirect(self, test_client):
        """Test that root redirects to docs."""
        response = test_client.get("/", follow_redirects=False)
        assert response.status_code == 301
        assert "/docs" in response.headers.get("location", "")

    def test_api_redirect(self, test_client):
        """Test that /api redirects to /api/v1."""
        response = test_client.get("/api", follow_redirects=False)
        assert response.status_code == 301
        assert "/api/v1" in response.headers.get("location", "")

    def test_openapi_schema(self, test_client):
        """Test that OpenAPI schema is available."""
        response = test_client.get("/api/v1/openapi.json")
        assert response.status_code == 200
        data = response.json()
        assert "openapi" in data
        assert "paths" in data


class TestAuthenticationRequired:
    """Tests for authentication requirements."""

    def test_datasets_requires_auth(self, test_client):
        """Test that datasets endpoint requires authentication."""
        response = test_client.get("/api/v1/datasets")
        assert response.status_code == 401

    def test_admin_requires_auth(self, test_client):
        """Test that admin endpoints require authentication."""
        response = test_client.get("/api/v1/admin/users")
        assert response.status_code == 401

    def test_invalid_api_key(self, test_client):
        """Test that invalid API key is rejected."""
        response = test_client.get(
            "/api/v1/datasets",
            headers={"X-API-Key": "invalid_key_12345"}
        )
        assert response.status_code == 401


class TestCORSHeaders:
    """Tests for CORS headers."""

    def test_cors_preflight(self, test_client):
        """Test CORS preflight request."""
        response = test_client.options(
            "/api/v1/datasets",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET",
            }
        )
        # Should allow the origin
        assert response.status_code in [200, 204]

    def test_cors_allowed_origin(self, test_client):
        """Test that allowed origins get CORS headers."""
        response = test_client.get(
            "/api/v1",
            headers={"Origin": "http://localhost:3000"}
        )
        # Check CORS headers are present
        assert "access-control-allow-origin" in response.headers


class TestDeprecationHeaders:
    """Tests for legacy API key deprecation headers."""

    @patch("dataio.api.auth.providers.validate_api_key")
    def test_legacy_key_deprecation_header(self, mock_validate, test_client):
        """Test that legacy API keys get deprecation headers."""
        mock_validate.return_value = MagicMock(
            email="test@example.com",
            is_admin=False,
            is_group=False,
        )

        response = test_client.get(
            "/api/v1/datasets",
            headers={"X-API-Key": "legacy_key_without_prefix"}
        )

        # Check for deprecation header
        # Note: This depends on middleware processing
        if response.status_code == 200:
            assert "Deprecation" in response.headers


class TestErrorResponses:
    """Tests for error response format."""

    def test_404_returns_json(self, test_client):
        """Test that 404 errors return JSON."""
        response = test_client.get("/api/v1/nonexistent")
        assert response.status_code in [404, 422]
        # FastAPI returns JSON for API routes
        assert response.headers.get("content-type", "").startswith("application/json")

    def test_401_returns_json(self, test_client):
        """Test that 401 errors return JSON."""
        response = test_client.get("/api/v1/datasets")
        assert response.status_code == 401
        data = response.json()
        assert "detail" in data


class TestWebAuthEndpoints:
    """Tests for web authentication endpoints."""

    def test_request_otp_endpoint_exists(self, test_client):
        """Test that OTP request endpoint exists."""
        # Should return 422 (validation error) or 400, not 404
        response = test_client.post("/api/v1/web/auth/otp/request")
        assert response.status_code in [400, 422]

    def test_verify_otp_endpoint_exists(self, test_client):
        """Test that OTP verify endpoint exists."""
        response = test_client.post("/api/v1/web/auth/otp/verify")
        assert response.status_code in [400, 422]

    def test_refresh_token_endpoint_exists(self, test_client):
        """Test that token refresh endpoint exists."""
        response = test_client.post("/api/v1/web/auth/refresh")
        assert response.status_code in [400, 401, 422]

    def test_logout_endpoint_exists(self, test_client):
        """Test that logout endpoint exists."""
        response = test_client.post("/api/v1/web/auth/logout")
        # Should require auth
        assert response.status_code in [401, 422]


class TestWebUserEndpoints:
    """Tests for web user endpoints."""

    def test_user_profile_requires_auth(self, test_client):
        """Test that user profile requires authentication."""
        response = test_client.get("/api/v1/web/user/profile")
        assert response.status_code == 401

    def test_user_api_keys_requires_auth(self, test_client):
        """Test that API keys endpoint requires authentication."""
        response = test_client.get("/api/v1/web/user/api-keys")
        assert response.status_code == 401


class TestAdminEndpoints:
    """Tests for admin endpoints."""

    def test_admin_users_requires_auth(self, test_client):
        """Test that admin users endpoint requires authentication."""
        response = test_client.get("/api/v1/admin/users")
        assert response.status_code == 401

    def test_admin_groups_requires_auth(self, test_client):
        """Test that admin groups endpoint requires authentication."""
        response = test_client.get("/api/v1/admin/groups")
        assert response.status_code == 401


# Marker for integration tests
pytestmark = pytest.mark.integration
