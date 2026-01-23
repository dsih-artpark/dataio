"""
Unit tests for JWT authentication utilities.

Tests token creation, verification, and session management.
"""

import os
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock


# Set JWT_SECRET_KEY before importing jwt module
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-for-testing-only-32chars")


class TestTokenCreation:
    """Tests for JWT token creation."""

    def test_create_access_token(self):
        """Test creating an access token."""
        from dataio.api.auth.jwt import create_access_token

        token = create_access_token("user@example.com")
        assert isinstance(token, str)
        assert len(token) > 0

    def test_create_access_token_with_custom_expiry(self):
        """Test creating an access token with custom expiry."""
        from dataio.api.auth.jwt import create_access_token

        expires = timedelta(minutes=30)
        token = create_access_token("user@example.com", expires_delta=expires)
        assert isinstance(token, str)

    def test_create_refresh_token(self):
        """Test creating a refresh token."""
        from dataio.api.auth.jwt import create_refresh_token

        token, jti, expires_at = create_refresh_token("user@example.com")
        assert isinstance(token, str)
        assert isinstance(jti, str)
        assert isinstance(expires_at, datetime)
        assert expires_at > datetime.now(timezone.utc)

    def test_create_refresh_token_with_custom_expiry(self):
        """Test creating a refresh token with custom expiry."""
        from dataio.api.auth.jwt import create_refresh_token

        expires = timedelta(days=14)
        token, jti, expires_at = create_refresh_token(
            "user@example.com", expires_delta=expires
        )
        expected_expiry = datetime.now(timezone.utc) + expires
        # Allow 1 second tolerance
        assert abs((expires_at - expected_expiry).total_seconds()) < 1


class TestTokenVerification:
    """Tests for JWT token verification."""

    def test_verify_valid_access_token(self):
        """Test verifying a valid access token."""
        from dataio.api.auth.jwt import create_access_token, verify_token

        token = create_access_token("user@example.com")
        payload = verify_token(token)
        assert payload.sub == "user@example.com"
        assert payload.type == "access"

    def test_verify_valid_refresh_token(self):
        """Test verifying a valid refresh token."""
        from dataio.api.auth.jwt import create_refresh_token, verify_token

        token, jti, _ = create_refresh_token("user@example.com")
        payload = verify_token(token)
        assert payload.sub == "user@example.com"
        assert payload.type == "refresh"
        assert payload.jti == jti

    def test_verify_expired_token(self):
        """Test that expired tokens raise AuthenticationError."""
        from dataio.api.auth.jwt import create_access_token, verify_token
        from dataio.api.auth.exceptions import AuthenticationError

        # Create token that's already expired
        expires = timedelta(seconds=-1)
        token = create_access_token("user@example.com", expires_delta=expires)

        with pytest.raises(AuthenticationError) as exc_info:
            verify_token(token)
        assert "expired" in str(exc_info.value).lower()

    def test_verify_invalid_token(self):
        """Test that invalid tokens raise AuthenticationError."""
        from dataio.api.auth.jwt import verify_token
        from dataio.api.auth.exceptions import AuthenticationError

        with pytest.raises(AuthenticationError) as exc_info:
            verify_token("invalid.token.here")
        assert "invalid" in str(exc_info.value).lower()

    def test_verify_tampered_token(self):
        """Test that tampered tokens raise AuthenticationError."""
        from dataio.api.auth.jwt import create_access_token, verify_token
        from dataio.api.auth.exceptions import AuthenticationError

        token = create_access_token("user@example.com")
        # Tamper with the token
        tampered = token[:-5] + "XXXXX"

        with pytest.raises(AuthenticationError):
            verify_token(tampered)


class TestTokenPayload:
    """Tests for TokenPayload class."""

    def test_token_payload_attributes(self):
        """Test TokenPayload has correct attributes."""
        from dataio.api.auth.jwt import TokenPayload

        exp = datetime.now(timezone.utc) + timedelta(minutes=15)
        payload = TokenPayload(
            sub="user@example.com",
            exp=exp,
            type="access",
            jti="unique-id-123",
        )
        assert payload.sub == "user@example.com"
        assert payload.exp == exp
        assert payload.type == "access"
        assert payload.jti == "unique-id-123"

    def test_token_payload_optional_jti(self):
        """Test TokenPayload with optional jti."""
        from dataio.api.auth.jwt import TokenPayload

        exp = datetime.now(timezone.utc) + timedelta(minutes=15)
        payload = TokenPayload(sub="user@example.com", exp=exp, type="access")
        assert payload.jti is None


class TestSessionManagement:
    """Tests for session management functions."""

    @patch("dataio.api.auth.jwt.DBSession")
    def test_create_session(self, mock_db_session_class):
        """Test creating a session in the database."""
        from dataio.api.auth.jwt import create_session

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session

        expires_at = datetime.now(timezone.utc) + timedelta(days=7)
        result = create_session(
            user_email="user@example.com",
            refresh_token="token123",
            expires_at=expires_at,
            user_agent="Mozilla/5.0",
            ip_address="127.0.0.1",
        )

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    @patch("dataio.api.auth.jwt.DBSession")
    def test_revoke_session(self, mock_db_session_class):
        """Test revoking a session."""
        from dataio.api.auth.jwt import revoke_session

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session

        mock_db_session_obj = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = (
            mock_db_session_obj
        )

        result = revoke_session("token123")

        assert result is True
        assert mock_db_session_obj.revoked_at is not None
        mock_session.commit.assert_called_once()

    @patch("dataio.api.auth.jwt.DBSession")
    def test_revoke_nonexistent_session(self, mock_db_session_class):
        """Test revoking a session that doesn't exist."""
        from dataio.api.auth.jwt import revoke_session

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session
        mock_session.query.return_value.filter.return_value.first.return_value = None

        result = revoke_session("nonexistent_token")

        assert result is False

    @patch("dataio.api.auth.jwt.DBSession")
    def test_revoke_all_user_sessions(self, mock_db_session_class):
        """Test revoking all sessions for a user."""
        from dataio.api.auth.jwt import revoke_all_user_sessions

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session
        mock_session.query.return_value.filter.return_value.update.return_value = 3

        result = revoke_all_user_sessions("user@example.com")

        assert result == 3
        mock_session.commit.assert_called_once()

    @patch("dataio.api.auth.jwt.DBSession")
    @patch("dataio.api.auth.jwt.verify_token")
    def test_validate_refresh_token_valid(self, mock_verify, mock_db_session_class):
        """Test validating a valid refresh token."""
        from dataio.api.auth.jwt import validate_refresh_token, TokenPayload

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session

        mock_payload = TokenPayload(
            sub="user@example.com",
            exp=datetime.now(timezone.utc) + timedelta(days=7),
            type="refresh",
            jti="unique-id",
        )
        mock_verify.return_value = mock_payload

        mock_db_session_obj = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = (
            mock_db_session_obj
        )

        result = validate_refresh_token("valid_token")

        assert result is mock_db_session_obj

    @patch("dataio.api.auth.jwt.verify_token")
    def test_validate_refresh_token_wrong_type(self, mock_verify):
        """Test validating a token that's not a refresh token."""
        from dataio.api.auth.jwt import validate_refresh_token, TokenPayload

        mock_payload = TokenPayload(
            sub="user@example.com",
            exp=datetime.now(timezone.utc) + timedelta(minutes=15),
            type="access",  # Wrong type
        )
        mock_verify.return_value = mock_payload

        result = validate_refresh_token("access_token")

        assert result is None


class TestGetCurrentWebUser:
    """Tests for get_current_web_user dependency."""

    def test_missing_credentials(self):
        """Test that missing credentials raises HTTPException."""
        from dataio.api.auth.jwt import get_current_web_user
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            get_current_web_user(credentials=None)
        assert exc_info.value.status_code == 401
        assert "Missing" in str(exc_info.value.detail)

    @patch("dataio.api.auth.jwt.verify_token")
    @patch("dataio.api.auth.jwt.DBSession")
    def test_user_not_found(self, mock_db_session_class, mock_verify):
        """Test that non-existent user raises HTTPException."""
        from dataio.api.auth.jwt import get_current_web_user, TokenPayload
        from fastapi import HTTPException
        from fastapi.security import HTTPAuthorizationCredentials

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session
        mock_session.query.return_value.filter.return_value.first.return_value = None

        mock_payload = TokenPayload(
            sub="nonexistent@example.com",
            exp=datetime.now(timezone.utc) + timedelta(minutes=15),
            type="access",
        )
        mock_verify.return_value = mock_payload

        credentials = HTTPAuthorizationCredentials(
            scheme="Bearer", credentials="valid_token"
        )

        with pytest.raises(HTTPException) as exc_info:
            get_current_web_user(credentials)
        assert exc_info.value.status_code == 401


class TestGetOptionalWebUser:
    """Tests for get_optional_web_user dependency."""

    def test_no_credentials_returns_none(self):
        """Test that no credentials returns None instead of raising."""
        from dataio.api.auth.jwt import get_optional_web_user

        result = get_optional_web_user(credentials=None)
        assert result is None

    @patch("dataio.api.auth.jwt.get_current_web_user")
    def test_invalid_credentials_returns_none(self, mock_get_current):
        """Test that invalid credentials returns None."""
        from dataio.api.auth.jwt import get_optional_web_user
        from fastapi import HTTPException
        from fastapi.security import HTTPAuthorizationCredentials

        mock_get_current.side_effect = HTTPException(status_code=401)

        credentials = HTTPAuthorizationCredentials(
            scheme="Bearer", credentials="invalid"
        )
        result = get_optional_web_user(credentials)

        assert result is None
