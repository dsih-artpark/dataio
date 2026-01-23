"""
Unit tests for OTP (One-Time Password) utilities.

Tests OTP generation, validation, and expiry.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock


class TestOTPGeneration:
    """Tests for OTP generation."""

    def test_generate_otp_default_length(self):
        """Test that generated OTP has default length of 6."""
        from dataio.api.auth.otp import generate_otp_code

        otp = generate_otp_code()
        assert len(otp) == 6

    def test_generate_otp_is_numeric(self):
        """Test that generated OTP is numeric."""
        from dataio.api.auth.otp import generate_otp_code

        otp = generate_otp_code()
        assert otp.isdigit()

    def test_generate_otp_randomness(self):
        """Test that OTPs are random (different values)."""
        from dataio.api.auth.otp import generate_otp_code

        otps = [generate_otp_code() for _ in range(10)]
        # With 6 digits, the chance of collision in 10 tries is very low
        # Check that at least some are different
        assert len(set(otps)) > 1

    def test_generate_otp_custom_length(self):
        """Test OTP generation with custom length."""
        from dataio.api.auth.otp import generate_otp_code

        otp = generate_otp_code(length=8)
        assert len(otp) == 8
        assert otp.isdigit()

    def test_generate_otp_single_digit(self):
        """Test OTP generation with single digit length."""
        from dataio.api.auth.otp import generate_otp_code

        otp = generate_otp_code(length=1)
        assert len(otp) == 1
        assert otp.isdigit()


class TestOTPCreation:
    """Tests for creating OTP tokens in database."""

    @patch("dataio.api.auth.otp.DBSession")
    def test_create_otp_success(self, mock_db_session_class):
        """Test creating an OTP token."""
        from dataio.api.auth.otp import create_otp

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session

        # No recent OTP (rate limit passed)
        mock_session.query.return_value.filter.return_value.first.return_value = None
        mock_session.query.return_value.filter.return_value.update.return_value = 0

        code, otp_token = create_otp(
            email="user@example.com",
            purpose="login",
        )

        assert len(code) == 6
        assert code.isdigit()
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @patch("dataio.api.auth.otp.DBSession")
    def test_create_otp_rate_limited(self, mock_db_session_class):
        """Test that rate limiting prevents OTP creation."""
        from dataio.api.auth.otp import create_otp

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session

        # Create a recent OTP to trigger rate limit
        mock_recent_otp = MagicMock()
        mock_recent_otp.created_at = datetime.now(timezone.utc)
        mock_session.query.return_value.filter.return_value.first.return_value = mock_recent_otp

        with pytest.raises(ValueError) as exc_info:
            create_otp(email="user@example.com", purpose="login")

        assert "wait" in str(exc_info.value).lower()

    @patch("dataio.api.auth.otp.DBSession")
    def test_create_otp_invalidates_existing(self, mock_db_session_class):
        """Test that creating OTP invalidates existing unused OTPs."""
        from dataio.api.auth.otp import create_otp

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session
        mock_session.query.return_value.filter.return_value.first.return_value = None

        create_otp(email="user@example.com", purpose="login")

        # Should have called update to invalidate existing
        assert mock_session.query.return_value.filter.return_value.update.called

    @patch("dataio.api.auth.otp.DBSession")
    def test_create_otp_with_custom_expiry(self, mock_db_session_class):
        """Test creating OTP with custom expiry time."""
        from dataio.api.auth.otp import create_otp

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session
        mock_session.query.return_value.filter.return_value.first.return_value = None

        code, _ = create_otp(
            email="user@example.com",
            purpose="verify_email",
            expires_minutes=30,
        )

        assert len(code) == 6


class TestOTPVerification:
    """Tests for OTP verification."""

    @patch("dataio.api.auth.otp.DBSession")
    def test_verify_valid_otp(self, mock_db_session_class):
        """Test verifying a valid OTP."""
        from dataio.api.auth.otp import verify_otp

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session

        # Create a mock OTP token that's valid
        mock_otp = MagicMock()
        mock_otp.code = "123456"
        mock_otp.expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
        mock_otp.used_at = None
        mock_otp.attempts = 0
        mock_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_otp

        result = verify_otp(
            email="user@example.com",
            code="123456",
            purpose="login",
        )

        assert result is True
        assert mock_otp.used_at is not None
        mock_session.commit.assert_called()

    @patch("dataio.api.auth.otp.DBSession")
    def test_verify_otp_not_found(self, mock_db_session_class):
        """Test verifying when no OTP exists."""
        from dataio.api.auth.otp import verify_otp

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

        result = verify_otp(
            email="user@example.com",
            code="123456",
            purpose="login",
        )

        assert result is False

    @patch("dataio.api.auth.otp.DBSession")
    def test_verify_otp_wrong_code(self, mock_db_session_class):
        """Test verifying with wrong code."""
        from dataio.api.auth.otp import verify_otp

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session

        mock_otp = MagicMock()
        mock_otp.code = "123456"
        mock_otp.expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
        mock_otp.used_at = None
        mock_otp.attempts = 0
        mock_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_otp

        result = verify_otp(
            email="user@example.com",
            code="000000",  # Wrong code
            purpose="login",
        )

        assert result is False
        # Attempts should be incremented
        assert mock_otp.attempts == 1

    @patch("dataio.api.auth.otp.DBSession")
    def test_verify_otp_max_attempts_exceeded(self, mock_db_session_class):
        """Test that max attempts are enforced."""
        from dataio.api.auth.otp import verify_otp

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session

        mock_otp = MagicMock()
        mock_otp.code = "123456"
        mock_otp.expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
        mock_otp.used_at = None
        mock_otp.attempts = 5  # Already at max
        mock_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_otp

        result = verify_otp(
            email="user@example.com",
            code="123456",
            purpose="login",
        )

        assert result is False
        # OTP should be marked as used
        assert mock_otp.used_at is not None


class TestRemainingAttempts:
    """Tests for checking remaining OTP attempts."""

    @patch("dataio.api.auth.otp.DBSession")
    def test_get_remaining_attempts_with_active_otp(self, mock_db_session_class):
        """Test getting remaining attempts when OTP exists."""
        from dataio.api.auth.otp import get_remaining_attempts

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session

        mock_otp = MagicMock()
        mock_otp.attempts = 2
        mock_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_otp

        result = get_remaining_attempts("user@example.com")

        assert result == 3  # 5 (max) - 2 (attempts) = 3

    @patch("dataio.api.auth.otp.DBSession")
    def test_get_remaining_attempts_no_otp(self, mock_db_session_class):
        """Test getting remaining attempts when no OTP exists."""
        from dataio.api.auth.otp import get_remaining_attempts

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

        result = get_remaining_attempts("user@example.com")

        assert result is None


class TestOTPCleanup:
    """Tests for OTP cleanup functions."""

    @patch("dataio.api.auth.otp.DBSession")
    def test_cleanup_expired_otps(self, mock_db_session_class):
        """Test cleaning up expired OTP tokens."""
        from dataio.api.auth.otp import cleanup_expired_otps

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session
        mock_session.query.return_value.filter.return_value.delete.return_value = 5

        result = cleanup_expired_otps()

        assert result == 5
        mock_session.commit.assert_called_once()

    @patch("dataio.api.auth.otp.DBSession")
    def test_invalidate_all_otps(self, mock_db_session_class):
        """Test invalidating all OTPs for an email."""
        from dataio.api.auth.otp import invalidate_all_otps

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session
        mock_session.query.return_value.filter.return_value.update.return_value = 3

        result = invalidate_all_otps("user@example.com")

        assert result == 3
        mock_session.commit.assert_called_once()

    @patch("dataio.api.auth.otp.DBSession")
    def test_invalidate_otps_by_purpose(self, mock_db_session_class):
        """Test invalidating OTPs for specific purpose."""
        from dataio.api.auth.otp import invalidate_all_otps

        mock_session = MagicMock()
        mock_db_session_class.return_value = mock_session
        mock_session.query.return_value.filter.return_value.filter.return_value.update.return_value = 1

        result = invalidate_all_otps("user@example.com", purpose="login")

        assert result == 1
