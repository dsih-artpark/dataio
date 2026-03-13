"""
Unit tests for web auth security behavior.
"""

import importlib
import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


def _load_modules():
    os.environ.setdefault("JWT_SECRET_KEY", "test-secret")
    security_module = importlib.import_module("dataio.api.auth.security")
    service_module = importlib.import_module("dataio.api.services.web_auth_service")
    otp_module = importlib.import_module("dataio.api.auth.otp")
    return security_module, service_module, otp_module


def _build_session_with_user(user):
    session = MagicMock()
    query = session.query.return_value
    filtered = query.filter.return_value
    filtered.first.return_value = user
    return session


class WebAuthSecurityTests(unittest.TestCase):
    def test_blocked_accounts_use_generic_passkey_message_when_reason_hidden(self):
        security_module, _, _ = _load_modules()
        user = SimpleNamespace(
            is_group=False,
            suspended_at="2026-03-13T00:00:00Z",
            verification_status="verified",
        )

        with self.assertRaises(security_module.HTTPException) as ctx:
            security_module.ensure_user_can_authenticate(user, reveal_reason=False)

        self.assertEqual(ctx.exception.status_code, 403)
        self.assertEqual(
            ctx.exception.detail,
            security_module.GENERIC_PASSKEY_UNAVAILABLE_MESSAGE,
        )

    def test_login_initiate_returns_generic_message_for_unknown_user(self):
        _, service_module, _ = _load_modules()
        service = service_module.WebAuthService()
        session = _build_session_with_user(None)

        with patch.object(service_module, "DBSession", return_value=session), \
             patch.object(service_module, "enforce_rate_limit"), \
             patch.object(service_module, "record_auth_event"), \
             patch.object(service.email_service, "send_otp_email", return_value=True), \
             patch.object(service_module, "create_otp", return_value=("123456", object())) as create_otp:
            response = service.initiate_login("missing@example.com")

        self.assertEqual(response["sent"], True)
        self.assertEqual(
            response["message"],
            service_module.GENERIC_AUTH_INITIATE_MESSAGE,
        )
        create_otp.assert_called_once_with("missing@example.com", purpose="login")

    def test_login_initiate_returns_generic_message_for_blocked_user(self):
        _, service_module, _ = _load_modules()
        service = service_module.WebAuthService()
        blocked_user = SimpleNamespace(
            email="pending@example.com",
            is_group=False,
            suspended_at=None,
            verification_status="pending",
        )
        session = _build_session_with_user(blocked_user)

        with patch.object(service_module, "DBSession", return_value=session), \
             patch.object(service_module, "enforce_rate_limit"), \
             patch.object(service_module, "record_auth_event"), \
             patch.object(service_module, "create_otp") as create_otp:
            response = service.initiate_login("pending@example.com")

        self.assertEqual(response["sent"], True)
        self.assertEqual(
            response["message"],
            service_module.GENERIC_AUTH_INITIATE_MESSAGE,
        )
        create_otp.assert_not_called()

    def test_passkey_options_return_generic_response_for_blocked_user(self):
        _, service_module, _ = _load_modules()
        service = service_module.WebAuthService()
        blocked_user = SimpleNamespace(
            email="suspended@example.com",
            is_group=False,
            suspended_at="2026-03-13T00:00:00Z",
            verification_status="verified",
        )
        session = _build_session_with_user(blocked_user)

        with patch.object(service_module, "DBSession", return_value=session), \
             patch.object(service_module, "enforce_rate_limit"), \
             patch.object(service_module, "record_auth_event"):
            with self.assertRaises(service_module.HTTPException) as ctx:
                service.get_passkey_authentication_options("suspended@example.com")

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(
            ctx.exception.detail,
            "Passkey sign-in is not available for this account",
        )

    def test_otp_codes_are_hashed_before_storage(self):
        _, _, otp_module = _load_modules()
        hashed = otp_module.hash_otp_code("user@example.com", "login", "123456")
        self.assertTrue(hashed.startswith(otp_module.OTP_HASH_PREFIX))
        self.assertNotEqual(hashed, "123456")

    def test_verify_login_returns_pending_without_tokens_for_pending_user(self):
        _, service_module, _ = _load_modules()
        service = service_module.WebAuthService()
        pending_user = SimpleNamespace(
            email="pending@example.com",
            display_name=None,
            is_admin=False,
            is_group=False,
            email_verified=True,
            verification_status="pending",
            last_login=None,
            suspended_at=None,
        )
        session = _build_session_with_user(None)

        with patch.object(service_module, "DBSession", return_value=session), \
             patch.object(service_module, "enforce_rate_limit"), \
             patch.object(service_module, "verify_otp", return_value=True), \
             patch.object(service_module, "record_auth_event"), \
             patch.object(service, "_build_pending_response", return_value={"verification_status": "pending", "user": {"email": pending_user.email}}), \
             patch.object(service, "_get_or_create_user", return_value=(pending_user, "pending", "Pending review", True)):
            response = service.verify_login("pending@example.com", "123456")

        self.assertEqual(response["verification_status"], "pending")
        self.assertNotIn("access_token", response)

    def test_github_oauth_requires_verified_email_match(self):
        _, service_module, _ = _load_modules()
        service = service_module.WebAuthService()

        token_response = MagicMock(status_code=200)
        token_response.json.return_value = {"access_token": "github-token"}
        user_response = MagicMock(status_code=200)
        user_response.json.return_value = {"id": 12345, "email": "public@example.com"}
        emails_response = MagicMock(status_code=200)
        emails_response.json.return_value = [
            {"email": "public@example.com", "verified": False, "primary": True}
        ]

        with patch.object(service_module, "GITHUB_OAUTH_CLIENT_ID", "client-id"), \
             patch.object(service_module, "GITHUB_OAUTH_CLIENT_SECRET", "client-secret"), \
             patch.object(service_module.requests, "post", return_value=token_response), \
             patch.object(service_module.requests, "get", side_effect=[user_response, emails_response]):
            with self.assertRaises(service_module.HTTPException) as ctx:
                service.complete_github_oauth("oauth-code")

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(
            ctx.exception.detail,
            "GitHub account must expose at least one verified email",
        )


if __name__ == "__main__":
    unittest.main()
