from __future__ import annotations

import importlib
import os

from fastapi.testclient import TestClient

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")
os.environ.setdefault("FRONTEND_URL", "http://localhost:3000")

app = importlib.import_module("dataio.api").app
WebAuthService = importlib.import_module("dataio.api.routers.web").WebAuthService

client = TestClient(app)


def test_start_oauth_sets_next_cookie():
    class WebAuthServiceStub:
        def get_oauth_authorize_url(self, provider, state):
            assert provider == "google"
            assert state
            return "https://accounts.example.test/oauth"

    app.dependency_overrides[WebAuthService] = lambda: WebAuthServiceStub()

    response = client.get(
        "/api/v1/web/auth/oauth/google/start?next=%2Fadmin%2Fdatasets",
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["location"] == "https://accounts.example.test/oauth"
    cookies = response.headers.get("set-cookie", "")
    assert "oauth_state_google=" in cookies
    assert 'oauth_next_google="/admin/datasets"' in cookies or "oauth_next_google=%2Fadmin%2Fdatasets" in cookies

    app.dependency_overrides.clear()


def test_oauth_callback_redirects_to_next_target_on_success():
    class WebAuthServiceStub:
        def complete_google_oauth(self, code, user_agent=None, ip_address=None):
            assert code == "oauth-code"
            return {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "needs_passkey": False,
            }

    app.dependency_overrides[WebAuthService] = lambda: WebAuthServiceStub()

    client.cookies.set("oauth_state_google", "state-123")
    client.cookies.set("oauth_next_google", "/admin/datasets")

    response = client.get(
        "/api/v1/web/auth/oauth/google/callback?code=oauth-code&state=state-123",
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["location"] == "http://localhost:3000/admin/datasets"

    app.dependency_overrides.clear()
    client.cookies.clear()


def test_oauth_callback_uses_login_flow_when_passkey_setup_needed():
    class WebAuthServiceStub:
        def complete_google_oauth(self, code, user_agent=None, ip_address=None):
            return {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "needs_passkey": True,
            }

    app.dependency_overrides[WebAuthService] = lambda: WebAuthServiceStub()

    client.cookies.set("oauth_state_google", "state-123")
    client.cookies.set("oauth_next_google", "/admin/datasets")

    response = client.get(
        "/api/v1/web/auth/oauth/google/callback?code=oauth-code&state=state-123",
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["location"].startswith("http://localhost:3000/login#")
    assert "needs_passkey=true" in response.headers["location"]
    assert "next=%2Fadmin%2Fdatasets" in response.headers["location"]

    app.dependency_overrides.clear()
    client.cookies.clear()
