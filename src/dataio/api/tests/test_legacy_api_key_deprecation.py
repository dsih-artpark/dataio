from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from dataio.api.auth.providers import get_user
from dataio.api.main import LegacyAPIKeyDeprecationMiddleware


class DummyUser:
    def __init__(self, legacy: bool):
        self.email = "tester@example.com"
        self.is_group = False
        self._legacy_key_used = legacy


def test_get_user_marks_request_state_for_authenticated_legacy_key(monkeypatch):
    scope = {"type": "http", "headers": [], "state": {}}
    request = Request(scope)

    monkeypatch.setattr(
        "dataio.api.auth.providers.check_api_key",
        lambda api_key: DummyUser(legacy=True),
    )

    user = get_user(request, api_key_header="legacy-key")

    assert user.email == "tester@example.com"
    assert request.state.legacy_api_key_authenticated is True


def test_middleware_adds_deprecation_headers_only_for_authenticated_legacy_keys():
    app = FastAPI()
    app.add_middleware(LegacyAPIKeyDeprecationMiddleware)

    def legacy_auth(request: Request):
        request.state.legacy_api_key_authenticated = True
        return DummyUser(legacy=True)

    @app.get("/legacy-ok")
    def legacy_ok(_user=Depends(legacy_auth)):
        return {"ok": True}

    @app.get("/legacy-fail")
    def legacy_fail():
        return {"ok": False}

    client = TestClient(app)

    ok_response = client.get("/legacy-ok", headers={"X-API-Key": "legacy-key"})
    assert ok_response.status_code == 200
    assert ok_response.headers.get("Deprecation") == "true"
    assert ok_response.headers.get("X-Deprecation-Notice") is not None

    fail_response = client.get("/legacy-fail", headers={"X-API-Key": "legacy-key"})
    assert fail_response.status_code == 200
    assert fail_response.headers.get("Deprecation") is None
