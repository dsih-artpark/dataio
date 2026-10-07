import asyncio
import os
import threading

os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from dataio.api.auth import decorators
from dataio.api.database.models import User


def _admin_user():
    return User(email="admin@example.com", is_group=False)


def test_admin_required_runs_sync_routes_off_the_event_loop(monkeypatch):
    monkeypatch.setattr(decorators, "require_admin", lambda _user: None)
    seen = {}

    @decorators.admin_required
    def blocking_route(user):
        seen["thread"] = threading.get_ident()
        return "done"

    async def call():
        seen["loop_thread"] = threading.get_ident()
        return await blocking_route(user=_admin_user())

    assert asyncio.run(call()) == "done"
    assert seen["thread"] != seen["loop_thread"]


def test_admin_required_still_awaits_async_routes(monkeypatch):
    monkeypatch.setattr(decorators, "require_admin", lambda _user: None)

    @decorators.admin_required
    async def async_route(user):
        return "awaited"

    assert asyncio.run(async_route(user=_admin_user())) == "awaited"
