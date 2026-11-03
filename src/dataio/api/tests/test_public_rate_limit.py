"""Per-IP limits for public (signed-out) actions."""

from __future__ import annotations

import os

import pytest
from fastapi import HTTPException
from starlette.requests import Request

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from sqlalchemy.orm import sessionmaker

from dataio.api.auth import security
from dataio.api.database.models import AuthRateLimit
from dataio.api.tests.postgres_schema import fresh_schema_engine, requires_postgres


def make_request(client_ip="203.0.113.7", headers=None) -> Request:
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": raw_headers,
            "client": (client_ip, 1234),
        }
    )


# ------------------------------------------------------------ client IP


def test_proxy_headers_are_ignored_unless_trusted(monkeypatch):
    monkeypatch.delenv("TRUST_PROXY_HEADERS", raising=False)
    request = make_request(headers={"X-Forwarded-For": "198.51.100.1"})
    assert security.get_client_ip(request) == "203.0.113.7"


@pytest.mark.parametrize(
    "headers, expected",
    [
        ({"CF-Connecting-IP": "198.51.100.1", "X-Real-IP": "198.51.100.2"}, "198.51.100.1"),
        ({"X-Real-IP": " 198.51.100.2 ", "X-Forwarded-For": "198.51.100.3"}, "198.51.100.2"),
        ({"X-Forwarded-For": "198.51.100.3, 10.0.0.1"}, "198.51.100.3"),
        ({}, "203.0.113.7"),
    ],
)
def test_trusted_proxy_headers(monkeypatch, headers, expected):
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "true")
    assert security.get_client_ip(make_request(headers=headers)) == expected


def test_limit_is_keyed_by_action_and_ip(monkeypatch):
    calls = []
    monkeypatch.setattr(security, "enforce_rate_limit", lambda **kwargs: calls.append(kwargs))
    security.enforce_ip_rate_limit(make_request(), "download", limit=20, window_seconds=3600)
    assert calls == [
        {
            "action": "public:download",
            "subject": "ip:203.0.113.7",
            "limit": 20,
            "window_seconds": 3600,
            "block_seconds": 3600,
        }
    ]


# ------------------------------------------------------- against Postgres


@pytest.fixture
def rate_limit_db(monkeypatch):
    engine = fresh_schema_engine()
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(security, "DBSession", factory)
    yield factory
    engine.dispose()


@requires_postgres
def test_blocks_an_ip_after_the_limit_and_not_others(rate_limit_db):
    for _ in range(3):
        security.enforce_ip_rate_limit(make_request(), "chat", limit=3, window_seconds=60)
    with pytest.raises(HTTPException) as exc_info:
        security.enforce_ip_rate_limit(make_request(), "chat", limit=3, window_seconds=60)
    assert exc_info.value.status_code == 429

    # still blocked on the next call, even though no new window has started
    with pytest.raises(HTTPException):
        security.enforce_ip_rate_limit(make_request(), "chat", limit=3, window_seconds=60)

    # another IP, and the same IP for another action, are unaffected
    security.enforce_ip_rate_limit(
        make_request(client_ip="203.0.113.8"), "chat", limit=3, window_seconds=60
    )
    security.enforce_ip_rate_limit(make_request(), "download", limit=3, window_seconds=60)

    session = rate_limit_db()
    buckets = {(b.action, b.subject): b.attempt_count for b in session.query(AuthRateLimit)}
    session.close()
    assert buckets == {
        ("public:chat", "ip:203.0.113.7"): 4,
        ("public:chat", "ip:203.0.113.8"): 1,
        ("public:download", "ip:203.0.113.7"): 1,
    }
