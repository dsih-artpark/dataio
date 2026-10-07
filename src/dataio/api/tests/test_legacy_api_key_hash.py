import bcrypt
import pytest

from dataio.api.auth import providers
from dataio.api.auth.providers import _legacy_key_hash_bytes


RAW_KEY = "legacy-raw-key"
HASH_STR = bcrypt.hashpw(RAW_KEY.encode("utf-8"), bcrypt.gensalt(rounds=4)).decode("utf-8")
# What psycopg2 + Postgres stored in users.key (TEXT) when create_user bound raw bytes
HASH_BYTEA_HEX = "\\x" + HASH_STR.encode("utf-8").hex()


@pytest.mark.parametrize("stored", [HASH_STR, HASH_BYTEA_HEX, HASH_STR.encode("utf-8")])
def test_legacy_key_hash_bytes_verifies_every_stored_form(stored):
    assert bcrypt.checkpw(RAW_KEY.encode("utf-8"), _legacy_key_hash_bytes(stored))


class _FakeUser:
    def __init__(self, email, key):
        self.email = email
        self.key = key


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows

    def query(self, _model):
        return _FakeQuery(self._rows)

    def expunge(self, _obj):
        pass

    def close(self):
        pass


@pytest.mark.parametrize("stored", [HASH_STR, HASH_BYTEA_HEX])
def test_check_api_key_accepts_legacy_text_hashes(monkeypatch, stored):
    user = _FakeUser("legacy@example.com", stored)
    monkeypatch.setattr(providers, "Session", lambda: _FakeSession([user]))

    result = providers.check_api_key(RAW_KEY)

    assert result is user
    assert result._legacy_key_used is True


def test_check_api_key_rejects_wrong_legacy_key(monkeypatch):
    monkeypatch.setattr(providers, "Session", lambda: _FakeSession([_FakeUser("legacy@example.com", HASH_STR)]))

    assert providers.check_api_key("not-the-key") is None
