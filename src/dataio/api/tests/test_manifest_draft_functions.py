from __future__ import annotations

import os
import uuid

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

import pytest

from dataio.api.database.functions import _coerce_draft_id

# The CRUD functions themselves (create_manifest_draft, get_manifest_draft,
# etc.) open a real Session and are exercised against a live DB, matching
# this repo's existing convention of not standing up a test DB for
# dataio.api.database.functions - see test_admin_manifest_service.py, which
# only ever monkeypatches these at the service-module boundary. This file
# covers the one piece of pure logic that doesn't need a live session.


def test_coerce_draft_id_accepts_uuid_instance():
    value = uuid.uuid4()
    assert _coerce_draft_id(value) == value


def test_coerce_draft_id_accepts_string():
    value = uuid.uuid4()
    assert _coerce_draft_id(str(value)) == value


def test_coerce_draft_id_rejects_garbage():
    with pytest.raises(ValueError):
        _coerce_draft_id("not-a-uuid")
