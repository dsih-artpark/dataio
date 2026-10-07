from __future__ import annotations

import os

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

import pytest

from dataio.api.database.rds_id_helpers import resolve_category_id, resolve_rds_id


def test_resolve_category_id_prefers_explicit_category():
    meta = {"category": {"ID": "CS", "name": "Census and Surveys"}, "collection": {"ID": "CS0007"}}
    assert resolve_category_id(meta) == "CS"


def test_resolve_category_id_falls_back_to_collection_id():
    meta = {"collection": {"ID": "CS0007", "name": "Livestock Census"}}
    assert resolve_category_id(meta) == "CS"


def test_resolve_category_id_empty_when_nothing_present():
    assert resolve_category_id({}) == ""


def test_resolve_rds_id_calls_db_counter(monkeypatch):
    calls = {}

    def fake_suggest(category_id):
        calls["category_id"] = category_id
        return "CSRDS0016"

    monkeypatch.setattr(
        "dataio.api.database.rds_id_helpers.suggest_next_raw_dataset_id_for_category",
        fake_suggest,
    )

    meta = {"category": {"ID": "CS"}, "collection": {"ID": "CS0007"}}
    assert resolve_rds_id(meta) == "CSRDS0016"
    assert calls["category_id"] == "CS"


def test_resolve_rds_id_raises_without_any_category_info():
    with pytest.raises(ValueError):
        resolve_rds_id({})
