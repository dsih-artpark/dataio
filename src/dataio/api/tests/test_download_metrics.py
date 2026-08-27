from __future__ import annotations

import logging
import os
from types import SimpleNamespace

from fastapi import HTTPException

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from dataio.api.database.models import DatasetDownload
from dataio.api.services.web_admin_service import WebAdminService
from dataio.api.services.web_user_service import WebUserService


def _download_row(user_email, dataset_id, channel="WEB", ua="Mozilla/5.0 (Windows NT 10.0) Chrome/120.0"):
    return SimpleNamespace(
        id="11111111-1111-1111-1111-111111111111",
        user_email=user_email,
        dataset_id=dataset_id,
        access_channel=channel,
        ip_address="127.0.0.1",
        user_agent=ua,
        downloaded_at=None,
    )


class FakeDownloadQuery:
    """Stands in for session.query(DatasetDownload)...filter()... .

    Like QueryStub in test_admin_manifest_service.py, this doesn't interpret
    the real SQLAlchemy filter expressions - .filter() just switches to a
    pre-configured, narrower row set. That's enough to prove
    get_download_metrics computes unique_users/unique_datasets from that
    SAME filtered set rather than a fresh unfiltered query (the bug this
    regression test guards against).
    """

    def __init__(self, all_rows, filtered_rows=None):
        self._rows = all_rows
        self._filtered_rows = filtered_rows if filtered_rows is not None else all_rows
        self._offset = 0
        self._limit = None

    def filter(self, *_conditions):
        self._rows = self._filtered_rows
        return self

    def count(self):
        return len(self._rows)

    def order_by(self, *_args, **_kwargs):
        return self

    def offset(self, n):
        self._offset = n
        return self

    def limit(self, n):
        self._limit = n
        return self

    def all(self):
        rows = self._rows[self._offset :]
        return rows[: self._limit] if self._limit is not None else rows

    def with_entities(self, *_args):
        return self

    def scalar(self):
        return len(self._rows)


class FakeTitlesQuery:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *_conditions):
        return self

    def all(self):
        return self._rows


class MetricsSessionStub:
    def __init__(self, download_query, titles_rows=None):
        self._download_query = download_query
        self._titles_rows = titles_rows or []

    def query(self, *args):
        # `is`, not `==`: comparing a real SQLAlchemy column/model with `==`
        # doesn't do normal Python equality, it builds a SQL clause element
        # - `args == (DatasetDownload,)` blows up as soon as `args` holds
        # real column attributes (the Dataset.ds_id, Dataset.title titles
        # lookup below) instead of short-circuiting on the length mismatch.
        if len(args) == 1 and args[0] is DatasetDownload:
            return self._download_query
        return FakeTitlesQuery(self._titles_rows)

    def rollback(self):
        pass

    def close(self):
        pass


def test_get_download_metrics_requires_admin():
    service = object.__new__(WebAdminService)
    service.logger = logging.getLogger(__name__)

    non_admin = SimpleNamespace(email="user@example.com", is_admin=False, is_group=False)
    try:
        service.get_download_metrics(non_admin)
        raise AssertionError("Expected HTTPException for a non-admin caller")
    except HTTPException as exc:
        assert exc.status_code == 403


def test_get_download_metrics_returns_summary_and_parsed_downloads(monkeypatch):
    service = object.__new__(WebAdminService)
    service.logger = logging.getLogger(__name__)
    service._require_admin = lambda _user: None

    rows = [
        _download_row("a@example.com", "TS0001DS0001", ua="dataio-sdk/1.2.0"),
        _download_row("b@example.com", "TS0001DS0002", ua="Mozilla/5.0 (Windows NT 10.0) Chrome/120.0"),
        _download_row("c@example.com", "TS0001DS0003", ua=None),
    ]
    query = FakeDownloadQuery(rows)
    session = MetricsSessionStub(query, titles_rows=[SimpleNamespace(ds_id="TS0001DS0001", title="Sample Title")])
    monkeypatch.setattr("dataio.api.services.web_admin_service.DBSession", lambda: session)

    result = service.get_download_metrics(SimpleNamespace(email="admin@example.com", is_admin=True, is_group=False))

    assert result["summary"] == {"total_downloads": 3, "unique_users": 3, "unique_datasets": 3}
    by_id = {d["dataset_id"]: d for d in result["downloads"]}

    # Dataset row exists -> real title used.
    assert by_id["TS0001DS0001"]["dataset_title"] == "Sample Title"
    assert by_id["TS0001DS0001"]["device_info"] == "Python SDK / Script"
    # No matching Dataset row -> falls back to the raw dataset_id, not a crash.
    assert by_id["TS0001DS0002"]["dataset_title"] == "TS0001DS0002"
    assert by_id["TS0001DS0002"]["device_info"] == "Windows (Chrome)"
    # No user agent recorded -> labeled, not a crash on None.
    assert by_id["TS0001DS0003"]["device_info"] == "Unknown Device"


def test_get_download_metrics_unique_counts_respect_filters(monkeypatch):
    """Regression test: unique_users/unique_datasets used to be computed
    from a fresh, unfiltered session.query(DatasetDownload) and would have
    reported counts across ALL rows regardless of dataset_id/user_email/
    channel/search filters applied to `total` and `downloads`.
    """
    service = object.__new__(WebAdminService)
    service.logger = logging.getLogger(__name__)
    service._require_admin = lambda _user: None

    all_rows = [
        _download_row("a@example.com", "TS0001DS0001"),
        _download_row("b@example.com", "TS0001DS0002"),
    ]
    filtered_rows = [all_rows[0]]

    query = FakeDownloadQuery(all_rows, filtered_rows=filtered_rows)
    session = MetricsSessionStub(query)
    monkeypatch.setattr("dataio.api.services.web_admin_service.DBSession", lambda: session)

    result = service.get_download_metrics(
        SimpleNamespace(email="admin@example.com", is_admin=True, is_group=False),
        dataset_id="TS0001DS0001",
    )

    assert result["summary"]["total_downloads"] == 1
    assert result["summary"]["unique_users"] == 1
    assert result["summary"]["unique_datasets"] == 1


def test_get_download_metrics_degrades_gracefully_before_migration(monkeypatch):
    """A pre-migration DB (table doesn't exist yet) must return a friendly
    zeroed summary with a warning, not a 500."""
    service = object.__new__(WebAdminService)
    service.logger = logging.getLogger(__name__)
    service._require_admin = lambda _user: None

    class RaisingQuery:
        def filter(self, *_conditions):
            return self

        def count(self):
            raise Exception('relation "dataset_downloads" does not exist')

    session = MetricsSessionStub(RaisingQuery())
    monkeypatch.setattr("dataio.api.services.web_admin_service.DBSession", lambda: session)

    result = service.get_download_metrics(SimpleNamespace(email="admin@example.com", is_admin=True, is_group=False))

    assert result["summary"] == {"total_downloads": 0, "unique_users": 0, "unique_datasets": 0}
    assert result["downloads"] == []
    assert "warning" in result


class UserDBSessionStub:
    def __init__(self, raise_on_commit=False):
        self.added = []
        self.committed = False
        self.rolled_back = False
        self.raise_on_commit = raise_on_commit

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        if self.raise_on_commit:
            raise Exception("db unavailable")
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def _downloadable_dataset():
    return SimpleNamespace(
        ds_id="TS0001DS0001",
        title="Sample Dataset",
        access_level=SimpleNamespace(value="DOWNLOAD"),
        readme_md=None,
        data_dictionary_json=None,
        manifest_yaml=None,
        manifest_json=None,
    )


class FilestoreStub:
    def list_files_in_s3(self, _dataset_id, _version_type):
        return [{"table_name": "sample", "download_link": "https://example/sample.csv", "metadata": {}}]


def test_get_dataset_download_urls_logs_audit_row(monkeypatch):
    service = object.__new__(WebUserService)
    service.logger = logging.getLogger(__name__)

    monkeypatch.setattr("dataio.api.services.web_user_service.database.get_dataset", lambda _id: _downloadable_dataset())
    monkeypatch.setattr("dataio.api.services.web_user_service.determine_user_permissions", lambda _user: [])
    monkeypatch.setattr("dataio.api.services.filestore_service.FilestoreService", FilestoreStub)

    session = UserDBSessionStub()
    monkeypatch.setattr("dataio.api.services.web_user_service.DBSession", lambda: session)

    user = SimpleNamespace(email="reader@example.com", is_admin=False)
    result = service.get_dataset_download_urls(
        user, "TS0001DS0001", access_channel="SDK", ip_address="10.0.0.5", user_agent="dataio-sdk/1.0"
    )

    assert result["ds_id"] == "TS0001DS0001"
    assert session.committed is True
    assert len(session.added) == 1
    logged = session.added[0]
    assert logged.user_email == "reader@example.com"
    assert logged.dataset_id == "TS0001DS0001"
    assert logged.access_channel == "SDK"
    assert logged.ip_address == "10.0.0.5"
    assert logged.user_agent == "dataio-sdk/1.0"


def test_get_dataset_download_urls_survives_audit_log_failure(monkeypatch):
    """A DB hiccup while writing the audit row must not block the download
    itself - get_dataset_download_urls wraps that insert in its own
    try/except specifically so this degrades silently instead of turning an
    analytics-logging failure into a user-facing download failure."""
    service = object.__new__(WebUserService)
    service.logger = logging.getLogger(__name__)

    monkeypatch.setattr("dataio.api.services.web_user_service.database.get_dataset", lambda _id: _downloadable_dataset())
    monkeypatch.setattr("dataio.api.services.web_user_service.determine_user_permissions", lambda _user: [])
    monkeypatch.setattr("dataio.api.services.filestore_service.FilestoreService", FilestoreStub)

    session = UserDBSessionStub(raise_on_commit=True)
    monkeypatch.setattr("dataio.api.services.web_user_service.DBSession", lambda: session)

    user = SimpleNamespace(email="reader@example.com", is_admin=False)
    result = service.get_dataset_download_urls(user, "TS0001DS0001")

    assert result["ds_id"] == "TS0001DS0001"
    assert len(result["tables"]) == 1
    assert session.rolled_back is True


def test_get_dataset_download_urls_denies_without_permission(monkeypatch):
    service = object.__new__(WebUserService)
    service.logger = logging.getLogger(__name__)

    dataset = SimpleNamespace(
        ds_id="TS0001DS0001",
        title="Sample Dataset",
        access_level=SimpleNamespace(value="VIEW"),
    )
    monkeypatch.setattr("dataio.api.services.web_user_service.database.get_dataset", lambda _id: dataset)
    monkeypatch.setattr("dataio.api.services.web_user_service.determine_user_permissions", lambda _user: [])

    session = UserDBSessionStub()
    monkeypatch.setattr("dataio.api.services.web_user_service.DBSession", lambda: session)

    user = SimpleNamespace(email="reader@example.com", is_admin=False)
    try:
        service.get_dataset_download_urls(user, "TS0001DS0001")
        raise AssertionError("Expected HTTPException for a user without download access")
    except HTTPException as exc:
        assert exc.status_code == 403

    # Permission was denied before the audit-log step - nothing should have
    # been logged for a download that never happened.
    assert session.added == []
