from __future__ import annotations

import json
import os
from types import SimpleNamespace

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

import pytest
from fastapi import HTTPException

from dataio.api.database.enums import VersionType
from dataio.api.services.web_admin_service import WebAdminService

ADMIN_USER = SimpleNamespace(email="admin@example.com", is_admin=True, is_group=False)


def _fake_draft(*, status: str, source_csv_path: str):
    return SimpleNamespace(
        status=SimpleNamespace(value=status),
        draft_yaml="datasetTitle: foo\ntables:\n  foo: {}\n",
        draft_json={"tables": {"foo": {}}},
        dataset_id="CS0007DS0999",
        collection_id="CS0007",
        raw_dataset_id="CSRDS0099",
        source_csv_path=source_csv_path,
    )


def _service():
    return WebAdminService()


def test_import_dataset_from_draft_happy_path(monkeypatch, tmp_path):
    csv_path = tmp_path / "foo.csv"
    csv_path.write_bytes(b"a,b\n1,2\n")
    draft = _fake_draft(status="approved", source_csv_path=json.dumps({"foo": str(csv_path)}))

    service = _service()
    monkeypatch.setattr(service.draft_review_service, "_get_draft_or_404", lambda draft_id: draft)
    monkeypatch.setattr(
        service.draft_review_service,
        "generate_info_yaml",
        lambda draft_id, access_level: {"info_yaml": f"ds_id: {draft.dataset_id}\naccess_level: {access_level}\n"},
    )

    captured = {}

    def fake_import_dataset_package(
        admin_user, info_file, metadata_file, csv_files, dataset_override=None,
        raw_dataset_override=None, bucket_type=VersionType.STANDARDISED,
    ):
        captured["admin_user"] = admin_user
        captured["info_text"] = info_file.file.read().decode("utf-8")
        captured["metadata_text"] = metadata_file.file.read().decode("utf-8")
        captured["csv_files"] = {f.filename: f.file.read().decode("utf-8") for f in csv_files}
        captured["dataset_override"] = dataset_override
        captured["bucket_type"] = bucket_type
        return {"dataset_id": draft.dataset_id, "bucket_type": bucket_type.value, "uploaded_tables": ["foo"], "manifest_uploaded": True}

    monkeypatch.setattr(service, "import_dataset_package", fake_import_dataset_package)

    result = service.import_dataset_from_draft(ADMIN_USER, "draft-1", "VIEW", VersionType.STANDARDISED)

    assert result["dataset_id"] == "CS0007DS0999"
    assert "access_level: VIEW" in captured["info_text"]
    assert captured["metadata_text"] == draft.draft_yaml
    assert captured["csv_files"] == {"foo.csv": "a,b\n1,2\n"}
    assert captured["dataset_override"] == {"existing_dataset_id": "CS0007DS0999"}
    assert captured["bucket_type"] == VersionType.STANDARDISED


def test_import_dataset_from_draft_rejects_non_approved(monkeypatch, tmp_path):
    draft = _fake_draft(status="pending", source_csv_path=json.dumps({"foo": str(tmp_path / "foo.csv")}))

    service = _service()
    monkeypatch.setattr(service.draft_review_service, "_get_draft_or_404", lambda draft_id: draft)

    def fail_if_called(*a, **kw):
        raise AssertionError("import_dataset_package must not be called for a non-approved draft")

    monkeypatch.setattr(service, "import_dataset_package", fail_if_called)

    with pytest.raises(HTTPException) as exc_info:
        service.import_dataset_from_draft(ADMIN_USER, "draft-1", "NONE", VersionType.STANDARDISED)

    assert exc_info.value.status_code == 400


def test_import_dataset_from_draft_raises_clear_error_for_missing_csv(monkeypatch, tmp_path):
    missing_path = tmp_path / "does_not_exist.csv"
    draft = _fake_draft(status="approved", source_csv_path=json.dumps({"foo": str(missing_path)}))

    service = _service()
    monkeypatch.setattr(service.draft_review_service, "_get_draft_or_404", lambda draft_id: draft)
    monkeypatch.setattr(
        service.draft_review_service, "generate_info_yaml", lambda draft_id, access_level: {"info_yaml": "ds_id: x\n"}
    )

    def fail_if_called(*a, **kw):
        raise AssertionError("import_dataset_package must not be called when a CSV can't be read")

    monkeypatch.setattr(service, "import_dataset_package", fail_if_called)

    with pytest.raises(HTTPException) as exc_info:
        service.import_dataset_from_draft(ADMIN_USER, "draft-1", "NONE", VersionType.STANDARDISED)

    assert exc_info.value.status_code == 400
    assert "foo" in exc_info.value.detail
