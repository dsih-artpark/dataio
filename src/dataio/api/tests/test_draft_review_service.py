from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime
from types import SimpleNamespace

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

import pytest
from fastapi import HTTPException

from dataio.api.database.enums import DatasetManifestDraftStatus
from dataio.api.services import draft_review_service as service_module
from dataio.api.services.draft_review_service import DraftReviewService
from dataio.validate.reports.models import ValidationResult


def _fake_draft(**overrides):
    defaults = dict(
        draft_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        dataset_id="CS0007DS0112",
        collection_id="CS0007",
        category_id="CS",
        source_csv_path="foo.csv",
        digitization_log_path=None,
        raw_dataset_id="CSRDS0016",
        status=DatasetManifestDraftStatus.PENDING,
        draft_yaml="datasetTitle: Foo\n",
        draft_json={
            "datasetTitle": "Foo",
            "tables": {"main": {"description": "d", "data_dictionary": {"count": {"type": "int"}}}},
        },
        flagged_fields=[],
        reviewer_notes=[],
        validation_result=None,
        llm_model_id="anthropic/claude-3.5-sonnet",
        created_by="engineer@artpark.in",
        created_at=datetime(2026, 7, 21),
        reviewed_by=None,
        reviewed_at=None,
        superseded_by_draft_id=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_service():
    service = object.__new__(DraftReviewService)
    service.logger = logging.getLogger(__name__)
    return service


def test_get_draft_raises_404_when_missing(monkeypatch):
    service = _make_service()
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: None)

    with pytest.raises(HTTPException) as exc_info:
        service.get_draft("does-not-exist")
    assert exc_info.value.status_code == 404


def test_list_drafts_returns_serialized_rows(monkeypatch):
    service = _make_service()
    draft = _fake_draft()
    monkeypatch.setattr(service_module.database, "list_manifest_drafts", lambda **kw: ([draft], 1))

    result = service.list_drafts(status="pending")

    assert result["total"] == 1
    assert result["drafts"][0]["draft_id"] == str(draft.draft_id)
    assert result["drafts"][0]["status"] == "pending"


def test_delete_draft_raises_404_when_missing(monkeypatch):
    service = _make_service()
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: None)

    with pytest.raises(HTTPException) as exc_info:
        service.delete_draft("does-not-exist")
    assert exc_info.value.status_code == 404


def test_delete_draft_calls_db_delete(monkeypatch):
    service = _make_service()
    draft = _fake_draft()
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)
    monkeypatch.setattr(service_module.database, "check_if_dataset_exists", lambda dataset_id: True)
    monkeypatch.setattr(service_module, "check_if_raw_dataset_exists", lambda rds_id: True)

    recorded = {}
    monkeypatch.setattr(
        service_module.database, "delete_manifest_draft",
        lambda draft_id: recorded.setdefault("deleted_id", draft_id),
    )

    service.delete_draft(str(draft.draft_id))

    assert recorded["deleted_id"] == str(draft.draft_id)


def test_delete_draft_releases_reservation_when_dataset_not_yet_created(monkeypatch):
    service = _make_service()
    draft = _fake_draft(dataset_id="CS0007DS0999")
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)
    monkeypatch.setattr(service_module.database, "check_if_dataset_exists", lambda dataset_id: False)
    monkeypatch.setattr(service_module, "check_if_raw_dataset_exists", lambda rds_id: True)
    monkeypatch.setattr(service_module.database, "delete_manifest_draft", lambda draft_id: None)

    recorded = {}
    monkeypatch.setattr(
        service_module, "delete_reserved_dataset_id",
        lambda ds_id: recorded.setdefault("released_id", ds_id),
    )

    service.delete_draft(str(draft.draft_id))

    assert recorded["released_id"] == "CS0007DS0999"


def test_delete_draft_leaves_reservation_alone_when_dataset_already_exists(monkeypatch):
    service = _make_service()
    draft = _fake_draft(dataset_id="CS0007DS0112")
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)
    monkeypatch.setattr(service_module.database, "check_if_dataset_exists", lambda dataset_id: True)
    monkeypatch.setattr(service_module, "check_if_raw_dataset_exists", lambda rds_id: True)
    monkeypatch.setattr(service_module.database, "delete_manifest_draft", lambda draft_id: None)

    def fail_if_called(ds_id):
        raise AssertionError("should not release a reservation for a dataset that already exists")

    monkeypatch.setattr(service_module, "delete_reserved_dataset_id", fail_if_called)

    service.delete_draft(str(draft.draft_id))


def test_delete_draft_releases_raw_dataset_reservation_when_not_yet_created(monkeypatch):
    service = _make_service()
    draft = _fake_draft(raw_dataset_id="CSRDS0099")
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)
    monkeypatch.setattr(service_module.database, "check_if_dataset_exists", lambda dataset_id: True)
    monkeypatch.setattr(service_module, "check_if_raw_dataset_exists", lambda rds_id: False)
    monkeypatch.setattr(service_module.database, "delete_manifest_draft", lambda draft_id: None)

    recorded = {}
    monkeypatch.setattr(
        service_module, "delete_reserved_raw_dataset_id",
        lambda rds_id: recorded.setdefault("released_id", rds_id),
    )

    service.delete_draft(str(draft.draft_id))

    assert recorded["released_id"] == "CSRDS0099"


def test_delete_draft_leaves_raw_dataset_reservation_alone_when_already_created(monkeypatch):
    service = _make_service()
    draft = _fake_draft(raw_dataset_id="CSRDS0016")
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)
    monkeypatch.setattr(service_module.database, "check_if_dataset_exists", lambda dataset_id: True)
    monkeypatch.setattr(service_module, "check_if_raw_dataset_exists", lambda rds_id: True)
    monkeypatch.setattr(service_module.database, "delete_manifest_draft", lambda draft_id: None)

    def fail_if_called(rds_id):
        raise AssertionError("should not release a reservation for a raw dataset that already exists")

    monkeypatch.setattr(service_module, "delete_reserved_raw_dataset_id", fail_if_called)

    service.delete_draft(str(draft.draft_id))


def test_delete_draft_cleans_up_uploaded_files(monkeypatch, tmp_path):
    service = _make_service()
    csv_path = tmp_path / "foo.csv"
    csv_path.write_text("a,b\n1,2\n")
    log_path = tmp_path / "log.yaml"
    log_path.write_text("notes: x")

    draft = _fake_draft(
        source_csv_path=f'{{"main": "{csv_path.as_posix()}"}}',
        digitization_log_path=str(log_path),
    )
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)
    monkeypatch.setattr(service_module.database, "check_if_dataset_exists", lambda dataset_id: True)
    monkeypatch.setattr(service_module, "check_if_raw_dataset_exists", lambda rds_id: True)
    monkeypatch.setattr(service_module.database, "delete_manifest_draft", lambda draft_id: None)

    service.delete_draft(str(draft.draft_id))

    assert not csv_path.exists()
    assert not log_path.exists()


def test_revalidate_draft_converts_and_uses_existing_validator(monkeypatch):
    """revalidate_draft must go through the same conversion +
    DataIOValidator generate_draft() uses - not a separate/parallel
    validator - matching how every real dataset is actually checked.
    """
    service = _make_service()
    draft = _fake_draft(draft_json={
        "datasetTitle": "Foo",
        "tables": {"main": {"description": "d", "data_dictionary": {"count": {"type": "int"}}}},
    })
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    recorded = {}

    class FakeValidator:
        def validate_tabular(self, *, manifest, data_files, deep_check, full_scan):
            recorded["manifest"] = manifest
            recorded["data_files"] = data_files
            return ValidationResult(dataset_kind="tabular")

    monkeypatch.setattr(service_module, "DataIOValidator", FakeValidator)

    def fake_update_status(draft_id, status, **kw):
        recorded["kwargs"] = kw
        return _fake_draft()

    monkeypatch.setattr(service_module.database, "update_manifest_draft_status", fake_update_status)

    service.revalidate_draft(str(draft.draft_id))

    assert recorded["kwargs"]["validation_result"]["dataset_kind"] == "tabular"
    assert "datasetTables" in recorded["manifest"]
    assert "tables:" not in recorded["manifest"]
    assert recorded["data_files"] == {"main": draft.source_csv_path}


def test_get_draft_reports_whether_reserved_dataset_id_already_exists(monkeypatch):
    service = _make_service()
    draft = _fake_draft(dataset_id="CS0007DS0113")
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)
    monkeypatch.setattr(service_module.database, "check_if_dataset_exists", lambda dataset_id: False)

    result = service.get_draft(str(draft.draft_id))

    assert result["dataset_id"] == "CS0007DS0113"
    assert result["dataset_exists"] is False


def test_approve_draft_marks_status_approved(monkeypatch):
    """approve_draft is deliberately lightweight now - this tool only
    generates/validates/downloads, it doesn't upload anything. Approving
    just records the curator's acceptance; the dataset_id stays whatever
    was reserved at generation time, untouched.
    """
    service = _make_service()
    draft = _fake_draft()
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    recorded = {}

    def fake_update_status(draft_id, status, **kw):
        recorded["draft_id"] = draft_id
        recorded["status"] = status
        recorded["kwargs"] = kw
        return _fake_draft(status=DatasetManifestDraftStatus.APPROVED)

    monkeypatch.setattr(service_module.database, "update_manifest_draft_status", fake_update_status)

    result = service.approve_draft(str(draft.draft_id), "reviewer@artpark.in")

    assert recorded["draft_id"] == str(draft.draft_id)
    assert recorded["status"] == "approved"
    assert recorded["kwargs"]["reviewed_by"] == "reviewer@artpark.in"
    assert result["status"] == "approved"


def test_approve_draft_raises_404_when_missing(monkeypatch):
    service = _make_service()
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: None)

    with pytest.raises(HTTPException) as exc_info:
        service.approve_draft("does-not-exist", "reviewer@artpark.in")
    assert exc_info.value.status_code == 404


def test_reject_draft_records_reason(monkeypatch):
    service = _make_service()
    draft = _fake_draft()
    recorded = {}

    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)
    monkeypatch.setattr(service_module.database, "check_if_dataset_exists", lambda dataset_id: True)
    monkeypatch.setattr(service_module, "check_if_raw_dataset_exists", lambda rds_id: True)
    monkeypatch.setattr(
        service_module.database, "append_manifest_draft_note",
        lambda draft_id, note: recorded.setdefault("note", note),
    )
    monkeypatch.setattr(
        service_module.database, "update_manifest_draft_status",
        lambda draft_id, status, **kw: _fake_draft(status=DatasetManifestDraftStatus.REJECTED),
    )

    result = service.reject_draft(str(draft.draft_id), "reviewer@artpark.in", reason="not accurate")

    assert recorded["note"]["note"] == "not accurate"
    assert result["status"] == "rejected"


def test_reject_draft_releases_reservations_when_not_yet_created(monkeypatch):
    """Unlike regenerate_draft (which marks the original rejected via the
    raw database function directly, bypassing this release), a plain
    reject through this service method must free up both reservations -
    otherwise reject-and-restart cycles permanently burn through the
    global/category id counters for nothing.
    """
    service = _make_service()
    draft = _fake_draft(dataset_id="CS0007DS0999", raw_dataset_id="CSRDS0099")

    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)
    monkeypatch.setattr(service_module.database, "check_if_dataset_exists", lambda dataset_id: False)
    monkeypatch.setattr(service_module, "check_if_raw_dataset_exists", lambda rds_id: False)
    monkeypatch.setattr(
        service_module.database, "update_manifest_draft_status",
        lambda draft_id, status, **kw: _fake_draft(status=DatasetManifestDraftStatus.REJECTED),
    )

    released = {}
    monkeypatch.setattr(
        service_module, "delete_reserved_dataset_id",
        lambda ds_id: released.setdefault("ds_id", ds_id),
    )
    monkeypatch.setattr(
        service_module, "delete_reserved_raw_dataset_id",
        lambda rds_id: released.setdefault("rds_id", rds_id),
    )

    service.reject_draft(str(draft.draft_id), "reviewer@artpark.in")

    assert released["ds_id"] == "CS0007DS0999"
    assert released["rds_id"] == "CSRDS0099"


def test_generate_draft_from_upload_saves_files_and_calls_generate_draft(monkeypatch, tmp_path):
    import io

    from fastapi import UploadFile

    service = _make_service()

    saved_paths = iter(["/tmp/csv-abc.csv", "/tmp/log-abc.yaml"])
    monkeypatch.setattr(service_module, "save_upload", lambda upload_file: next(saved_paths))

    recorded = {}

    def fake_generate_draft(**kwargs):
        recorded.update(kwargs)
        return SimpleNamespace(model_dump=lambda: {"draft_id": "new-draft-id", "status": "pending"})

    monkeypatch.setattr("dataio.api.services.draft_service.generate_draft", fake_generate_draft)

    result = service.generate_draft_from_upload(
        csv_files=[UploadFile(filename="data.csv", file=io.BytesIO(b"a,b\n1,2\n"))],
        category_id="CS",
        collection_id="CS0007",
        data_owner_name="DAHD",
        created_by="engineer@artpark.in",
        digitization_log_file=UploadFile(filename="log.yaml", file=io.BytesIO(b"notes: x")),
    )

    assert recorded["csv_paths"] == ["/tmp/csv-abc.csv"]
    assert recorded["digitization_log_path"] == "/tmp/log-abc.yaml"
    assert recorded["category_id"] == "CS"
    assert result["draft_id"] == "new-draft-id"


def test_generate_draft_from_upload_saves_multiple_csvs(monkeypatch):
    import io

    from fastapi import UploadFile

    service = _make_service()

    saved_paths = iter(["/tmp/a.csv", "/tmp/b.csv"])
    monkeypatch.setattr(service_module, "save_upload", lambda upload_file: next(saved_paths))

    recorded = {}

    def fake_generate_draft(**kwargs):
        recorded.update(kwargs)
        return SimpleNamespace(model_dump=lambda: {"draft_id": "new-draft-id", "status": "pending"})

    monkeypatch.setattr("dataio.api.services.draft_service.generate_draft", fake_generate_draft)

    service.generate_draft_from_upload(
        csv_files=[
            UploadFile(filename="a.csv", file=io.BytesIO(b"a,b\n1,2\n")),
            UploadFile(filename="b.csv", file=io.BytesIO(b"c,d\n3,4\n")),
        ],
        category_id="CS",
        collection_id="CS0007",
        data_owner_name="DAHD",
        created_by="engineer@artpark.in",
    )

    assert recorded["csv_paths"] == ["/tmp/a.csv", "/tmp/b.csv"]


def test_generate_draft_from_upload_wraps_failures_as_502(monkeypatch):
    import io

    from fastapi import UploadFile

    service = _make_service()
    monkeypatch.setattr(service_module, "save_upload", lambda upload_file: "/tmp/csv-abc.csv")

    def failing_generate_draft(**kwargs):
        raise RuntimeError("OpenRouter is down")

    monkeypatch.setattr("dataio.api.services.draft_service.generate_draft", failing_generate_draft)

    with pytest.raises(HTTPException) as exc_info:
        service.generate_draft_from_upload(
            csv_files=[UploadFile(filename="data.csv", file=io.BytesIO(b"a,b\n1,2\n"))],
            category_id="CS",
            collection_id="CS0007",
            data_owner_name="DAHD",
            created_by="engineer@artpark.in",
        )
    assert exc_info.value.status_code == 502


def test_flag_field_delegates_to_db_function(monkeypatch):
    service = _make_service()
    draft = _fake_draft()
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    recorded = {}

    def fake_flag(draft_id, field_path, reason, flagged_by):
        recorded.update(draft_id=draft_id, field_path=field_path, reason=reason, flagged_by=flagged_by)
        return _fake_draft(status=DatasetManifestDraftStatus.FLAGGED, flagged_fields=[{"field": field_path, "reason": reason}])

    monkeypatch.setattr(service_module.database, "flag_manifest_draft_field", fake_flag)

    result = service.flag_field(str(draft.draft_id), "sourceTableID", "missing from CSV", "reviewer@artpark.in")

    assert recorded["field_path"] == "sourceTableID"
    assert result["status"] == "flagged"


def test_regenerate_draft_reuses_original_raw_dataset_id(monkeypatch):
    """The new draft must reuse the original's raw_dataset_id rather than
    reserving a fresh one - this is a redraft of the same dataset, not a
    new one, and generate_draft's own raw_dataset_id passthrough (tested in
    test_draft_service.py) is what makes reuse-not-reserve possible.
    """
    service = _make_service()
    original = _fake_draft(raw_dataset_id="CSRDS0016")
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: original)
    monkeypatch.setattr(
        service_module.database, "update_manifest_draft_status",
        lambda draft_id, status, **kw: None,
    )

    recorded = {}

    def fake_generate_draft(**kwargs):
        recorded.update(kwargs)
        return SimpleNamespace(model_dump=lambda: {"draft_id": "new-draft-id", "status": "pending"})

    monkeypatch.setattr("dataio.api.services.draft_service.generate_draft", fake_generate_draft)
    monkeypatch.setattr(
        "dataio.api.services.draft_service.decode_csv_paths",
        lambda source_csv_path, table_names=None: {"main": "foo.csv"},
    )

    result = service.regenerate_draft(str(original.draft_id), "reviewer@artpark.in")

    assert recorded["raw_dataset_id"] == "CSRDS0016"
    assert result["draft_id"] == "new-draft-id"
