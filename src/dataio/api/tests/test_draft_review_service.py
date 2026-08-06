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
import yaml
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


def test_update_draft_content_revalidates_and_persists(monkeypatch):
    service = _make_service()
    draft = _fake_draft(draft_json={
        "datasetTitle": "Foo",
        "tables": {"main": {"description": "old", "data_dictionary": {"count": {"type": "int"}}}},
    })
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    recorded = {}

    class FakeValidator:
        def validate_tabular(self, *, manifest, data_files, deep_check, full_scan):
            recorded["manifest"] = manifest
            return ValidationResult(dataset_kind="tabular")

    monkeypatch.setattr(service_module, "DataIOValidator", FakeValidator)

    def fake_update_content(draft_id, *, draft_yaml, draft_json, validation_result=None):
        recorded["draft_yaml"] = draft_yaml
        recorded["draft_json"] = draft_json
        recorded["validation_result"] = validation_result
        return _fake_draft(draft_yaml=draft_yaml, draft_json=draft_json)

    monkeypatch.setattr(service_module.database, "update_manifest_draft_content", fake_update_content)

    new_yaml = "datasetTitle: Foo\ntables:\n  main:\n    description: new and improved\n"
    result = service.update_draft_content(str(draft.draft_id), new_yaml)

    assert recorded["draft_yaml"] == new_yaml
    assert recorded["draft_json"]["tables"]["main"]["description"] == "new and improved"
    assert recorded["validation_result"]["dataset_kind"] == "tabular"
    assert result["draft_yaml"] == new_yaml


def test_update_draft_content_stringifies_yaml_implicit_dates(monkeypatch):
    """yaml.safe_load silently parses an ISO-8601-looking scalar (e.g. a
    temporalCoverage value like 2019-06-30) into a real datetime.date -
    every manifest field is a plain string everywhere else in the app, and
    the JSONB column's json serializer can't write a date object out, so
    saving a curator edit containing one used to crash with a 500.
    """
    service = _make_service()
    draft = _fake_draft(draft_json={"datasetTitle": "Foo", "tables": {}})
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    class FakeValidator:
        def validate_tabular(self, *, manifest, data_files, deep_check, full_scan):
            return ValidationResult(dataset_kind="tabular")

    monkeypatch.setattr(service_module, "DataIOValidator", FakeValidator)

    recorded = {}

    def fake_update_content(draft_id, *, draft_yaml, draft_json, validation_result=None):
        recorded["draft_json"] = draft_json
        return _fake_draft(draft_yaml=draft_yaml, draft_json=draft_json)

    monkeypatch.setattr(
        service_module.database, "update_manifest_draft_content", fake_update_content
    )

    new_yaml = "datasetTitle: Foo\ntemporalCoverage: 2019-06-30\ntables: {}\n"
    service.update_draft_content(str(draft.draft_id), new_yaml)

    assert recorded["draft_json"]["temporalCoverage"] == "2019-06-30"
    assert isinstance(recorded["draft_json"]["temporalCoverage"], str)


def test_update_draft_content_reorders_keys_to_canonical_order(monkeypatch):
    """A full-manifest save (e.g. the Draft Review screen's numeric-field
    "Apply" action, or a raw YAML edit) submits draft_json in whatever key
    order it happened to come back in from the DB's JSONB column, which
    does not preserve insertion order - both the persisted draft_json and
    the regenerated draft_yaml must come back in the same canonical order
    every real metadata.yaml uses (datasetTitle, ..., source, ..., tables).
    """
    service = _make_service()
    draft = _fake_draft(draft_json={"datasetTitle": "Foo", "tables": {}})
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    class FakeValidator:
        def validate_tabular(self, *, manifest, data_files, deep_check, full_scan):
            return ValidationResult(dataset_kind="tabular")

    monkeypatch.setattr(service_module, "DataIOValidator", FakeValidator)

    recorded = {}

    def fake_update_content(draft_id, *, draft_yaml, draft_json, validation_result=None):
        recorded["draft_yaml"] = draft_yaml
        recorded["draft_json"] = draft_json
        return _fake_draft(draft_yaml=draft_yaml, draft_json=draft_json)

    monkeypatch.setattr(
        service_module.database, "update_manifest_draft_content", fake_update_content
    )

    # Keys deliberately out of canonical order, as a JSONB round-trip would
    # scramble them.
    scrambled_yaml = "tables: {}\nsource: []\ndatasetTitle: Foo\n"
    service.update_draft_content(str(draft.draft_id), scrambled_yaml)

    assert list(recorded["draft_json"].keys()) == ["datasetTitle", "source", "tables"]
    assert recorded["draft_yaml"].index("datasetTitle") < recorded["draft_yaml"].index("source")
    assert recorded["draft_yaml"].index("source") < recorded["draft_yaml"].index("tables")


def test_update_draft_content_restores_data_dictionary_column_and_field_order(
    monkeypatch, tmp_path
):
    """The same JSONB-order-loss problem hits nested structure too: a
    data_dictionary's column order and each field's own key order (e.g.
    "min" ending up before "type") both get scrambled by a round-trip. The
    fix re-derives column order from the table's own CSV header (the true
    source of "natural" order) and re-canonicalizes each field's own keys.
    """
    service = _make_service()
    csv_path = tmp_path / "main.csv"
    csv_path.write_text("state.ID,year,species,count\n", encoding="utf-8")
    draft = _fake_draft(
        source_csv_path=f'{{"main": "{csv_path.as_posix()}"}}',
        draft_json={
            "datasetTitle": "Foo",
            "tables": {
                "main": {
                    "description": "d",
                    "data_dictionary": {
                        # Deliberately out of CSV-header order, and each
                        # field's own keys deliberately scrambled too.
                        "count": {"min": 0, "type": "int", "additive": True},
                        "state.ID": {"nullable": False, "type": "regionID"},
                        "species": {"type": "enum", "enumRef": "speciesEnum"},
                        "year": {"type": "date", "format": "%Y"},
                    },
                }
            },
        },
    )
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    class FakeValidator:
        def validate_tabular(self, *, manifest, data_files, deep_check, full_scan):
            return ValidationResult(dataset_kind="tabular")

    monkeypatch.setattr(service_module, "DataIOValidator", FakeValidator)

    recorded = {}

    def fake_update_content(draft_id, *, draft_yaml, draft_json, validation_result=None):
        recorded["draft_json"] = draft_json
        return _fake_draft(draft_yaml=draft_yaml, draft_json=draft_json)

    monkeypatch.setattr(
        service_module.database, "update_manifest_draft_content", fake_update_content
    )

    service.update_draft_content(str(draft.draft_id), yaml.safe_dump(draft.draft_json))

    data_dictionary = recorded["draft_json"]["tables"]["main"]["data_dictionary"]
    assert list(data_dictionary.keys()) == ["state.ID", "year", "species", "count"]
    assert list(data_dictionary["count"].keys()) == ["type", "additive", "min"]


def test_update_draft_content_rejects_invalid_yaml(monkeypatch):
    service = _make_service()
    draft = _fake_draft()
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    with pytest.raises(HTTPException) as exc_info:
        service.update_draft_content(str(draft.draft_id), "not: valid: yaml: at: all:")
    assert exc_info.value.status_code == 400


def test_update_draft_content_rejects_non_mapping_yaml(monkeypatch):
    service = _make_service()
    draft = _fake_draft()
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    with pytest.raises(HTTPException) as exc_info:
        service.update_draft_content(str(draft.draft_id), "- just\n- a\n- list\n")
    assert exc_info.value.status_code == 400


@pytest.mark.parametrize("status", [DatasetManifestDraftStatus.APPROVED, DatasetManifestDraftStatus.REJECTED])
def test_update_draft_content_rejects_editing_approved_or_rejected_draft(monkeypatch, status):
    service = _make_service()
    draft = _fake_draft(status=status)
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    with pytest.raises(HTTPException) as exc_info:
        service.update_draft_content(str(draft.draft_id), "datasetTitle: Foo\n")
    assert exc_info.value.status_code == 400


def test_generate_info_yaml_passes_draft_fields_to_the_builder(monkeypatch):
    service = _make_service()
    draft = _fake_draft(
        dataset_id="CS0007DS0119",
        collection_id="CS0007",
        raw_dataset_id="CS0007RDS0005",
        status=DatasetManifestDraftStatus.APPROVED,
        draft_json={"datasetTitle": "Foo", "tables": {}},
    )
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    recorded = {}

    def fake_build(draft_json, *, dataset_id, collection_id, raw_dataset_id, access_level):
        recorded.update(
            draft_json=draft_json, dataset_id=dataset_id, collection_id=collection_id,
            raw_dataset_id=raw_dataset_id, access_level=access_level,
        )
        return "ds_id: CS0007DS0119\n"

    monkeypatch.setattr("dataio.api.services.info_yaml_builder.build_info_yaml", fake_build)

    result = service.generate_info_yaml(str(draft.draft_id), "DOWNLOAD")

    assert recorded["draft_json"] == {"datasetTitle": "Foo", "tables": {}}
    assert recorded["dataset_id"] == "CS0007DS0119"
    assert recorded["collection_id"] == "CS0007"
    assert recorded["raw_dataset_id"] == "CS0007RDS0005"
    assert recorded["access_level"] == "DOWNLOAD"
    assert result == {"info_yaml": "ds_id: CS0007DS0119\n"}


def test_generate_info_yaml_does_not_persist_anything(monkeypatch):
    """Same non-persisting contract as update_draft_content/revalidate_draft
    - this only returns text for the curator to download.
    """
    service = _make_service()
    draft = _fake_draft(
        status=DatasetManifestDraftStatus.APPROVED,
        draft_json={"datasetTitle": "Foo", "tables": {}},
    )
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    def fail_if_called(*a, **kw):
        raise AssertionError("generate_info_yaml must not write to the database")

    monkeypatch.setattr(service_module.database, "update_manifest_draft_content", fail_if_called)
    monkeypatch.setattr(service_module.database, "update_manifest_draft_status", fail_if_called)

    result = service.generate_info_yaml(str(draft.draft_id), "NONE")

    assert "access_level: NONE" in result["info_yaml"]


@pytest.mark.parametrize(
    "status",
    [DatasetManifestDraftStatus.PENDING, DatasetManifestDraftStatus.FLAGGED, DatasetManifestDraftStatus.REJECTED],
)
def test_generate_info_yaml_rejects_a_draft_that_is_not_yet_approved(monkeypatch, status):
    """info.yml must reflect the curator's *final* review, not a draft
    that's still changeable - only generate it once approve_draft has
    frozen the manifest (update_draft_content already refuses further
    edits at that point).
    """
    service = _make_service()
    draft = _fake_draft(status=status)
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: draft)

    with pytest.raises(HTTPException) as exc_info:
        service.generate_info_yaml(str(draft.draft_id), "NONE")
    assert exc_info.value.status_code == 400


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


def _valid_curator_input(**overrides) -> dict:
    defaults = dict(
        datasetDescription="desc",
        spatialCoverage="India",
        spatialResolution="state",
        temporalCoverage="1997-2019",
        temporalResolution="annual",
        updateFrequency="annual",
    )
    defaults.update(overrides)
    return defaults


def test_generate_deterministic_draft_from_upload_saves_files_and_calls_generator(monkeypatch):
    import io

    from fastapi import UploadFile

    service = _make_service()
    saved_paths = iter(["/tmp/csv-abc.csv"])
    monkeypatch.setattr(service_module, "save_upload", lambda upload_file: next(saved_paths))

    recorded = {}

    def fake_generate_deterministic_draft(**kwargs):
        recorded.update(kwargs)
        return SimpleNamespace(model_dump=lambda: {"draft_id": "new-draft-id", "status": "pending"})

    monkeypatch.setattr(
        "dataio.api.services.deterministic_draft_service.generate_deterministic_draft",
        fake_generate_deterministic_draft,
    )

    result = service.generate_deterministic_draft_from_upload(
        csv_files=[UploadFile(filename="data.csv", file=io.BytesIO(b"a,b\n1,2\n"))],
        category_id="CS",
        collection_id="CS0007",
        data_owner_name="DAHD",
        created_by="engineer@artpark.in",
        curator_input=_valid_curator_input(),
    )

    assert recorded["csv_paths"] == ["/tmp/csv-abc.csv"]
    assert recorded["curator_input"].datasetDescription == "desc"
    assert result["draft_id"] == "new-draft-id"


def test_generate_deterministic_draft_from_upload_rejects_invalid_curator_input(monkeypatch):
    service = _make_service()

    with pytest.raises(HTTPException) as exc_info:
        service.generate_deterministic_draft_from_upload(
            csv_files=[],
            category_id="CS",
            collection_id="CS0007",
            data_owner_name="DAHD",
            created_by="engineer@artpark.in",
            curator_input={},  # missing every required field
        )
    assert exc_info.value.status_code == 400


def test_generate_deterministic_draft_from_upload_wraps_failures_as_502(monkeypatch):
    import io

    from fastapi import UploadFile

    service = _make_service()
    monkeypatch.setattr(service_module, "save_upload", lambda upload_file: "/tmp/csv-abc.csv")

    def failing_generate(**kwargs):
        raise RuntimeError("profiling blew up")

    monkeypatch.setattr(
        "dataio.api.services.deterministic_draft_service.generate_deterministic_draft",
        failing_generate,
    )

    with pytest.raises(HTTPException) as exc_info:
        service.generate_deterministic_draft_from_upload(
            csv_files=[UploadFile(filename="data.csv", file=io.BytesIO(b"a,b\n1,2\n"))],
            category_id="CS",
            collection_id="CS0007",
            data_owner_name="DAHD",
            created_by="engineer@artpark.in",
            curator_input=_valid_curator_input(),
        )
    assert exc_info.value.status_code == 502


def test_regenerate_draft_dispatches_to_deterministic_path_when_llm_model_id_is_none(monkeypatch):
    """A draft with llm_model_id=None was produced by the deterministic
    path (see generate_deterministic_draft_from_upload), so regenerating it
    must reconstruct a CuratorMetadataInput from the original draft_json and
    re-run generate_deterministic_draft - not the LLM's generate_draft.
    """
    service = _make_service()
    original = _fake_draft(
        llm_model_id=None,
        raw_dataset_id="CSRDS0016",
        draft_json={
            "datasetTitle": "Foo",
            "datasetDescription": "desc",
            "source": ["https://example.com"],
            "references": [],
            "tags": {"concept": ["livestock"], "epiType": []},
            "spatialCoverage": "India",
            "spatialResolution": "state",
            "temporalCoverage": "1997-2019",
            "temporalResolution": "annual",
            "updateFrequency": "annual",
            "joinKeys": ["state.ID", "year"],
            # A region-history comment must NOT round-trip verbatim -
            # generate_deterministic_draft recomputes and re-appends those
            # fresh, so carrying the old one forward would duplicate it.
            "comments": ["curator note", "[region history] Telangana was carved out of Andhra Pradesh..."],
            "tables": {"main": {"description": "d", "data_dictionary": {"count": {"type": "int"}}}},
        },
    )
    monkeypatch.setattr(service_module.database, "get_manifest_draft", lambda draft_id: original)
    monkeypatch.setattr(
        service_module.database, "update_manifest_draft_status",
        lambda draft_id, status, **kw: None,
    )
    monkeypatch.setattr(
        "dataio.api.services.draft_service.decode_csv_paths",
        lambda source_csv_path, table_names=None: {"main": "foo.csv"},
    )

    recorded = {}

    def fake_generate_deterministic_draft(**kwargs):
        recorded.update(kwargs)
        return SimpleNamespace(model_dump=lambda: {"draft_id": "new-draft-id", "status": "pending"})

    monkeypatch.setattr(
        "dataio.api.services.deterministic_draft_service.generate_deterministic_draft",
        fake_generate_deterministic_draft,
    )

    result = service.regenerate_draft(str(original.draft_id), "reviewer@artpark.in")

    assert recorded["raw_dataset_id"] == "CSRDS0016"
    assert recorded["curator_input"].comments == ["curator note"]
    assert recorded["curator_input"].joinKeyColumns == ["state.ID", "year"]
    assert recorded["curator_input"].tableDescriptions == {"main": "d"}
    assert recorded["curator_input"].datasetTitle == "Foo"
    assert result["draft_id"] == "new-draft-id"


def test_classify_columns_splits_fixed_and_needs_description():
    service = _make_service()

    result = service.classify_columns(
        column_names=[
            "state.ID", "state.name", "state.lgd_code", "sourceDocument", "species", "count",
        ],
    )

    assert set(result["fixed"]) == {"state.ID", "state.name", "state.lgd_code", "sourceDocument"}
    assert set(result["needsDescription"]) == {"species", "count"}


def test_infer_dataset_coverage_suggests_all_three_fields_from_real_csv(tmp_path):
    csv_path = tmp_path / "main.csv"
    csv_path.write_text(
        "state.ID,state.name,year,count\n"
        "state_KA,Karnataka,1997,10\n"
        "state_KL,Kerala,2003,20\n"
        "state_KA,Karnataka,2019,30\n",
        encoding="utf-8",
    )
    service = _make_service()

    result = service.infer_dataset_coverage([str(csv_path)])

    assert result["spatialCoverage"] == "India"
    assert result["spatialResolution"] == "state"
    assert result["temporalCoverage"] == "1997, 2003, 2019"


def test_infer_dataset_coverage_combines_across_multiple_tables(tmp_path):
    coarse_csv = tmp_path / "coarse.csv"
    coarse_csv.write_text("state.name,year,count\nKarnataka,1997,10\n", encoding="utf-8")
    fine_csv = tmp_path / "fine.csv"
    fine_csv.write_text("district.name,year,count\nBengaluru,2019,5\n", encoding="utf-8")
    service = _make_service()

    result = service.infer_dataset_coverage([str(coarse_csv), str(fine_csv)])

    # finest resolution across both tables wins, and years union across both
    assert result["spatialResolution"] == "district"
    assert result["temporalCoverage"] == "1997, 2019"


def test_infer_dataset_coverage_returns_none_when_nothing_detected(tmp_path):
    csv_path = tmp_path / "plain.csv"
    csv_path.write_text("indicator,count\nfoo,1\n", encoding="utf-8")
    service = _make_service()

    result = service.infer_dataset_coverage([str(csv_path)])

    assert result == {"spatialCoverage": None, "spatialResolution": None, "temporalCoverage": None}


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
