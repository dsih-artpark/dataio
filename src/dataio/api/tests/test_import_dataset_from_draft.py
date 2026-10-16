from __future__ import annotations

import contextlib
import json
import os
from io import BytesIO
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
from dataio.api.database.functions import DatasetBusy
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
        imported_at=None,
        imported_by=None,
    )


VALID_INFO = (
    "spatial_resolution: STATE\n"
    "temporal_resolution: YEAR\n"
    "temporal_coverage_start_date: '1997-01-01'\n"
    "temporal_coverage_end_date: '2019-12-31'\n"
)


@pytest.fixture(autouse=True)
def _dataset_does_not_exist_yet(monkeypatch):
    monkeypatch.setattr(
        "dataio.api.services.web_admin_service.database.check_if_dataset_exists", lambda ds_id: False
    )


@pytest.fixture(autouse=True)
def import_records(monkeypatch):
    """Stubs the draft's import claim/outcome writes and records the calls."""
    records = {"claimed": [], "finished": []}
    monkeypatch.setattr(
        "dataio.api.services.web_admin_service.database.claim_manifest_draft_import",
        lambda draft_id: records["claimed"].append(draft_id),
    )
    monkeypatch.setattr(
        "dataio.api.services.web_admin_service.database.finish_manifest_draft_import",
        lambda draft_id, **outcome: records["finished"].append({"draft_id": draft_id, **outcome}),
    )
    return records


def _service():
    return WebAdminService()


def test_import_dataset_from_draft_happy_path(monkeypatch, tmp_path, import_records):
    csv_path = tmp_path / "foo.csv"
    csv_path.write_bytes(b"a,b\n1,2\n")
    draft = _fake_draft(status="approved", source_csv_path=json.dumps({"foo": str(csv_path)}))

    service = _service()
    monkeypatch.setattr(service.draft_review_service, "_get_draft_or_404", lambda draft_id: draft)
    monkeypatch.setattr(
        service.draft_review_service,
        "generate_info_yaml",
        lambda draft_id, access_level: {
            "info_yaml": f"ds_id: {draft.dataset_id}\naccess_level: {access_level}\n{VALID_INFO}"
        },
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
    assert import_records["claimed"] == ["draft-1"]
    [finished] = import_records["finished"]
    assert finished["succeeded"] is True
    assert finished["imported_by"] == ADMIN_USER.email
    assert finished["result"]["status"] == "succeeded"
    assert finished["result"]["uploaded_tables"] == ["foo"]


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
        service.draft_review_service,
        "generate_info_yaml",
        lambda draft_id, access_level: {"info_yaml": f"ds_id: x\n{VALID_INFO}"},
    )

    def fail_if_called(*a, **kw):
        raise AssertionError("import_dataset_package must not be called when a CSV can't be read")

    monkeypatch.setattr(service, "import_dataset_package", fail_if_called)

    with pytest.raises(HTTPException) as exc_info:
        service.import_dataset_from_draft(ADMIN_USER, "draft-1", "NONE", VersionType.STANDARDISED)

    assert exc_info.value.status_code == 400
    assert "foo" in exc_info.value.detail


def _fail_if_imported(*a, **kw):
    raise AssertionError("import_dataset_package must not run when pre-checks fail")


def test_import_dataset_from_draft_refuses_existing_dataset_before_any_write(monkeypatch, tmp_path):
    draft = _fake_draft(status="approved", source_csv_path=json.dumps({"foo": str(tmp_path / "foo.csv")}))
    service = _service()
    monkeypatch.setattr(service.draft_review_service, "_get_draft_or_404", lambda draft_id: draft)
    monkeypatch.setattr(
        "dataio.api.services.web_admin_service.database.check_if_dataset_exists", lambda ds_id: True
    )
    monkeypatch.setattr(service, "import_dataset_package", _fail_if_imported)

    with pytest.raises(HTTPException) as exc_info:
        service.import_dataset_from_draft(ADMIN_USER, "draft-1", "VIEW", VersionType.STANDARDISED)

    assert exc_info.value.status_code == 409


@pytest.mark.parametrize(
    "info_yaml, expected_problem",
    [
        # free-text resolution from the intake form / field inference
        (VALID_INFO.replace("STATE", "TALUK"), "spatial_resolution"),
        # no %Y column with values -> no temporal_resolution
        (VALID_INFO.replace("temporal_resolution: YEAR\n", ""), "temporal_resolution"),
        # a year column with a blank cell profiled as float
        (VALID_INFO.replace("'1997-01-01'", "'1997.0'"), "temporal_coverage_start_date"),
    ],
)
def test_import_dataset_from_draft_rejects_values_create_dataset_would_fail_on(
    monkeypatch, tmp_path, info_yaml, expected_problem
):
    csv_path = tmp_path / "foo.csv"
    csv_path.write_bytes(b"a,b\n1,2\n")
    draft = _fake_draft(status="approved", source_csv_path=json.dumps({"foo": str(csv_path)}))
    service = _service()
    monkeypatch.setattr(service.draft_review_service, "_get_draft_or_404", lambda draft_id: draft)
    monkeypatch.setattr(
        service.draft_review_service, "generate_info_yaml", lambda draft_id, access_level: {"info_yaml": info_yaml}
    )
    monkeypatch.setattr(service, "import_dataset_package", _fail_if_imported)

    with pytest.raises(HTTPException) as exc_info:
        service.import_dataset_from_draft(ADMIN_USER, "draft-1", "VIEW", VersionType.STANDARDISED)

    assert exc_info.value.status_code == 400
    assert any(expected_problem in problem for problem in exc_info.value.detail["problems"])


def _approved_draft_service(monkeypatch, tmp_path, draft=None):
    csv_path = tmp_path / "foo.csv"
    csv_path.write_bytes(b"a,b\n1,2\n")
    draft = draft or _fake_draft(status="approved", source_csv_path=json.dumps({"foo": str(csv_path)}))
    service = _service()
    monkeypatch.setattr(service.draft_review_service, "_get_draft_or_404", lambda draft_id: draft)
    monkeypatch.setattr(
        service.draft_review_service,
        "generate_info_yaml",
        lambda draft_id, access_level: {"info_yaml": f"ds_id: {draft.dataset_id}\n{VALID_INFO}"},
    )
    return service


def test_import_dataset_from_draft_refuses_an_already_uploaded_draft(monkeypatch, tmp_path, import_records):
    from datetime import datetime

    draft = _fake_draft(status="approved", source_csv_path=json.dumps({"foo": str(tmp_path / "foo.csv")}))
    draft.imported_at = datetime(2026, 10, 7, 9, 30)
    draft.imported_by = "curator@example.com"
    service = _approved_draft_service(monkeypatch, tmp_path, draft)
    monkeypatch.setattr(service, "import_dataset_package", _fail_if_imported)

    with pytest.raises(HTTPException) as exc_info:
        service.import_dataset_from_draft(ADMIN_USER, "draft-1", "VIEW", VersionType.STANDARDISED)

    assert exc_info.value.status_code == 409
    assert "curator@example.com" in exc_info.value.detail
    assert import_records["claimed"] == []


def test_import_dataset_from_draft_refuses_while_another_upload_holds_the_draft(monkeypatch, tmp_path, import_records):
    from dataio.api.database.functions import DraftImportConflict

    service = _approved_draft_service(monkeypatch, tmp_path)

    def claim_conflict(draft_id):
        raise DraftImportConflict("An upload of manifest draft draft-1 is already running")

    monkeypatch.setattr("dataio.api.services.web_admin_service.database.claim_manifest_draft_import", claim_conflict)
    monkeypatch.setattr(service, "import_dataset_package", _fail_if_imported)

    with pytest.raises(HTTPException) as exc_info:
        service.import_dataset_from_draft(ADMIN_USER, "draft-1", "VIEW", VersionType.STANDARDISED)

    assert exc_info.value.status_code == 409
    assert "already running" in exc_info.value.detail
    assert import_records["finished"] == []


def test_import_dataset_from_draft_releases_the_claim_and_records_a_failed_upload(monkeypatch, tmp_path, import_records):
    service = _approved_draft_service(monkeypatch, tmp_path)

    def failing_import(*a, **kw):
        raise HTTPException(status_code=500, detail={"message": "Import of CS0007DS0999 failed (S3 down)."})

    monkeypatch.setattr(service, "import_dataset_package", failing_import)

    with pytest.raises(HTTPException) as exc_info:
        service.import_dataset_from_draft(ADMIN_USER, "draft-1", "VIEW", VersionType.STANDARDISED)

    assert exc_info.value.status_code == 500
    assert import_records["claimed"] == ["draft-1"]
    [finished] = import_records["finished"]
    assert finished["succeeded"] is False
    assert finished["result"]["status"] == "failed"
    assert finished["result"]["message"] == "Import of CS0007DS0999 failed (S3 down)."


def test_import_dataset_from_draft_still_succeeds_if_recording_the_outcome_fails(monkeypatch, tmp_path):
    service = _approved_draft_service(monkeypatch, tmp_path)
    monkeypatch.setattr(
        service,
        "import_dataset_package",
        lambda *a, **kw: {"dataset_id": "CS0007DS0999", "bucket_type": "STANDARDISED", "uploaded_tables": ["foo"], "manifest_uploaded": True},
    )

    def broken_finish(draft_id, **outcome):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr("dataio.api.services.web_admin_service.database.finish_manifest_draft_import", broken_finish)

    result = service.import_dataset_from_draft(ADMIN_USER, "draft-1", "VIEW", VersionType.STANDARDISED)

    assert result["dataset_id"] == "CS0007DS0999"


# --- import_dataset_package: undo on failure ---------------------------------

DS_ID = "CS0007DS0999"
RDS_ID = "CSRDS0099"


class _PackageImport:
    """WebAdminService with every write of import_dataset_package stubbed.
    Records each call in order; a step named in fail_at raises instead."""

    def __init__(self, monkeypatch, *, existing_raw=None, fail_at=(), s3_has_files=False,
                 ds_reservation=True, rds_reservation=True, lock_busy=False):
        self.calls = []
        self.fail_at = set(fail_at)
        self.locks = []
        service = _service()
        self.service = service
        db = "dataio.api.services.web_admin_service.database"

        monkeypatch.setattr(service, "preview_dataset_package_import", lambda *a, **kw: {
            "can_import": True,
            "findings": [],
            "raw_dataset": {"rds_id": RDS_ID, "title": "New raw title", "source": "New source"},
            "dataset": {
                "ds_id": DS_ID, "title": "Foo", "collection_id": "CS0007", "data_owner_name": "Owner",
                "spatial_resolution": "STATE", "temporal_resolution": "YEAR", "access_level": "VIEW",
            },
            "tables": [{"table_name": "foo", "table_metadata": {"table_name": "foo", "data_dictionary": {}}}],
            "manifest_yaml": "datasetID: CS0007DS0999\n",
        })
        monkeypatch.setattr(f"{db}.get_raw_dataset_by_identifier", lambda rds_id: existing_raw)

        @contextlib.contextmanager
        def upload_lock(ds_id, *, exclusive):
            if lock_busy:
                raise DatasetBusy(f"Files are being uploaded to dataset {ds_id} right now")
            self.locks.append(("acquired", ds_id, exclusive))
            try:
                yield
            finally:
                self.released_after = self.names()
                self.locks.append(("released", ds_id, exclusive))

        monkeypatch.setattr(f"{db}.dataset_upload_lock", upload_lock)
        monkeypatch.setattr(
            f"{db}.get_reserved_dataset_id",
            lambda ds_id: SimpleNamespace(collection_id="CS0007", note="Reserved for draft", reserved_by="curator@example.com")
            if ds_reservation else None,
        )
        monkeypatch.setattr(
            f"{db}.get_reserved_raw_dataset_id",
            lambda rds_id: SimpleNamespace(category_id="CS", note="Reserved for draft", reserved_by="curator@example.com")
            if rds_reservation else None,
        )

        filestore = service.admin_dataset_service.filestore_service
        monkeypatch.setattr(filestore, "dataset_has_objects", lambda ds_id: s3_has_files)
        admin = service.admin_dataset_service
        for name in ("update_raw_dataset", "create_raw_dataset", "create_dataset", "create_dataset_table", "upsert_dataset_manifest"):
            monkeypatch.setattr(admin, name, self._step(name))
        monkeypatch.setattr(filestore, "delete_dataset", self._step("undo:delete_s3"))
        monkeypatch.setattr(f"{db}.delete_dataset", self._step("undo:delete_dataset"))
        monkeypatch.setattr(f"{db}.delete_raw_dataset", self._step("undo:delete_raw_dataset"))
        monkeypatch.setattr(f"{db}.create_reserved_dataset_id", self._step("undo:reserve_dataset_id"))
        monkeypatch.setattr(f"{db}.create_reserved_raw_dataset_id", self._step("undo:reserve_raw_dataset_id"))
        monkeypatch.setattr(f"{db}.update_raw_dataset", self._step("undo:restore_raw_dataset"))

    def _step(self, name):
        def step(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            if name in self.fail_at:
                raise HTTPException(status_code=500, detail=f"{name} failed")
        return step

    def names(self):
        return [name for name, _, _ in self.calls]

    def call(self, name):
        """(args, kwargs) of the first call to step `name`."""
        return next((args, kwargs) for step, args, kwargs in self.calls if step == name)

    def run(self):
        info = SimpleNamespace(file=None)
        csv = SimpleNamespace(filename="foo.csv", file=BytesIO(b"a,b\n1,2\n"))
        return self.service.import_dataset_package(ADMIN_USER, info, info, [csv])


def test_import_dataset_package_happy_path_undoes_nothing(monkeypatch):
    package = _PackageImport(monkeypatch)

    result = package.run()

    assert result["uploaded_tables"] == ["foo"]
    assert package.names() == ["create_raw_dataset", "create_dataset", "create_dataset_table", "upsert_dataset_manifest"]


def test_import_dataset_package_refuses_when_s3_already_has_files(monkeypatch):
    package = _PackageImport(monkeypatch, s3_has_files=True)

    with pytest.raises(HTTPException) as exc_info:
        package.run()

    assert exc_info.value.status_code == 409
    assert package.calls == []


def test_import_dataset_package_undoes_everything_when_a_table_upload_fails(monkeypatch):
    package = _PackageImport(monkeypatch, fail_at={"create_dataset_table"})

    with pytest.raises(HTTPException) as exc_info:
        package.run()

    assert package.names() == [
        "create_raw_dataset", "create_dataset", "create_dataset_table",
        "undo:delete_s3", "undo:delete_dataset", "undo:reserve_dataset_id",
        "undo:delete_raw_dataset", "undo:reserve_raw_dataset_id",
    ]
    assert package.call("undo:reserve_dataset_id") == (
        (DS_ID,), {"collection_id": "CS0007", "note": "Reserved for draft", "reserved_by": "curator@example.com"}
    )
    detail = exc_info.value.detail
    assert exc_info.value.status_code == 500
    assert detail["undone"] is True
    assert "create_dataset_table failed" in detail["message"]
    assert "retry" in detail["message"]


def test_import_dataset_package_undoes_when_the_manifest_upload_fails(monkeypatch):
    package = _PackageImport(monkeypatch, fail_at={"upsert_dataset_manifest"})

    with pytest.raises(HTTPException):
        package.run()

    assert "undo:delete_s3" in package.names()
    assert "undo:delete_dataset" in package.names()


def test_import_dataset_package_never_deletes_a_dataset_it_did_not_create(monkeypatch):
    # e.g. another admin created the same ds_id a moment earlier
    package = _PackageImport(monkeypatch, fail_at={"create_dataset"})

    with pytest.raises(HTTPException):
        package.run()

    assert package.names() == [
        "create_raw_dataset", "create_dataset", "undo:delete_raw_dataset", "undo:reserve_raw_dataset_id",
    ]


def test_import_dataset_package_restores_an_existing_raw_dataset_instead_of_deleting_it(monkeypatch):
    existing = SimpleNamespace(rds_id=RDS_ID, title="Old raw title", source="Old source")
    package = _PackageImport(monkeypatch, existing_raw=existing, fail_at={"create_dataset_table"})

    with pytest.raises(HTTPException):
        package.run()

    names = package.names()
    assert "undo:delete_raw_dataset" not in names
    assert "undo:reserve_raw_dataset_id" not in names
    (restored_id, restored_fields), _ = package.call("undo:restore_raw_dataset")
    assert restored_id == RDS_ID
    assert restored_fields.title == "Old raw title"
    assert restored_fields.source == "Old source"


def test_import_dataset_package_does_not_invent_reservations_that_did_not_exist(monkeypatch):
    package = _PackageImport(monkeypatch, fail_at={"create_dataset_table"}, ds_reservation=False, rds_reservation=False)

    with pytest.raises(HTTPException):
        package.run()

    assert "undo:reserve_dataset_id" not in package.names()
    assert "undo:reserve_raw_dataset_id" not in package.names()


def test_import_dataset_package_reports_what_could_not_be_undone(monkeypatch):
    package = _PackageImport(monkeypatch, fail_at={"create_dataset_table", "undo:delete_s3"})

    with pytest.raises(HTTPException) as exc_info:
        package.run()

    detail = exc_info.value.detail
    assert detail["undone"] is False
    assert detail["leftovers"] == [f"delete S3 files of dataset {DS_ID}"]
    assert "clean up by hand" in detail["message"]
    # the remaining undo steps still ran
    assert "undo:delete_dataset" in package.names()
    assert "undo:delete_raw_dataset" in package.names()


def test_import_dataset_package_keeps_validation_findings_in_the_error(monkeypatch):
    package = _PackageImport(monkeypatch)

    def invalid_manifest(*a, **kw):
        raise HTTPException(status_code=400, detail={"message": "Manifest and stored data validation failed", "findings": [{"code": "x"}]})

    monkeypatch.setattr(package.service.admin_dataset_service, "upsert_dataset_manifest", invalid_manifest)

    with pytest.raises(HTTPException) as exc_info:
        package.run()

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["findings"] == [{"code": "x"}]
    assert exc_info.value.detail["undone"] is True


def test_import_dataset_package_holds_the_dataset_upload_lock_for_the_whole_import(monkeypatch):
    package = _PackageImport(monkeypatch, fail_at={"create_dataset_table"})

    with pytest.raises(HTTPException):
        package.run()

    # taken exclusively before any write, released only after the undo ran
    assert package.locks == [("acquired", DS_ID, True), ("released", DS_ID, True)]
    assert "undo:delete_s3" in package.released_after
    # the import's own uploads don't try to take the lock it already holds
    _, table_kwargs = package.call("create_dataset_table")
    assert table_kwargs == {"hold_upload_lock": False}


def test_import_dataset_package_refuses_while_files_are_being_uploaded_to_the_dataset(monkeypatch):
    package = _PackageImport(monkeypatch, lock_busy=True)

    with pytest.raises(HTTPException) as exc_info:
        package.run()

    assert exc_info.value.status_code == 409
    assert package.calls == []


ADMIN_DB = "dataio.api.services.admin_dataset_service.database"


@pytest.mark.parametrize("upload", ["create_dataset_table", "upsert_dataset_manifest"])
def test_manual_uploads_take_the_shared_lock_and_refuse_during_an_import(monkeypatch, upload):
    from dataio.api.services.admin_dataset_service import AdminDatasetService

    service = AdminDatasetService()
    taken = []

    @contextlib.contextmanager
    def import_running(ds_id, *, exclusive):
        taken.append(exclusive)
        raise DatasetBusy(f"Dataset {ds_id} is being imported right now; try again in a few minutes.")
        yield  # pragma: no cover

    monkeypatch.setattr(f"{ADMIN_DB}.dataset_upload_lock", import_running)
    monkeypatch.setattr(service, f"_{upload}", lambda *a, **kw: pytest.fail("uploaded during an import"))
    args = (DS_ID, VersionType.STANDARDISED, None, None if upload == "create_dataset_table" else "a@b.c")

    with pytest.raises(HTTPException) as exc_info:
        getattr(service, upload)(*args)

    assert exc_info.value.status_code == 409
    assert "being imported" in exc_info.value.detail
    assert taken == [False]  # shared, so manual uploads don't block each other


# --- import_dataset_package(replace_existing=True) ------------------------------

ARCHIVE_PREFIX = "archive/filestore/STANDARDISED/CS0007DS0999/20261015T000000Z"


def _current_dataset():
    return SimpleNamespace(
        title="Old title",
        data_owner=SimpleNamespace(name="Old owner"),
        description="Old description",
        spatial_coverage_region_id="state_29",
        spatial_resolution=SimpleNamespace(value="DISTRICT"),
        temporal_coverage_start_date=None,
        temporal_coverage_end_date=None,
        temporal_resolution=SimpleNamespace(value="YEAR"),
        additional_metadata=None,
        tags=[SimpleNamespace(tag_name="old-tag")],
        raw_datasets=[SimpleNamespace(rds_id="CSRDS0001")],
        access_level=SimpleNamespace(value="VIEW"),
    )


class _PackageReplace(_PackageImport):
    """_PackageImport for replace_existing=True, with the replace-only steps stubbed."""

    def __init__(self, monkeypatch, *, current=True, fail_at=(), **kwargs):
        super().__init__(monkeypatch, fail_at=fail_at, **kwargs)
        db = "dataio.api.services.web_admin_service.database"
        monkeypatch.setattr(f"{db}.get_dataset", lambda ds_id: _current_dataset() if current else None)
        filestore = self.service.admin_dataset_service.filestore_service
        admin = self.service.admin_dataset_service

        def archive(ds_id, bucket_type, stamp):
            self.calls.append(("archive", (ds_id, bucket_type, stamp), {}))
            if "archive" in self.fail_at:
                raise RuntimeError("S3 copy failed")
            return ARCHIVE_PREFIX, ["foo.csv", "metadata.json"]

        monkeypatch.setattr(filestore, "archive_dataset_version", archive)
        monkeypatch.setattr(filestore, "delete_dataset_version", self._step("delete_version"))
        monkeypatch.setattr(filestore, "restore_dataset_version", self._step("undo:restore_files"))
        monkeypatch.setattr(admin, "update_dataset", self._step("update_dataset"))
        monkeypatch.setattr(admin, "refresh_dataset_documentation_cache", self._step("undo:refresh_cache"))
        monkeypatch.setattr(f"{db}.update_dataset", self._step("undo:restore_dataset"))

    def run(self):
        info = SimpleNamespace(file=None)
        csv = SimpleNamespace(filename="foo.csv", file=BytesIO(b"a,b\n1,2\n"))
        return self.service.import_dataset_package(ADMIN_USER, info, info, [csv], replace_existing=True)


def test_replace_archives_then_swaps_files_and_keeps_the_dataset(monkeypatch):
    package = _PackageReplace(monkeypatch, s3_has_files=True)

    result = package.run()

    assert package.names() == [
        "archive", "create_raw_dataset", "update_dataset", "delete_version",
        "create_dataset_table", "upsert_dataset_manifest",
    ]
    assert "create_dataset" not in package.names()
    assert result["replaced"] is True
    assert result["archive_prefix"] == ARCHIVE_PREFIX
    assert result["archived_files"] == ["foo.csv", "metadata.json"]
    assert result["uploaded_tables"] == ["foo"]
    assert package.locks[0] == ("acquired", DS_ID, True)


def test_replace_updates_details_but_keeps_access_level_and_raw_links(monkeypatch):
    package = _PackageReplace(monkeypatch)

    package.run()

    (ds_id, update), _ = package.call("update_dataset")
    assert ds_id == DS_ID
    assert update.title == "Foo"
    assert update.data_owner_name == "Owner"
    assert "access_level" not in update.model_fields_set
    assert update.ds_id is None and update.collection_id is None
    assert update.raw_dataset_ids == ["CSRDS0001", RDS_ID]


def test_replace_refuses_a_dataset_that_does_not_exist(monkeypatch):
    package = _PackageReplace(monkeypatch, current=False)

    with pytest.raises(HTTPException) as exc_info:
        package.run()

    assert exc_info.value.status_code == 404
    assert package.calls == []


def test_replace_changes_nothing_when_archiving_fails(monkeypatch):
    package = _PackageReplace(monkeypatch, fail_at={"archive"})

    with pytest.raises(HTTPException) as exc_info:
        package.run()

    assert exc_info.value.status_code == 500
    assert "nothing was replaced" in exc_info.value.detail
    assert package.names() == ["archive"]


def test_replace_puts_everything_back_when_a_table_upload_fails(monkeypatch):
    package = _PackageReplace(monkeypatch, fail_at={"create_dataset_table"})

    with pytest.raises(HTTPException) as exc_info:
        package.run()

    assert package.names() == [
        "archive", "create_raw_dataset", "update_dataset", "delete_version", "create_dataset_table",
        "undo:restore_files", "undo:restore_dataset",
        "undo:delete_raw_dataset", "undo:reserve_raw_dataset_id",
        "undo:refresh_cache",
    ]
    assert package.call("undo:restore_files")[0] == (
        DS_ID, VersionType.STANDARDISED, ARCHIVE_PREFIX, ["foo.csv", "metadata.json"]
    )
    (ds_id, previous), _ = package.call("undo:restore_dataset")
    assert ds_id == DS_ID
    assert previous.title == "Old title"
    assert previous.tags == ["old-tag"]
    assert previous.raw_dataset_ids == ["CSRDS0001"]
    detail = exc_info.value.detail
    assert detail["undone"] is True
    assert "put back as it was" in detail["message"]


def test_replace_restores_an_existing_raw_dataset_and_reports_leftovers(monkeypatch):
    existing = SimpleNamespace(rds_id=RDS_ID, title="Old raw title", source="Old source")
    package = _PackageReplace(
        monkeypatch, existing_raw=existing, fail_at={"upsert_dataset_manifest", "undo:restore_files"}
    )

    with pytest.raises(HTTPException) as exc_info:
        package.run()

    assert "undo:delete_raw_dataset" not in package.names()
    (restored_id, restored_fields), _ = package.call("undo:restore_raw_dataset")
    assert (restored_id, restored_fields.title) == (RDS_ID, "Old raw title")
    detail = exc_info.value.detail
    assert detail["undone"] is False
    assert any("restore the files" in step for step in detail["leftovers"])
    assert ARCHIVE_PREFIX in detail["message"]


def test_replace_does_not_touch_files_or_details_when_the_raw_dataset_step_fails(monkeypatch):
    package = _PackageReplace(monkeypatch, fail_at={"create_raw_dataset"})

    with pytest.raises(HTTPException):
        package.run()

    names = package.names()
    assert "delete_version" not in names
    assert "undo:restore_files" not in names
    assert "undo:restore_dataset" not in names
    assert "undo:refresh_cache" not in names


def test_replace_plan_lists_what_will_be_archived_and_changed(monkeypatch):
    service = _service()
    monkeypatch.setattr(
        "dataio.api.services.web_admin_service.database.get_dataset", lambda ds_id: _current_dataset()
    )
    monkeypatch.setattr(
        service.admin_dataset_service.filestore_service,
        "list_dataset_version_files",
        lambda ds_id, bucket_type: ["keep.csv", "old.csv", "metadata.json"],
    )
    preview = {
        "dataset": {"ds_id": DS_ID},
        "tables": [{"table_name": "keep"}, {"table_name": "new"}],
    }

    plan = service._replace_plan(preview, VersionType.STANDARDISED)

    assert plan["current_files"] == ["keep.csv", "metadata.json", "old.csv"]
    assert plan["tables_replaced"] == ["keep"]
    assert plan["tables_added"] == ["new"]
    assert plan["tables_removed"] == ["old"]
    assert plan["access_level_kept"] == "VIEW"
    assert plan["archive_location"] == f"archive/filestore/STANDARDISED/{DS_ID}/<timestamp>/"


def test_replace_restores_files_when_the_delete_itself_fails_part_way(monkeypatch):
    package = _PackageReplace(monkeypatch, fail_at={"delete_version"})

    with pytest.raises(HTTPException) as exc_info:
        package.run()

    names = package.names()
    assert names.index("delete_version") < names.index("undo:restore_files")
    assert "undo:refresh_cache" in names
    assert exc_info.value.detail["undone"] is True


def test_replace_rollback_passes_every_field_so_empty_ones_are_restored(monkeypatch):
    # database.update_dataset writes fields by model_fields_set, so a previous
    # None (e.g. no temporal coverage) must be passed explicitly to restore it.
    package = _PackageReplace(monkeypatch, fail_at={"create_dataset_table"})

    with pytest.raises(HTTPException):
        package.run()

    (_, previous), _ = package.call("undo:restore_dataset")
    assert {"description", "temporal_coverage_start_date", "temporal_coverage_end_date",
            "additional_metadata"} <= previous.model_fields_set
    assert previous.temporal_coverage_start_date is None
