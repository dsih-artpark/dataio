from __future__ import annotations

import io
import logging
import os
from types import SimpleNamespace

from fastapi import HTTPException, UploadFile

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from dataio.api.models import VersionType
from dataio.api.services.admin_dataset_service import AdminDatasetService
from dataio.api.services.web_admin_service import WebAdminService
from dataio.validate.reports.models import Finding, ValidationResult
from dataio.api.database import functions as database_functions
from dataio.api.models import DatasetDocumentationUpdate, DatasetUpdate


def test_upsert_dataset_manifest_updates_filestore_and_db(monkeypatch):
    service = object.__new__(AdminDatasetService)
    service.logger = logging.getLogger(__name__)

    recorded = {}

    class FilestoreStub:
        def upload_manifest(self, dataset_id, version_type, manifest_yaml, manifest_json):
            recorded["filestore"] = {
                "dataset_id": dataset_id,
                "bucket_type": version_type.value,
                "manifest_yaml": manifest_yaml,
                "manifest_json": manifest_json,
            }

        def get_tabular_validation_sources(self, dataset_id, version_type):
            recorded["validation_sources"] = {
                "dataset_id": dataset_id,
                "bucket_type": version_type.value,
            }
            return {"sample": "year\n2024\n"}

    class ValidatorStub:
        def validate(self, request):
            recorded["request"] = request
            return ValidationResult(dataset_kind=request.dataset_kind.value)

    service.filestore_service = FilestoreStub()
    service.validation_service = ValidatorStub()
    service.refresh_dataset_documentation_cache = lambda _dataset_id: recorded.setdefault("refreshed", True)

    monkeypatch.setattr(
        "dataio.api.services.admin_dataset_service.database.check_if_dataset_exists",
        lambda _dataset_id: True,
    )

    def fake_update(dataset_id, *, manifest_yaml, manifest_json, updated_by):
        recorded["db"] = {
            "dataset_id": dataset_id,
            "manifest_yaml": manifest_yaml,
            "manifest_json": manifest_json,
            "updated_by": updated_by,
        }

    monkeypatch.setattr(
        "dataio.api.services.admin_dataset_service.database.update_dataset_manifest_cache",
        fake_update,
    )

    manifest_text = """
metadataSpecVersion: v2
datasetTitle: Sample Manifest
datasetSlug: ts0001ds0001-sample-manifest
datasetDescription: Example
source: Test
category: {ID: TS, name: Test}
collection: {ID: TS0001, name: Tests}
datasetID: TS0001DS0001
datasetKind: tabular
datasetTables:
  sample:
    dataDictionary:
      year:
        type: date
        format: "%Y"
        nullable: false
"""

    upload = UploadFile(
        filename="manifest.yaml",
        file=io.BytesIO(manifest_text.encode("utf-8")),
    )
    result = service.upsert_dataset_manifest(
        "TS0001DS0001",
        VersionType.STANDARDISED,
        upload,
        "admin@example.com",
    )

    assert result["message"] == "Manifest uploaded successfully"
    assert recorded["filestore"]["dataset_id"] == "TS0001DS0001"
    assert recorded["db"]["updated_by"] == "admin@example.com"
    assert recorded["request"].validate_data is True
    assert recorded["request"].data_files == {"sample": "year\n2024\n"}


def test_upsert_dataset_manifest_rejects_when_stored_data_fails(monkeypatch):
    service = object.__new__(AdminDatasetService)
    service.logger = logging.getLogger(__name__)

    class FilestoreStub:
        def get_tabular_validation_sources(self, _dataset_id, _version_type):
            return {"sample": "year\nnot-a-year\n"}

    class ValidatorStub:
        def validate(self, request):
            result = ValidationResult(dataset_kind=request.dataset_kind.value)
            result.add_finding(
                Finding(
                    severity="error",
                    code="type_validation_failed",
                    message="Stored data does not match manifest",
                    table="sample",
                    field="year",
                )
            )
            return result

    service.filestore_service = FilestoreStub()
    service.validation_service = ValidatorStub()

    monkeypatch.setattr(
        "dataio.api.services.admin_dataset_service.database.check_if_dataset_exists",
        lambda _dataset_id: True,
    )

    upload = UploadFile(
        filename="manifest.yaml",
        file=io.BytesIO(
            b"""
metadataSpecVersion: v2
datasetTitle: Sample Manifest
datasetSlug: ts0001ds0001-sample-manifest
datasetDescription: Example
source: Test
category: {ID: TS, name: Test}
collection: {ID: TS0001, name: Tests}
datasetID: TS0001DS0111
datasetKind: tabular
datasetTables:
  sample:
    dataDictionary:
      year:
        type: date
        format: "%Y"
        nullable: false
"""
        ),
    )

    try:
        service.upsert_dataset_manifest(
            "TS0001DS0001",
            VersionType.STANDARDISED,
            upload,
            "admin@example.com",
        )
    except HTTPException as exc:
        assert exc.status_code == 400
        assert exc.detail["message"] == "Manifest and stored data validation failed"
        assert exc.detail["findings"][0]["code"] == "type_validation_failed"
    else:
        raise AssertionError("Expected HTTPException to be raised")


def test_check_dataset_documentation_sync_requires_dataset_id():
    service = object.__new__(AdminDatasetService)
    service.logger = logging.getLogger(__name__)
    service.filestore_service = SimpleNamespace(bucket="test-bucket")

    class SessionStub:
        def rollback(self):
            return None

        def close(self):
            return None

    service.db_session_factory = lambda: SessionStub()

    try:
        service.check_dataset_documentation_sync()
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "Dataset ID is required" in str(exc.detail)
    else:
        raise AssertionError("Expected HTTPException to be raised")


def test_sync_dataset_documentation_requires_dataset_id():
    service = object.__new__(AdminDatasetService)
    service.logger = logging.getLogger(__name__)
    service.filestore_service = SimpleNamespace(bucket="test-bucket")

    class SessionStub:
        def rollback(self):
            return None

        def close(self):
            return None

    service.db_session_factory = lambda: SessionStub()

    try:
        service.sync_dataset_documentation()
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "Dataset ID is required" in str(exc.detail)
    else:
        raise AssertionError("Expected HTTPException to be raised")


def test_update_dataset_rejects_duplicate_dataset_id(monkeypatch):
    dataset = SimpleNamespace(
        ds_id="TS0001DS0001",
        collection=SimpleNamespace(collection_id="TS0001"),
        raw_datasets=[],
        tags=[],
    )

    class QueryStub:
        def __init__(self, result):
            self.result = result

        def options(self, *_args, **_kwargs):
            return self

        def filter(self, *_args, **_kwargs):
            return self

        def first(self):
            return self.result

    class SessionStub:
        def query(self, *_args, **_kwargs):
            return QueryStub(dataset)

        def close(self):
            return None

    monkeypatch.setattr(database_functions, "Session", lambda: SessionStub())
    monkeypatch.setattr(
        database_functions,
        "check_if_dataset_exists",
        lambda dataset_id: dataset_id == "CUSTOM-ID-2",
    )

    try:
        database_functions.update_dataset(
            "TS0001DS0001",
            DatasetUpdate(ds_id="CUSTOM-ID-2"),
        )
    except ValueError as exc:
        assert str(exc) == "Dataset with ID CUSTOM-ID-2 already exists"
    else:
        raise AssertionError("Expected ValueError to be raised")


def test_update_dataset_documentation_writes_filestore_and_refreshes_cache(monkeypatch):
    service = object.__new__(WebAdminService)
    service.logger = logging.getLogger(__name__)
    service._require_admin = lambda _user: None

    recorded = {}

    class QueryStub:
        def filter(self, *_args, **_kwargs):
            return self

        def first(self):
            return SimpleNamespace(ds_id="TS0001DS0001")

    class SessionStub:
        def query(self, *_args, **_kwargs):
            return QueryStub()

        def close(self):
            return None

    class FilestoreStub:
        def upsert_dataset_readme(self, dataset_id, readme_md):
            recorded["readme"] = (dataset_id, readme_md)

        def upsert_dataset_metadata_json(self, dataset_id, metadata_json):
            recorded["metadata"] = (dataset_id, metadata_json)

    class AdminDatasetServiceStub:
        def __init__(self):
            self.filestore_service = FilestoreStub()

        def refresh_dataset_documentation_cache(self, dataset_id):
            recorded["refreshed"] = dataset_id

        def get_dataset_admin_detail(self, dataset_id):
            recorded["detail"] = dataset_id
            return {"ds_id": dataset_id}

    service.admin_dataset_service = AdminDatasetServiceStub()

    monkeypatch.setattr("dataio.api.services.web_admin_service.DBSession", lambda: SessionStub())

    result = service.update_dataset_documentation(
        SimpleNamespace(email="admin@example.com", is_admin=True),
        "TS0001DS0001",
        DatasetDocumentationUpdate(
            readme_md="# Updated",
            data_dictionary_json={"tables": {"main": {"data_dictionary": {}}}},
        ),
    )

    assert result == {"ds_id": "TS0001DS0001"}
    assert recorded["readme"] == ("TS0001DS0001", "# Updated")
    assert recorded["metadata"][0] == "TS0001DS0001"
    assert recorded["refreshed"] == "TS0001DS0001"
