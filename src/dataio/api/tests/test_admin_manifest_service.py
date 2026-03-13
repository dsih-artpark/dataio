from __future__ import annotations

import io
import logging
import os

from fastapi import HTTPException, UploadFile

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from dataio.api.models import VersionType
from dataio.api.services.admin_dataset_service import AdminDatasetService
from dataio.validate.reports.models import Finding, ValidationResult


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
