from __future__ import annotations

import io
import logging
import os

from fastapi import UploadFile

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from dataio.api.models import VersionType
from dataio.api.services.admin_dataset_service import AdminDatasetService
from dataio.validate.reports.models import ValidationResult


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
datasetSlug: sample-manifest
datasetDescription: Example
source: Test
category: {ID: TS, name: Test}
collection: {ID: TS0001, name: Tests}
datasetKind: tabular
datasetTables:
  sample:
    dataDictionary:
      year:
        type: date
        format: YYYY
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
    assert recorded["request"].validate_data is False
