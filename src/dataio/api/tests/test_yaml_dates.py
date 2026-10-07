from __future__ import annotations

import contextlib
import datetime
import io
import json
import logging
import os

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

import pytest
from fastapi import UploadFile

from dataio.api.models import VersionType
from dataio.api.services.admin_dataset_service import AdminDatasetService
from dataio.api.services.yaml_utils import stringify_yaml_dates
from dataio.validate.reports.models import ValidationResult


@pytest.fixture(autouse=True)
def _no_dataset_upload_lock(monkeypatch):
    """Manual uploads take a Postgres advisory lock; these unit tests have no DB."""
    monkeypatch.setattr(
        "dataio.api.services.admin_dataset_service.database.dataset_upload_lock",
        lambda ds_id, exclusive: contextlib.nullcontext(),
    )


def test_stringify_yaml_dates_converts_nested_values_and_keys():
    value = {
        "temporalCoverage": {"start": datetime.date(2019, 6, 30)},
        "events": [datetime.datetime(2024, 1, 1, 10, 0)],
        datetime.date(2020, 1, 1): "keyed by date",
        "count": 3,
    }

    result = stringify_yaml_dates(value)

    assert result == {
        "temporalCoverage": {"start": "2019-06-30"},
        "events": ["2024-01-01T10:00:00"],
        "2020-01-01": "keyed by date",
        "count": 3,
    }
    json.dumps(result)


def test_upsert_dataset_manifest_persists_json_safe_manifest_with_unquoted_dates(monkeypatch):
    """An unquoted ISO date in a manifest used to reach json.dumps as a
    datetime.date, failing after manifest.yaml was already written."""
    service = object.__new__(AdminDatasetService)
    service.logger = logging.getLogger(__name__)
    recorded = {}

    class FilestoreStub:
        def upload_manifest(self, dataset_id, version_type, manifest_yaml, manifest_json):
            recorded["manifest_json_body"] = json.dumps(manifest_json)

        def get_tabular_validation_sources(self, dataset_id, version_type):
            return {"sample": "year\n2024\n"}

    class ValidatorStub:
        def validate(self, request):
            return ValidationResult(dataset_kind=request.dataset_kind.value)

    service.filestore_service = FilestoreStub()
    service.validation_service = ValidatorStub()
    service.refresh_dataset_documentation_cache = lambda _dataset_id: None
    monkeypatch.setattr(
        "dataio.api.services.admin_dataset_service.database.check_if_dataset_exists",
        lambda _dataset_id: True,
    )
    monkeypatch.setattr(
        "dataio.api.services.admin_dataset_service.database.update_dataset_manifest_cache",
        lambda dataset_id, *, manifest_yaml, manifest_json, updated_by: recorded.setdefault(
            "db_manifest_json", json.dumps(manifest_json)
        ),
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
temporalCoverage: {start: 2019-06-30, end: 2020-03-31}
datasetTables:
  sample:
    dataDictionary:
      year:
        type: date
        format: "%Y"
        nullable: false
"""
    upload = UploadFile(filename="manifest.yaml", file=io.BytesIO(manifest_text.encode("utf-8")))

    result = service.upsert_dataset_manifest(
        "TS0001DS0001", VersionType.STANDARDISED, upload, "admin@example.com"
    )

    expected = {"start": "2019-06-30", "end": "2020-03-31"}
    assert result["manifest_json"]["temporalCoverage"] == expected
    assert '"start": "2019-06-30"' in recorded["manifest_json_body"]
    assert "db_manifest_json" in recorded
