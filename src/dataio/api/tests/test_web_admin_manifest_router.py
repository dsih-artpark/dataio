from __future__ import annotations

import importlib
import io
import os
from types import SimpleNamespace

from fastapi.testclient import TestClient

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

app = importlib.import_module("dataio.api").app
get_current_web_user = importlib.import_module(
    "dataio.api.auth.jwt"
).get_current_web_user
WebAdminService = importlib.import_module("dataio.api.routers.web").WebAdminService


client = TestClient(app)


def test_web_admin_get_manifest():
    recorded = {}

    class WebAdminServiceStub:
        def get_dataset_manifest(self, user, dataset_id, bucket_type):
            recorded["user"] = user.email
            recorded["dataset_id"] = dataset_id
            recorded["bucket_type"] = bucket_type.value
            return {
                "dataset_id": dataset_id,
                "bucket_type": bucket_type.value,
                "manifest_yaml": "datasetKind: tabular\n",
                "manifest_json": {"datasetKind": "tabular"},
                "has_manifest": True,
                "manifest_updated_at": None,
                "manifest_updated_by": None,
            }

    app.dependency_overrides[get_current_web_user] = lambda: SimpleNamespace(
        email="admin@example.com",
        is_admin=True,
    )
    app.dependency_overrides[WebAdminService] = lambda: WebAdminServiceStub()

    response = client.get("/api/v1/web/admin/datasets/TS0001DS0001/STANDARDISED/manifest")

    assert response.status_code == 200
    assert response.json()["has_manifest"] is True
    assert recorded["dataset_id"] == "TS0001DS0001"

    app.dependency_overrides.clear()


def test_web_admin_validate_tabular_endpoint():
    recorded = {}

    class WebAdminServiceStub:
        def validate_dataset(
            self,
            user,
            dataset_kind,
            manifest_file,
            data_file=None,
            table_name=None,
            strict=False,
            extra_column_policy="warn",
        ):
            recorded["user"] = user.email
            recorded["dataset_kind"] = dataset_kind.value
            recorded["manifest_name"] = manifest_file.filename
            recorded["data_name"] = data_file.filename if data_file else None
            recorded["table_name"] = table_name
            recorded["strict"] = strict
            recorded["extra_column_policy"] = extra_column_policy
            return {
                "status": "pass",
                "dataset_kind": dataset_kind.value,
                "metadata_spec_version": "v2",
                "summary": {
                    "errors": 0,
                    "warnings": 0,
                    "infos": 0,
                    "rows_checked": 1,
                    "tables_checked": 1,
                },
                "findings": [],
                "inferred": {},
            }

    app.dependency_overrides[get_current_web_user] = lambda: SimpleNamespace(
        email="admin@example.com",
        is_admin=True,
    )
    app.dependency_overrides[WebAdminService] = lambda: WebAdminServiceStub()

    response = client.post(
        "/api/v1/web/admin/validate/tabular",
        files={
            "manifest_file": (
                "manifest.yaml",
                io.BytesIO(b"datasetKind: tabular\n"),
                "application/x-yaml",
            ),
            "table_file": ("sample.csv", io.BytesIO(b"year,value\n2024,1\n"), "text/csv"),
        },
        data={
            "table_name": "sample",
            "strict": "true",
            "extra_column_policy": "warn",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "pass"
    assert recorded["dataset_kind"] == "tabular"
    assert recorded["data_name"] == "sample.csv"
    assert recorded["table_name"] == "sample"
    assert recorded["strict"] is True

    app.dependency_overrides.clear()
