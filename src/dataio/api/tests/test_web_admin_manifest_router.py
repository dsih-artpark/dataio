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
            deep_check=False,
            extra_column_policy="warn",
        ):
            recorded["user"] = user.email
            recorded["dataset_kind"] = dataset_kind.value
            recorded["manifest_name"] = manifest_file.filename
            recorded["data_name"] = data_file.filename if data_file else None
            recorded["table_name"] = table_name
            recorded["deep_check"] = deep_check
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
            "deep_check": "true",
            "extra_column_policy": "warn",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "pass"
    assert recorded["dataset_kind"] == "tabular"
    assert recorded["data_name"] == "sample.csv"
    assert recorded["table_name"] == "sample"
    assert recorded["deep_check"] is True

    app.dependency_overrides.clear()


def test_web_admin_suggest_dataset_id():
    recorded = {}

    class WebAdminServiceStub:
        def suggest_next_dataset_id(self, user, collection_id):
            recorded["user"] = user.email
            recorded["collection_id"] = collection_id
            return {
                "collection_id": collection_id,
                "suggested_dataset_id": f"{collection_id}DS0042",
            }

    app.dependency_overrides[get_current_web_user] = lambda: SimpleNamespace(
        email="admin@example.com",
        is_admin=True,
    )
    app.dependency_overrides[WebAdminService] = lambda: WebAdminServiceStub()

    response = client.get("/api/v1/web/admin/datasets/suggest-id?collection_id=TS0001")

    assert response.status_code == 200
    assert response.json()["suggested_dataset_id"] == "TS0001DS0042"
    assert recorded["collection_id"] == "TS0001"

    app.dependency_overrides.clear()


def test_web_admin_documentation_sync_endpoint():
    recorded = {}

    class WebAdminServiceStub:
        def sync_dataset_documentation(self, user, dataset_id=None, only_outdated=True, force=False):
            recorded["user"] = user.email
            recorded["dataset_id"] = dataset_id
            recorded["only_outdated"] = only_outdated
            recorded["force"] = force
            return {
                "datasets": [
                    {
                        "ds_id": dataset_id or "TS0001DS0001",
                        "changed_fields": ["manifest_yaml"],
                        "needs_update": True,
                        "updated": True,
                    }
                ],
                "total": 1,
                "updated": 1,
            }

    app.dependency_overrides[get_current_web_user] = lambda: SimpleNamespace(
        email="admin@example.com",
        is_admin=True,
    )
    app.dependency_overrides[WebAdminService] = lambda: WebAdminServiceStub()

    response = client.post(
        "/api/v1/web/admin/documentation-sync",
        json={
            "dataset_id": "TS0001DS0001",
            "only_outdated": True,
            "force": False,
        },
    )

    assert response.status_code == 200
    assert response.json()["updated"] == 1
    assert recorded["dataset_id"] == "TS0001DS0001"
    assert recorded["only_outdated"] is True
    assert recorded["force"] is False

    app.dependency_overrides.clear()


def test_web_admin_dataset_documentation_update_endpoint():
    recorded = {}

    class WebAdminServiceStub:
        def update_dataset_documentation(self, user, dataset_id, body):
            recorded["user"] = user.email
            recorded["dataset_id"] = dataset_id
            recorded["readme_md"] = body.readme_md
            recorded["data_dictionary_json"] = body.data_dictionary_json
            return {
                "ds_id": dataset_id,
                "title": "Updated Dataset",
                "collection_id": "TS0001",
                "collection_name": "Transport",
                "data_owner_name": "ARTPARK",
                "description": None,
                "spatial_coverage_region_id": None,
                "spatial_resolution": None,
                "temporal_coverage_start_date": None,
                "temporal_coverage_end_date": None,
                "temporal_resolution": None,
                "access_level": "VIEW",
                "additional_metadata": None,
                "tags": [],
                "raw_dataset_ids": [],
                "raw_datasets": [],
                "readme_md": body.readme_md,
                "data_dictionary_json": '{"tables": {"main": {"data_dictionary": {}}}}',
                "manifest_yaml": None,
                "manifest_json": None,
                "manifest_updated_at": None,
                "manifest_updated_by": None,
                "documentation_synced_at": None,
            }

    app.dependency_overrides[get_current_web_user] = lambda: SimpleNamespace(
        email="admin@example.com",
        is_admin=True,
    )
    app.dependency_overrides[WebAdminService] = lambda: WebAdminServiceStub()

    response = client.put(
        "/api/v1/web/admin/datasets/TS0001DS0001/documentation",
        json={
            "readme_md": "# Updated README",
            "data_dictionary_json": {"tables": {"main": {"data_dictionary": {}}}},
        },
    )

    assert response.status_code == 200
    assert response.json()["readme_md"] == "# Updated README"
    assert recorded["dataset_id"] == "TS0001DS0001"
    assert recorded["data_dictionary_json"]["tables"]["main"]["data_dictionary"] == {}

    app.dependency_overrides.clear()


def test_web_admin_dataset_import_preview_endpoint():
    recorded = {}

    class WebAdminServiceStub:
        def preview_dataset_package_import(
            self,
            user,
            info_file,
            metadata_file,
            csv_files=None,
            dataset_override=None,
            raw_dataset_override=None,
        ):
            recorded["user"] = user.email
            recorded["info_name"] = info_file.filename
            recorded["metadata_name"] = metadata_file.filename
            recorded["csv_count"] = len(csv_files or [])
            recorded["dataset_override"] = dataset_override
            return {
                "dataset": {
                    "ds_id": "CUSTOM-ID",
                    "title": "Preview Dataset",
                    "collection_id": "TS0001",
                    "data_owner_name": "ARTPARK",
                    "description": None,
                    "spatial_coverage_region_id": None,
                    "spatial_resolution": None,
                    "temporal_coverage_start_date": None,
                    "temporal_coverage_end_date": None,
                    "temporal_resolution": None,
                    "access_level": "NONE",
                    "additional_metadata": None,
                    "tags": [],
                    "raw_dataset_ids": ["CUSTOM-ID-raw-001"],
                },
                "raw_dataset": {
                    "rds_id": "CUSTOM-ID-raw-001",
                    "title": "Raw data for Preview Dataset",
                    "source": "Manual upload",
                },
                "tables": [],
                "manifest_yaml": "datasetKind: tabular\n",
                "findings": [],
                "suggested_dataset_id": "TS0001DS0042",
                "can_import": True,
            }

    app.dependency_overrides[get_current_web_user] = lambda: SimpleNamespace(
        email="admin@example.com",
        is_admin=True,
    )
    app.dependency_overrides[WebAdminService] = lambda: WebAdminServiceStub()

    response = client.post(
        "/api/v1/web/admin/datasets/import/preview",
        files={
            "info_file": ("info.yml", io.BytesIO(b"title: Preview Dataset\n"), "application/x-yaml"),
            "metadata_file": ("metadata.yml", io.BytesIO(b"tables: {}\n"), "application/x-yaml"),
            "csv_files": ("sample.csv", io.BytesIO(b"year,value\n2024,1\n"), "text/csv"),
        },
        data={
            "dataset_override_json": '{"ds_id":"CUSTOM-ID"}',
        },
    )

    assert response.status_code == 200
    assert response.json()["dataset"]["ds_id"] == "CUSTOM-ID"
    assert recorded["csv_count"] == 1
    assert recorded["dataset_override"]["ds_id"] == "CUSTOM-ID"

    app.dependency_overrides.clear()


def test_web_admin_dataset_delete_verify_endpoint():
    recorded = {}

    class WebAdminServiceStub:
        def verify_dataset_deletion(self, user, dataset_id, code, confirmation_dataset_id):
            recorded["user"] = user.email
            recorded["dataset_id"] = dataset_id
            recorded["code"] = code
            recorded["confirmation_dataset_id"] = confirmation_dataset_id
            return {"deleted": True, "dataset_id": dataset_id}

    app.dependency_overrides[get_current_web_user] = lambda: SimpleNamespace(
        email="admin@example.com",
        is_admin=True,
    )
    app.dependency_overrides[WebAdminService] = lambda: WebAdminServiceStub()

    response = client.post(
        "/api/v1/web/admin/datasets/TS0001DS0001/delete/verify",
        json={
            "code": "123456",
            "confirmation_dataset_id": "TS0001DS0001",
        },
    )

    assert response.status_code == 200
    assert response.json()["deleted"] is True
    assert recorded["dataset_id"] == "TS0001DS0001"
    assert recorded["confirmation_dataset_id"] == "TS0001DS0001"

    app.dependency_overrides.clear()
