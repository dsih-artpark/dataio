from __future__ import annotations

import importlib
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
WebUserService = importlib.import_module("dataio.api.routers.web").WebUserService

client = TestClient(app)


def test_web_user_get_manifest():
    recorded = {}

    class WebUserServiceStub:
        def get_dataset_manifest(self, user, dataset_id):
            recorded["user"] = user.email
            recorded["dataset_id"] = dataset_id
            return {
                "dataset_id": dataset_id,
                "bucket_type": "STANDARDISED",
                "manifest_yaml": "datasetKind: tabular\n",
                "manifest_json": {"datasetKind": "tabular"},
                "has_manifest": True,
                "manifest_updated_at": None,
                "manifest_updated_by": None,
            }

    app.dependency_overrides[get_current_web_user] = lambda: SimpleNamespace(
        email="user@example.com",
        is_admin=False,
    )
    app.dependency_overrides[WebUserService] = lambda: WebUserServiceStub()

    response = client.get("/api/v1/web/datasets/TS0001DS0001/manifest")

    assert response.status_code == 200
    assert response.json()["has_manifest"] is True
    assert recorded["dataset_id"] == "TS0001DS0001"
    assert recorded["user"] == "user@example.com"

    app.dependency_overrides.clear()
