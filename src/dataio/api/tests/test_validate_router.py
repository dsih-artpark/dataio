from __future__ import annotations

import os

from fastapi.testclient import TestClient

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from dataio.api import app

client = TestClient(app)


def test_validate_tabular_endpoint(tmp_path):
    manifest_path = tmp_path / "manifest.yaml"
    data_path = tmp_path / "sample.csv"
    data_path.write_text("year,value\n2024,1\n", encoding="utf-8")
    manifest_path.write_text(
        f"""
metadataSpecVersion: v2
datasetTitle: Sample
datasetSlug: sample-dataset
datasetDescription: Example
source: Test
category: {{ID: TS, name: Test}}
collection: {{ID: TS0001, name: Tests}}
datasetKind: tabular
datasetTables:
  sample:
    path: {data_path}
    dataDictionary:
      year:
        type: date
        format: YYYY
        nullable: false
      value:
        type: int
        nullable: false
""",
        encoding="utf-8",
    )

    with manifest_path.open("rb") as manifest_file, data_path.open("rb") as data_file:
        response = client.post(
            "/api/v1/validate/tabular",
            files={
                "manifest_file": ("manifest.yaml", manifest_file, "application/x-yaml"),
                "table_file": ("sample.csv", data_file, "text/csv"),
            },
            data={"table_name": "sample"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["dataset_kind"] == "tabular"
    assert payload["status"] == "pass"
