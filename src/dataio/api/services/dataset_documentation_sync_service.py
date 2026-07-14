from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
from sqlalchemy import text


def fetch_file_from_s3(bucket, dataset_id: str, filename: str) -> str | None:
    """
    Fetch a file from S3 for a dataset, trying standardised then preprocessed.
    """
    for version_type in ["STANDARDISED", "PREPROCESSED"]:
        key = f"filestore/{version_type}/{dataset_id}/{filename}"
        try:
            obj = bucket.Object(key)
            return obj.get()["Body"].read().decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                continue
            raise
    return None


def build_documentation_snapshot(bucket, dataset_id: str) -> dict[str, Any]:
    readme_content = fetch_file_from_s3(bucket, dataset_id, "README.md")
    manifest_yaml = fetch_file_from_s3(bucket, dataset_id, "manifest.yaml")
    manifest_json_content = fetch_file_from_s3(bucket, dataset_id, "manifest.json")
    data_dict_content = fetch_file_from_s3(bucket, dataset_id, "metadata.json")

    if manifest_yaml:
        if not manifest_json_content:
            parsed_manifest = None
            try:
                import yaml

                parsed_manifest = yaml.safe_load(manifest_yaml)
            except Exception:
                parsed_manifest = None
            if parsed_manifest is not None:
                manifest_json_content = json.dumps(parsed_manifest, sort_keys=True)

    return {
        "readme_md": readme_content,
        "data_dictionary_json": data_dict_content,
        "manifest_yaml": manifest_yaml,
        "manifest_json": manifest_json_content,
    }


def get_dataset_documentation_status(db_session, bucket, dataset_id: str) -> dict[str, Any]:
    snapshot = build_documentation_snapshot(bucket, dataset_id)
    dataset_row = db_session.execute(
        text(
            """
            SELECT ds_id, readme_md, data_dictionary_json, manifest_yaml, manifest_json,
                   manifest_updated_at, documentation_synced_at
            FROM datasets
            WHERE ds_id = :ds_id
            """
        ),
        {"ds_id": dataset_id},
    ).mappings().first()

    if dataset_row is None:
        raise ValueError(f"Dataset with ID {dataset_id} not found")

    current_manifest_json = dataset_row["manifest_json"]
    if current_manifest_json is not None:
        current_manifest_json = json.dumps(current_manifest_json, sort_keys=True)

    changed_fields = []
    comparisons = {
        "readme_md": (dataset_row["readme_md"], snapshot["readme_md"]),
        "data_dictionary_json": (
            dataset_row["data_dictionary_json"],
            snapshot["data_dictionary_json"],
        ),
        "manifest_yaml": (dataset_row["manifest_yaml"], snapshot["manifest_yaml"]),
        "manifest_json": (current_manifest_json, snapshot["manifest_json"]),
    }
    for field_name, (current_value, remote_value) in comparisons.items():
        if current_value != remote_value:
            changed_fields.append(field_name)

    return {
        "ds_id": dataset_id,
        "changed_fields": changed_fields,
        "needs_update": bool(changed_fields),
        "has_remote_documentation": any(value is not None for value in snapshot.values()),
        "manifest_updated_at": (
            dataset_row["manifest_updated_at"].isoformat()
            if dataset_row["manifest_updated_at"]
            else None
        ),
        "documentation_synced_at": (
            dataset_row["documentation_synced_at"].isoformat()
            if dataset_row["documentation_synced_at"]
            else None
        ),
        "snapshot": snapshot,
    }


def sync_dataset_documentation(
    db_session,
    bucket,
    dataset_id: str,
    *,
    dry_run: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    status = get_dataset_documentation_status(db_session, bucket, dataset_id)
    result = {
        "ds_id": dataset_id,
        "changed_fields": status["changed_fields"],
        "needs_update": status["needs_update"],
        "updated": False,
        "has_remote_documentation": status["has_remote_documentation"],
        "error": None,
    }

    if dry_run or (not force and not status["needs_update"]):
        return result

    snapshot = status["snapshot"]
    db_session.execute(
        text(
            """
            UPDATE datasets
            SET readme_md = :readme,
                data_dictionary_json = :data_dict,
                manifest_yaml = :manifest_yaml,
                manifest_json = CAST(:manifest_json AS jsonb),
                manifest_updated_at = :manifest_updated_at,
                documentation_synced_at = :synced_at
            WHERE ds_id = :ds_id
            """
        ),
        {
            "readme": snapshot["readme_md"],
            "data_dict": snapshot["data_dictionary_json"],
            "manifest_yaml": snapshot["manifest_yaml"],
            "manifest_json": snapshot["manifest_json"],
            "manifest_updated_at": (
                datetime.utcnow() if snapshot["manifest_yaml"] or snapshot["manifest_json"] else None
            ),
            "synced_at": datetime.utcnow(),
            "ds_id": dataset_id,
        },
    )
    db_session.commit()
    result["updated"] = True
    return result
