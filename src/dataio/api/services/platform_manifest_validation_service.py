from __future__ import annotations

from dataio.api.database import functions as database
from dataio.validate.contracts.models import DatasetManifest
from dataio.validate.reports.models import Finding, ValidationResult
from dataio.validate.validators.metadata import DATASET_ID_RE


def apply_platform_manifest_checks(
    manifest: DatasetManifest,
    result: ValidationResult,
) -> None:
    if manifest.datasetID is None or DATASET_ID_RE.match(manifest.datasetID) is None:
        return

    if not database.check_if_dataset_exists(manifest.datasetID):
        result.add_finding(
            Finding(
                severity="error",
                code="unknown_dataset_id",
                message="datasetID does not exist in the platform database.",
                path="datasetID",
                rule_id="dataset_id_exists",
            )
        )
    else:
        dataset = database.get_dataset(manifest.datasetID)
        if dataset and dataset.collection is not None and (
            manifest.collection.get("ID") != dataset.collection.collection_id
        ):
            result.add_finding(
                Finding(
                    severity="error",
                    code="dataset_collection_mismatch",
                    message="Manifest collection ID does not match the dataset record.",
                    path="collection.ID",
                    rule_id="dataset_collection_matches_record",
                )
            )

    collection = database.get_collection_by_identifier(manifest.collection["ID"])
    if collection is None:
        result.add_finding(
            Finding(
                severity="error",
                code="unknown_collection",
                message="collection.ID does not exist in the platform database.",
                path="collection.ID",
                rule_id="collection_exists",
            )
        )
        return

    if manifest.collection.get("name") != collection.collection_name:
        result.add_finding(
            Finding(
                severity="error",
                code="collection_name_mismatch",
                message="collection.name does not match the database record.",
                path="collection.name",
                rule_id="collection_name_matches_db",
            )
        )

    if manifest.category.get("ID") != collection.category_id:
        result.add_finding(
            Finding(
                severity="error",
                code="category_id_mismatch",
                message="category.ID does not match the collection's database category.",
                path="category.ID",
                rule_id="category_id_matches_db",
            )
        )

    if manifest.category.get("name") != collection.category_name:
        result.add_finding(
            Finding(
                severity="error",
                code="category_name_mismatch",
                message="category.name does not match the collection's database category name.",
                path="category.name",
                rule_id="category_name_matches_db",
            )
        )
