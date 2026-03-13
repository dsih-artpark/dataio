from __future__ import annotations

import re

from pydantic import ValidationError

from dataio.validate.contracts.models import DatasetManifest
from dataio.validate.reports.models import Finding, ValidationResult
from dataio.validate.validators.rules import apply_cross_field_rules

DATASET_ID_RE = re.compile(r"^[A-Z]{2}\d{4}DS\d{4}$")
SLUG_RE = re.compile(r"^(?P<dataset_id>[a-z]{2}\d{4}ds\d{4})-[a-z0-9]+(?:-[a-z0-9]+)*$")


def validate_metadata_contract(
    manifest: DatasetManifest,
    result: ValidationResult,
) -> None:
    try:
        DatasetManifest.model_validate(manifest.model_dump())
    except ValidationError as exc:
        for error in exc.errors():
            result.add_finding(
                Finding(
                    severity="error",
                    code="manifest_contract_error",
                    message=error["msg"],
                    path=".".join(str(item) for item in error["loc"]),
                    rule_id="metadata_contract",
                    )
                )

    if manifest.datasetID is None:
        result.add_finding(
            Finding(
                severity="error",
                code="missing_dataset_id",
                message="Manifest must define datasetID.",
                path="datasetID",
                rule_id="dataset_id_required",
            )
        )
    elif DATASET_ID_RE.match(manifest.datasetID) is None:
        result.add_finding(
            Finding(
                severity="error",
                code="invalid_dataset_id",
                message="datasetID must match the pattern AA0000DS0000.",
                path="datasetID",
                rule_id="dataset_id_pattern",
            )
        )

    slug_match = SLUG_RE.match(manifest.datasetSlug)
    if slug_match is None:
        result.add_finding(
            Finding(
                severity="error",
                code="invalid_dataset_slug",
                message=(
                    "datasetSlug must start with the lowercase dataset ID and use "
                    "lowercase hyphenated words, e.g. cs0007ds0112-example-dataset."
                ),
                path="datasetSlug",
                rule_id="dataset_slug_pattern",
            )
        )
    elif (
        manifest.datasetID is not None
        and slug_match.group("dataset_id") != manifest.datasetID.lower()
    ):
        result.add_finding(
            Finding(
                severity="error",
                code="dataset_slug_id_mismatch",
                message="datasetSlug must begin with the lowercase datasetID followed by '-'.",
                path="datasetSlug",
                rule_id="dataset_slug_matches_id",
            )
        )

    for table_name, table in manifest.datasetTables.items():
        if not table.dataDictionary:
            result.add_finding(
                Finding(
                    severity="error",
                    code="missing_data_dictionary",
                    message="Each table must define a non-empty dataDictionary.",
                    path=f"datasetTables.{table_name}.dataDictionary",
                    table=table_name,
                    rule_id="table_requires_data_dictionary",
                )
            )
        for field_name, field in table.dataDictionary.items():
            if field.enumRef and field.enumRef not in manifest.enumDefinitions:
                result.add_finding(
                    Finding(
                        severity="error",
                        code="unknown_enum_reference",
                        message=f"Field references unknown enum definition '{field.enumRef}'.",
                        path=f"datasetTables.{table_name}.dataDictionary.{field_name}.enumRef",
                        table=table_name,
                        field=field_name,
                        rule_id="enum_ref_must_resolve",
                    )
                )

    apply_cross_field_rules(manifest, result)
