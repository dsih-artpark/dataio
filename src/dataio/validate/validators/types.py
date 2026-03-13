from __future__ import annotations

from typing import Any

from dataio.validate.contracts.models import DatasetManifest, ManifestField
from dataio.validate.registry.types import TYPE_VALIDATORS
from dataio.validate.reports.models import Finding, ValidationResult


def validate_declared_types(manifest: DatasetManifest, result: ValidationResult) -> None:
    for table_name, table in manifest.datasetTables.items():
        for field_name, field in table.dataDictionary.items():
            if field.type not in TYPE_VALIDATORS:
                result.add_finding(
                    Finding(
                        severity="warning",
                        code="unknown_declared_type",
                        message=f"Field declares unknown type '{field.type}'.",
                        path=f"datasetTables.{table_name}.dataDictionary.{field_name}.type",
                        table=table_name,
                        field=field_name,
                        rule_id="declared_type_known",
                    )
                )


def validate_field_value(
    field_name: str,
    field: ManifestField,
    value: Any,
    result: ValidationResult,
    *,
    table: str | None = None,
    row: int | None = None,
    path: str | None = None,
) -> None:
    if value in (None, "") and field.nullable:
        return
    if value in (None, ""):
        result.add_finding(
            Finding(
                severity="error",
                code="nullability_violation",
                message="Value is required but missing.",
                table=table,
                row=row,
                field=field_name,
                path=path,
                rule_id="nullable_false",
            )
        )
        return

    validator = TYPE_VALIDATORS.get(field.type)
    if validator is None:
        return

    if not validator(value, field):
        result.add_finding(
            Finding(
                severity="error",
                code="type_validation_failed",
                message=f"Value '{value}' does not satisfy type '{field.type}'.",
                table=table,
                row=row,
                field=field_name,
                path=path,
                rule_id=f"type_{field.type}",
            )
        )
