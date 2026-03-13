from __future__ import annotations

from pydantic import ValidationError

from dataio.validate.contracts.models import DatasetManifest
from dataio.validate.reports.models import Finding, ValidationResult
from dataio.validate.validators.rules import apply_cross_field_rules


def validate_metadata_contract(manifest: DatasetManifest, result: ValidationResult) -> None:
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
