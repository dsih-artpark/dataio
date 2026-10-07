from __future__ import annotations

from pathlib import Path
from typing import Any

from dataio.validate.contracts.models import DatasetKind, DatasetManifest, ValidationRequest
from dataio.validate.loaders.data import load_tabular_rows
from dataio.validate.reports.models import Finding, ValidationResult
from dataio.validate.validators.base import ValidatorPlugin
from dataio.validate.validators.types import validate_field_value


class TabularValidator(ValidatorPlugin):
    @staticmethod
    def _get_table_source(
        table_name: str,
        manifest: DatasetManifest,
        request: ValidationRequest,
    ) -> tuple[str | None, bool]:
        inline_source = request.data_files.get(table_name)
        if inline_source is not None:
            return inline_source, True
        return manifest.datasetTables[table_name].path, False

    def supports(self, request: ValidationRequest) -> bool:
        return request.dataset_kind == DatasetKind.TABULAR

    def validate_structure(
        self,
        manifest: DatasetManifest,
        _data: Any,
        request: ValidationRequest,
        result: ValidationResult,
    ) -> None:
        for table_name, table in manifest.datasetTables.items():
            data_source, is_inline_source = self._get_table_source(
                table_name,
                manifest,
                request,
            )
            if table.required and not data_source:
                result.add_finding(
                    Finding(
                        severity="error",
                        code="missing_table_file",
                        message="Required table has no associated file path.",
                        table=table_name,
                        path=f"datasetTables.{table_name}.path",
                        rule_id="required_table_file",
                    )
                )
                continue
            if (
                data_source
                and not is_inline_source
                and not Path(data_source).exists()
            ):
                result.add_finding(
                    Finding(
                        severity="error",
                        code="missing_table_file",
                        message=f"Table file '{data_source}' does not exist.",
                        table=table_name,
                        path=f"datasetTables.{table_name}.path",
                        rule_id="required_table_file",
                    )
                )
                continue
            if data_source:
                result.summary.tables_checked += 1

    def validate_metadata(
        self,
        _manifest: DatasetManifest,
        _request: ValidationRequest,
        _result: ValidationResult,
    ) -> None:
        return

    def validate_content(
        self,
        manifest: DatasetManifest,
        _data: Any,
        request: ValidationRequest,
        result: ValidationResult,
    ) -> None:
        for table_name, table in manifest.datasetTables.items():
            data_source, is_inline_source = self._get_table_source(
                table_name,
                manifest,
                request,
            )
            if not data_source:
                continue
            if not is_inline_source and not Path(data_source).exists():
                continue

            max_rows = None if request.full_scan else request.max_rows
            rows = load_tabular_rows(data_source, max_rows=max_rows)
            if not rows:
                continue

            required_columns = set(table.dataDictionary.keys())
            actual_columns = set(rows[0].keys())
            missing_columns = sorted(required_columns - actual_columns)
            extra_columns = sorted(actual_columns - required_columns)

            for field_name in missing_columns:
                result.add_finding(
                    Finding(
                        severity="error",
                        code="missing_column",
                        message=f"Required column '{field_name}' is missing.",
                        table=table_name,
                        field=field_name,
                        path=f"{table_name}.{field_name}",
                        rule_id="required_column",
                    )
                )

            for field_name in extra_columns:
                if request.extra_column_policy == "ignore":
                    continue
                severity = "warning" if request.extra_column_policy == "warn" else "error"
                result.add_finding(
                    Finding(
                        severity=severity,
                        code="extra_column",
                        message=f"Column '{field_name}' is not declared in the manifest.",
                        table=table_name,
                        field=field_name,
                        path=f"{table_name}.{field_name}",
                        rule_id="extra_column",
                    )
                )

            for row_index, row in enumerate(rows, start=1):
                result.summary.rows_checked += 1
                for field_name, field in table.dataDictionary.items():
                    validate_field_value(
                        field_name,
                        field,
                        row.get(field_name),
                        result,
                        table=table_name,
                        row=row_index,
                        path=f"{table_name}.{field_name}",
                    )
