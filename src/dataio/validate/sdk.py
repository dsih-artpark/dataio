from __future__ import annotations

from dataio.validate.contracts.models import DatasetKind, ValidationRequest
from dataio.validate.reports.models import ValidationResult
from dataio.validate.service import DataIOValidationService


class DataIOValidator:
    def __init__(self):
        self.service = DataIOValidationService()

    def validate_tabular(
        self,
        *,
        manifest: str | bytes | dict,
        data_files: dict[str, str],
        full_scan: bool = True,
        max_rows: int | None = None,
        extra_column_policy: str = "warn",
    ) -> ValidationResult:
        request = ValidationRequest(
            dataset_kind=DatasetKind.TABULAR,
            manifest_source=manifest,
            data_files=data_files,
            full_scan=full_scan,
            max_rows=max_rows,
            extra_column_policy=extra_column_policy,
        )
        return self.service.validate(request)

    def validate_geojson(
        self,
        *,
        manifest: str | bytes | dict,
        data: str | bytes | dict,
    ) -> ValidationResult:
        request = ValidationRequest(
            dataset_kind=DatasetKind.GEOJSON,
            manifest_source=manifest,
            data=data,
        )
        return self.service.validate(request)
