from __future__ import annotations

from collections.abc import Callable

from pydantic import ValidationError

from dataio.validate.contracts.models import (
    DatasetKind,
    DatasetManifest,
    ValidationRequest,
)
from dataio.validate.loaders.data import load_geojson_data
from dataio.validate.loaders.schema import load_manifest
from dataio.validate.registry.plugins import get_validator_plugin
from dataio.validate.reports.models import Finding, ValidationResult
from dataio.validate.validators.metadata import validate_metadata_contract
from dataio.validate.validators.types import validate_declared_types


class DataIOValidationService:
    def __init__(
        self,
        *,
        platform_manifest_checker: Callable[[DatasetManifest, ValidationResult], None]
        | None = None,
    ) -> None:
        self.platform_manifest_checker = platform_manifest_checker

    def validate(self, request: ValidationRequest) -> ValidationResult:
        result = ValidationResult(dataset_kind=request.dataset_kind.value)
        try:
            manifest = load_manifest(request.manifest_source)
        except (ValidationError, ValueError) as exc:
            result.add_finding(
                Finding(
                    severity="error",
                    code="invalid_manifest",
                    message=str(exc),
                    path="manifest",
                    rule_id="manifest_parse",
                )
            )
            return result

        result.metadata_spec_version = manifest.metadataSpecVersion
        result.inferred["dataset_title"] = manifest.datasetTitle
        validate_metadata_contract(manifest, result)
        if (
            request.deep_check or request.strict
        ) and self.platform_manifest_checker is not None:
            self.platform_manifest_checker(manifest, result)
        validate_declared_types(manifest, result)

        plugin = get_validator_plugin(request.dataset_kind)
        if request.dataset_kind == DatasetKind.GEOJSON:
            try:
                loaded_data = load_geojson_data(request.data or {})
            except ValueError as exc:
                result.add_finding(
                    Finding(
                        severity="error",
                        code="invalid_data",
                        message=str(exc),
                        path="data",
                        rule_id="data_parse",
                    )
                )
                return result
        else:
            loaded_data = request.data

        if request.validate_data:
            plugin.validate_structure(manifest, loaded_data, request, result)
        plugin.validate_metadata(manifest, request, result)
        if request.validate_data:
            plugin.validate_content(manifest, loaded_data, request, result)
        return result
