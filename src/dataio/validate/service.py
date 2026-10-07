from __future__ import annotations

from collections.abc import Callable

from pydantic import ValidationError

from dataio.validate.contracts.models import (
    DatasetKind,
    DatasetManifest,
    ValidationRequest,
)
from dataio.validate.loaders.data import load_geojson_data
from dataio.validate.loaders.schema import build_manifest_source_map, load_manifest
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
        source_map = build_manifest_source_map(request.manifest_source)
        try:
            manifest = load_manifest(request.manifest_source)
        except ValidationError as exc:
            for finding in _validation_error_findings(exc, source_map):
                result.add_finding(finding)
            if not result.findings:
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
        except ValueError as exc:
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
        _attach_source_positions(result, source_map)
        return result


def _validation_error_findings(
    exc: ValidationError,
    source_map: dict[str, tuple[int, int]],
) -> list[Finding]:
    findings: list[Finding] = []
    for error in exc.errors():
        path = _normalize_error_path(error.get("loc", ()))
        line, column = _find_source_position(path, source_map)
        findings.append(
            Finding(
                severity="error",
                code="invalid_manifest",
                message=error.get("msg", str(exc)),
                path=path,
                line=line,
                column=column,
                rule_id="manifest_parse",
                hint=_build_error_hint(path),
            )
        )
    return findings


def _normalize_error_path(loc: tuple[object, ...] | list[object]) -> str:
    parts = [str(part) for part in loc if part != "__root__"]
    if not parts:
        return "manifest"
    return ".".join(parts)


def _build_error_hint(path: str) -> str | None:
    if path == "manifest":
        return "The manifest could not be parsed into the expected contract."
    return f"Check the YAML entry at '{path}' and its nested properties."


def _attach_source_positions(
    result: ValidationResult,
    source_map: dict[str, tuple[int, int]],
) -> None:
    for finding in result.findings:
        if finding.line is not None:
            continue
        line, column = _find_source_position(finding.path, source_map)
        finding.line = line
        finding.column = column


def _find_source_position(
    path: str | None,
    source_map: dict[str, tuple[int, int]],
) -> tuple[int | None, int | None]:
    if not path:
        return None, None
    if path in source_map:
        return source_map[path]

    current = path
    while "." in current:
        current = current.rsplit(".", 1)[0]
        if current in source_map:
            return source_map[current]
    return None, None
