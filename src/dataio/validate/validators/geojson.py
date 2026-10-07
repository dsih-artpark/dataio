from __future__ import annotations

from typing import Any

from dataio.validate.contracts.models import DatasetKind, DatasetManifest, ValidationRequest
from dataio.validate.reports.models import Finding, ValidationResult
from dataio.validate.validators.base import ValidatorPlugin
from dataio.validate.validators.types import validate_field_value


class GeoJSONValidator(ValidatorPlugin):
    def supports(self, request: ValidationRequest) -> bool:
        return request.dataset_kind == DatasetKind.GEOJSON

    def validate_structure(
        self,
        _manifest: DatasetManifest,
        data: Any,
        _request: ValidationRequest,
        result: ValidationResult,
    ) -> None:
        if not isinstance(data, dict):
            result.add_finding(
                Finding(
                    severity="error",
                    code="invalid_geojson",
                    message="GeoJSON payload must be an object.",
                    path="$",
                    rule_id="geojson_root_object",
                )
            )
            return
        if data.get("type") != "FeatureCollection":
            result.add_finding(
                Finding(
                    severity="error",
                    code="invalid_geojson_type",
                    message="GeoJSON root type must be FeatureCollection.",
                    path="type",
                    rule_id="geojson_feature_collection",
                )
            )
            return
        if not isinstance(data.get("features"), list):
            result.add_finding(
                Finding(
                    severity="error",
                    code="missing_features",
                    message="GeoJSON FeatureCollection must contain a features array.",
                    path="features",
                    rule_id="geojson_features_array",
                )
            )
            return
        result.summary.tables_checked = 1
        result.inferred["feature_count"] = len(data["features"])

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
        data: Any,
        _request: ValidationRequest,
        result: ValidationResult,
    ) -> None:
        features = data.get("features", []) if isinstance(data, dict) else []
        table = next(iter(manifest.datasetTables.values()), None)
        if table is None:
            return

        for index, feature in enumerate(features, start=1):
            result.summary.rows_checked += 1
            if not isinstance(feature, dict):
                result.add_finding(
                    Finding(
                        severity="error",
                        code="invalid_feature",
                        message="Each feature must be an object.",
                        row=index,
                        path=f"features[{index - 1}]",
                        rule_id="geojson_feature_object",
                    )
                )
                continue
            if feature.get("type") != "Feature":
                result.add_finding(
                    Finding(
                        severity="error",
                        code="invalid_feature_type",
                        message="Each entry in features must have type Feature.",
                        row=index,
                        path=f"features[{index - 1}].type",
                        rule_id="geojson_feature_type",
                    )
                )
            geometry = feature.get("geometry")
            if (
                not isinstance(geometry, dict)
                or "type" not in geometry
                or "coordinates" not in geometry
            ):
                result.add_finding(
                    Finding(
                        severity="error",
                        code="invalid_geometry",
                        message="Feature geometry must include type and coordinates.",
                        row=index,
                        path=f"features[{index - 1}].geometry",
                        rule_id="geojson_geometry_shape",
                    )
                )
            properties = feature.get("properties", {})
            for field_name, field in table.dataDictionary.items():
                if field_name == "id":
                    value = feature.get("id")
                    path = f"features[{index - 1}].id"
                elif field_name.startswith("properties."):
                    property_name = field_name.split(".", 1)[1]
                    value = properties.get(property_name)
                    path = f"features[{index - 1}].properties.{property_name}"
                else:
                    value = feature.get(field_name)
                    path = f"features[{index - 1}].{field_name}"
                validate_field_value(
                    field_name,
                    field,
                    value,
                    result,
                    table="features",
                    row=index,
                    path=path,
                )
