from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class DatasetKind(StrEnum):
    TABULAR = "tabular"
    GEOJSON = "geojson"


class ManifestField(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str
    description: str | None = None
    nullable: bool = True
    format: str | None = None
    allowedValues: list[Any] | None = None
    enumRef: str | None = None
    range: list[float] | None = None
    min: float | None = None
    max: float | None = None
    comments: str | list[str] | None = None

    @model_validator(mode="after")
    def validate_contract(self) -> ManifestField:
        if self.range is not None and (self.min is not None or self.max is not None):
            raise ValueError("range cannot be combined with min or max")
        if self.type == "enum" and self.allowedValues is None and self.enumRef is None:
            raise ValueError("enum fields must define allowedValues or enumRef")
        if self.type in {"date", "dateTime"} and not self.format:
            raise ValueError(f"{self.type} fields must define format")
        if self.type in {"date", "dateTime"} and self.format:
            if "%" not in self.format:
                raise ValueError(
                    f"{self.type} format must use strftime directives like %Y or %Y-%m-%d"
                )
            sample = datetime(2024, 3, 13, 12, 30, 45, tzinfo=UTC)
            try:
                rendered = sample.strftime(self.format)
                datetime.strptime(rendered, self.format)
            except ValueError as exc:
                raise ValueError(
                    f"{self.type} format must be a valid strftime format"
                ) from exc
        if self.type == "dateTime" and self.format and "%z" not in self.format:
            raise ValueError("dateTime formats must include timezone information via %z")
        return self


class ManifestTable(BaseModel):
    model_config = ConfigDict(extra="allow")

    description: str | None = None
    path: str | None = None
    required: bool = True
    dataDictionary: dict[str, ManifestField] = Field(default_factory=dict)


class EnumValueDefinition(BaseModel):
    model_config = ConfigDict(extra="allow")

    description: str | None = None


class EnumDefinition(BaseModel):
    model_config = ConfigDict(extra="allow")

    description: str | None = None
    values: dict[str, EnumValueDefinition] = Field(default_factory=dict)


class DatasetManifest(BaseModel):
    model_config = ConfigDict(extra="allow")

    metadataSpecVersion: str
    datasetTitle: str
    datasetSlug: str
    datasetDescription: str
    source: Any
    category: dict[str, Any]
    collection: dict[str, Any]
    datasetID: str | None = None
    datasetKind: DatasetKind = DatasetKind.TABULAR
    datasetTables: dict[str, ManifestTable] = Field(default_factory=dict)
    enumDefinitions: dict[str, EnumDefinition] = Field(default_factory=dict)
    specs: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_shape(self) -> DatasetManifest:
        if "ID" not in self.category or "name" not in self.category:
            raise ValueError("category must define ID and name")
        if "ID" not in self.collection or "name" not in self.collection:
            raise ValueError("collection must define ID and name")
        if self.datasetKind == DatasetKind.TABULAR and not self.datasetTables:
            raise ValueError("tabular manifests must define datasetTables")
        return self


class ValidationRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    dataset_kind: DatasetKind
    manifest_source: str | bytes | dict[str, Any] | DatasetManifest
    data: str | bytes | dict[str, Any] | None = None
    data_files: dict[str, str] = Field(default_factory=dict)
    enabled_specs: list[str] = Field(default_factory=list)
    strict: bool = False
    deep_check: bool = False
    validate_data: bool = True
    full_scan: bool = True
    max_rows: int | None = None
    extra_column_policy: Literal["warn", "error", "ignore"] = "warn"
