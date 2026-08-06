"""Converts a v2-schema manifest dict (tables/data_dictionary - the real,
curator-facing metadata.yaml authoring format) into the shape
dataio.validate.contracts.models.DatasetManifest expects (datasetTables/
dataDictionary) - so the drafter can be checked with the SAME existing
DataIOValidator every other dataset in this system was actually validated
and uploaded through.

This mirrors WebAdminService._build_manifest_field /
_parse_dataset_package (web_admin_service.py), which does exactly this
conversion for the dataset-package import flow - confirmed by reading the
actual live manifest_yaml stored in Postgres for CS0007DS0112: it is in
the datasetTables shape, not the tables/data_dictionary shape the
`data/` folder's pre-import staging copy uses. Kept as a standalone module
rather than imported from web_admin_service.py to avoid touching that
large, already-relied-upon service class; the two should be kept in sync
if either changes.
"""

from __future__ import annotations

from typing import Any


def convert_v2_field_to_manifest_field(
    field_name: str,
    field_spec: dict,
    enum_scope: dict | None = None,
) -> dict:
    field_type = field_spec.get("type")
    manifest_field: dict[str, Any] = {
        "description": field_spec.get("description"),
        "comments": field_spec.get("comments"),
        "nullable": field_spec.get("nullable", True),
    }
    if field_type == "year":
        manifest_field["type"] = "date"
        manifest_field["format"] = "%Y"
    elif field_type == "enum":
        manifest_field["type"] = "enum"
        allowed_values = field_spec.get("enum") or field_spec.get("allowedValues")
        if not allowed_values and field_spec.get("enumRef") and enum_scope:
            enum_ref = field_spec["enumRef"]
            nested_definitions = enum_scope.get("enumDefinitions")
            enum_def = enum_scope.get(enum_ref)
            if not isinstance(enum_def, dict) and isinstance(nested_definitions, dict):
                enum_def = nested_definitions.get(enum_ref)
            if isinstance(enum_def, dict):
                allowed_values = list((enum_def.get("values") or {}).keys())
        manifest_field["allowedValues"] = allowed_values or []
    elif field_type in {"string", "boolean", "int", "float", "regionID", "regionName", "date", "dateTime"}:
        manifest_field["type"] = field_type
        if field_spec.get("format"):
            manifest_field["format"] = field_spec["format"]
    else:
        if field_name == "year":
            manifest_field["type"] = "date"
            manifest_field["format"] = "%Y"
        elif field_name.endswith(".ID"):
            manifest_field["type"] = "regionID"
        elif field_name.endswith(".name"):
            manifest_field["type"] = "regionName"
        else:
            manifest_field["type"] = "string"

    if field_spec.get("range") is not None:
        manifest_field["range"] = field_spec["range"]
    if field_spec.get("min") is not None:
        manifest_field["min"] = field_spec["min"]
    if field_spec.get("max") is not None:
        manifest_field["max"] = field_spec["max"]

    handled_keys = {
        "type", "description", "comments", "nullable",
        "enum", "allowedValues", "enumRef", "range", "min", "max",
    }
    for key, value in field_spec.items():
        if key not in handled_keys and key not in manifest_field:
            manifest_field[key] = value
    return manifest_field


def convert_v2_manifest_to_contract(manifest_dict: dict) -> dict:
    """manifest_dict is expected to already carry datasetTitle, datasetSlug,
    datasetID, metadataSpecVersion, category, collection, source,
    datasetDescription (draft_service.py sets these deterministically) -
    this only needs to rename tables -> datasetTables (with per-field type
    conversion) and add datasetKind.
    """
    tables = manifest_dict.get("tables") or {}
    dataset_tables: dict[str, dict] = {}

    for table_name, table in tables.items():
        table = table or {}
        data_dictionary = table.get("data_dictionary") or {}
        dataset_table: dict[str, Any] = {
            "description": table.get("description"),
            "source": table.get("source"),
            "path": f"{table_name}.csv",
            "dataDictionary": {
                field_name: convert_v2_field_to_manifest_field(field_name, field_spec, manifest_dict)
                for field_name, field_spec in data_dictionary.items()
                if isinstance(field_spec, dict)
            },
        }
        for key, value in table.items():
            if key not in {"description", "source", "data_dictionary"} and key not in dataset_table:
                dataset_table[key] = value
        dataset_tables[table_name] = dataset_table

    return {
        **{key: value for key, value in manifest_dict.items() if key != "tables"},
        "datasetKind": "tabular",
        "datasetTables": dataset_tables,
    }
