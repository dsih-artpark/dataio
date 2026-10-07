from __future__ import annotations

import yaml

from dataio.api.services.manifest_v2_conversion import (
    convert_v2_field_to_manifest_field,
    convert_v2_manifest_to_contract,
)
from dataio.validate.sdk import DataIOValidator

V2_MANIFEST = {
    "datasetTitle": "Consolidated Livestock Census",
    "datasetSlug": "cs0007ds0112-consolidated-livestock-census",
    "datasetID": "CS0007DS0112",
    "metadataSpecVersion": "v2",
    "source": ["16th Livestock Census 1997"],
    "category": {"ID": "CS", "name": "Census and Surveys"},
    "collection": {"ID": "CS0007", "name": "Livestock Census (by DAHD)"},
    "datasetDescription": "State-level livestock counts.",
    "datasetOwner": "DAHD",
    "tags": {"concept": ["livestock"], "epiType": ["population"]},
    "spatialCoverage": "India",
    "spatialResolution": "state",
    "temporalCoverage": "1997, 2003, 2007, 2012, 2019",
    "temporalResolution": "quinquennial",
    "updateFrequency": "Quinquennial",
    "lastUpdated": "2026-07-21",
    "comments": ["Covers cattle and buffalo only."],
    "references": ["https://dahd.gov.in/report.pdf"],
    "enumDefinitions": {
        "livestockSpecies": {
            "description": "Species",
            "values": {"cattle": {"description": "Cattle"}, "buffalo": {"description": "Buffalo"}},
        }
    },
    "tables": {
        "consolidated-livestock-census": {
            "description": "One row per state/year/species.",
            "source": "16th-20th Livestock Census PDFs",
            "joinKeys": ["state.ID", "year", "species"],
            "data_dictionary": {
                "year": {"type": "date", "format": "%Y", "description": "Census year", "nullable": False},
                "species": {
                    "type": "enum", "description": "Species", "nullable": False, "enumRef": "livestockSpecies",
                },
                "count": {"type": "int", "description": "Count", "nullable": False},
            },
        }
    },
}


def test_convert_field_enum_resolves_enum_ref_against_manifest():
    field = convert_v2_field_to_manifest_field(
        "species",
        {"type": "enum", "enumRef": "livestockSpecies"},
        enum_scope=V2_MANIFEST,
    )
    assert field["type"] == "enum"
    assert set(field["allowedValues"]) == {"cattle", "buffalo"}


def test_convert_field_passes_through_plain_types():
    field = convert_v2_field_to_manifest_field("count", {"type": "int", "nullable": False})
    assert field["type"] == "int"
    assert field["nullable"] is False


def test_convert_field_carries_extra_keys_like_isjoinkey():
    field = convert_v2_field_to_manifest_field(
        "year", {"type": "date", "format": "%Y", "isJoinKey": True, "joinKeyType": "temporal"}
    )
    assert field["isJoinKey"] is True
    assert field["joinKeyType"] == "temporal"


def test_convert_manifest_renames_tables_to_dataset_tables():
    contract = convert_v2_manifest_to_contract(V2_MANIFEST)

    assert "tables" not in contract
    assert "consolidated-livestock-census" in contract["datasetTables"]
    table = contract["datasetTables"]["consolidated-livestock-census"]
    assert "dataDictionary" in table
    assert table["dataDictionary"]["species"]["allowedValues"] == ["cattle", "buffalo"]
    assert contract["datasetKind"] == "tabular"
    # required top-level fields carried through unchanged
    assert contract["datasetID"] == "CS0007DS0112"
    assert contract["category"] == {"ID": "CS", "name": "Census and Surveys"}


def test_converted_manifest_passes_the_existing_validator():
    """The whole point: this converted shape must actually pass
    dataio.validate.sdk.DataIOValidator - the same validator every real
    dataset in this system was checked against.
    """
    contract = convert_v2_manifest_to_contract(V2_MANIFEST)
    manifest_yaml = yaml.safe_dump(contract, sort_keys=False)

    result = DataIOValidator().validate_tabular(
        manifest=manifest_yaml,
        data_files={"consolidated-livestock-census": "year,species,count\n2019,cattle,10\n"},
        deep_check=False,
        full_scan=False,
        max_rows=1,
    )

    errors = [f for f in result.findings if f.severity == "error"]
    assert errors == [], errors


def test_converted_real_production_manifest_passes_the_existing_validator():
    """Convert the actual staging-format metadata.yaml on disk for
    CS0007DS0112 and confirm it passes - matching the live manifest_yaml
    already stored in Postgres for that same dataset (confirmed by
    inspecting it directly: datasetTables, not tables).
    """
    with open(
        "D:/dataio/data/CS0007DS0112-Consolidated-Livestock-Census-1997-2019/metadata.yaml",
        encoding="utf-8",
    ) as f:
        real_manifest = yaml.safe_load(f)

    contract = convert_v2_manifest_to_contract(real_manifest)
    manifest_yaml = yaml.safe_dump(contract, sort_keys=False)

    with open(
        "D:/dataio/data/CS0007DS0112-Consolidated-Livestock-Census-1997-2019/"
        "consolidated-livestock-census.csv",
        encoding="utf-8",
    ) as f:
        csv_text = f.read()

    result = DataIOValidator().validate_tabular(
        manifest=manifest_yaml,
        data_files={"consolidated-livestock-census": csv_text},
        deep_check=False,
        full_scan=False,
        max_rows=5,
    )

    errors = [f for f in result.findings if f.severity == "error"]
    assert errors == [], errors
