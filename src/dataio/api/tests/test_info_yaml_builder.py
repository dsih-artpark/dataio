from __future__ import annotations

import yaml

from dataio.api.services.info_yaml_builder import build_info_yaml


def _draft_json(**overrides):
    base = {
        "datasetTitle": "consolidated-livestock-census",
        "datasetOwner": "DAHD",
        "datasetDescription": "State-level livestock population counts.",
        "spatialResolution": "state",
        "source": ["16th Livestock Census 1997", "17th Livestock Census 2003"],
        "tables": {
            "main": {
                "data_dictionary": {
                    "year": {
                        "type": "date",
                        "format": "%Y",
                        "allowedValues": ["1997", "2003", "2007", "2012", "2019"],
                    },
                    "count": {"type": "int"},
                }
            }
        },
    }
    base.update(overrides)
    return base


def test_build_info_yaml_derives_every_field_from_the_draft():
    result = build_info_yaml(
        _draft_json(),
        dataset_id="CS0007DS0119",
        collection_id="CS0007",
        raw_dataset_id="CS0007RDS0005",
        access_level="DOWNLOAD",
    )
    parsed = yaml.safe_load(result)

    assert parsed == {
        "ds_id": "CS0007DS0119",
        "collection_id": "CS0007",
        "title": "consolidated-livestock-census",
        "data_owner_name": "DAHD",
        "description": "State-level livestock population counts.",
        "temporal_coverage_start_date": "1997",
        "temporal_coverage_end_date": "2019",
        "temporal_resolution": "YEAR",
        "spatial_resolution": "STATE",
        "access_level": "DOWNLOAD",
        "raw_dataset": {
            "rds_id": "CS0007RDS0005",
            "source": "16th Livestock Census 1997; 17th Livestock Census 2003",
        },
    }


def test_build_info_yaml_matches_hand_authored_key_order():
    result = build_info_yaml(
        _draft_json(),
        dataset_id="CS0007DS0119",
        collection_id="CS0007",
        raw_dataset_id="CS0007RDS0005",
        access_level="NONE",
    )
    keys = list(yaml.safe_load(result).keys())
    assert keys == [
        "ds_id", "collection_id", "title", "data_owner_name", "description",
        "temporal_coverage_start_date", "temporal_coverage_end_date", "temporal_resolution",
        "spatial_resolution", "access_level", "raw_dataset",
    ]


def test_build_info_yaml_scans_every_table_for_the_temporal_axis():
    draft_json = _draft_json(
        tables={
            "no_dates": {
                "data_dictionary": {"species": {"type": "enum", "enumRef": "speciesEnum"}},
            },
            "main": _draft_json()["tables"]["main"],
        }
    )
    result = build_info_yaml(
        draft_json, dataset_id="CS0007DS0119", collection_id="CS0007",
        raw_dataset_id=None, access_level="NONE",
    )
    parsed = yaml.safe_load(result)
    assert parsed["temporal_coverage_start_date"] == "1997"
    assert parsed["temporal_coverage_end_date"] == "2019"


def test_build_info_yaml_omits_missing_fields_instead_of_nulling_them():
    result = build_info_yaml(
        {"tables": {}},
        dataset_id="CS0007DS0119",
        collection_id="CS0007",
        raw_dataset_id=None,
        access_level="NONE",
    )
    parsed = yaml.safe_load(result)

    assert parsed == {"ds_id": "CS0007DS0119", "collection_id": "CS0007", "access_level": "NONE"}
    assert "raw_dataset" not in parsed
    assert "title" not in parsed


def test_build_info_yaml_uppercases_spatial_resolution():
    result = build_info_yaml(
        _draft_json(spatialResolution="district"),
        dataset_id="CS0007DS0119", collection_id="CS0007",
        raw_dataset_id=None, access_level="NONE",
    )
    assert yaml.safe_load(result)["spatial_resolution"] == "DISTRICT"


def test_build_info_yaml_leaves_unrecognized_date_format_resolution_blank():
    draft_json = _draft_json()
    draft_json["tables"]["main"]["data_dictionary"]["year"]["format"] = "%Y-%m"
    result = build_info_yaml(
        draft_json, dataset_id="CS0007DS0119", collection_id="CS0007",
        raw_dataset_id=None, access_level="NONE",
    )
    parsed = yaml.safe_load(result)
    assert "temporal_resolution" not in parsed
    # Start/end dates are still derived - only the resolution mapping is unknown.
    assert parsed["temporal_coverage_start_date"] == "1997"
