from __future__ import annotations

import json

from dataio.validate.sdk import DataIOValidator

VALID_TABULAR_MANIFEST = """
metadataSpecVersion: v2
datasetTitle: Livestock Census
datasetSlug: cs0007ds0112-livestock-census
datasetDescription: Population counts by state and year.
source: Census report
category:
  ID: CS
  name: Census & Surveys
collection:
  ID: CS0007
  name: Livestock Census
datasetKind: tabular
datasetTables:
  livestock:
    description: Livestock counts
    path: REPLACEME
    dataDictionary:
      state.ID:
        type: regionID
        nullable: false
      year:
        type: date
        format: YYYY
        nullable: false
      species:
        type: enum
        enumRef: livestockSpecies
        nullable: false
      count:
        type: int
        range: [0, 1000]
        nullable: false
enumDefinitions:
  livestockSpecies:
    description: Species vocabulary
    values:
      cattle:
        description: cattle
      buffalo:
        description: buffalo
"""


GEOJSON_MANIFEST = """
metadataSpecVersion: v2
datasetTitle: India States
datasetSlug: gs-states
datasetDescription: Region boundaries
source: Gazette
category:
  ID: GS
  name: Geospatial
collection:
  ID: GS0012
  name: Shapes
datasetKind: geojson
datasetTables:
  features:
    dataDictionary:
      id:
        type: regionID
        nullable: false
      properties.regionType:
        type: enum
        allowedValues: [country, state, district]
        nullable: false
      properties.lastUpdated:
        type: date
        format: YYYY-MM-DD
        nullable: false
"""


def test_valid_tabular_manifest_and_data_pass(tmp_path):
    csv_path = tmp_path / "livestock.csv"
    csv_path.write_text("state.ID,year,species,count\nstate_29,2024,cattle,10\n", encoding="utf-8")
    manifest = VALID_TABULAR_MANIFEST.replace("REPLACEME", str(csv_path))

    result = DataIOValidator().validate_tabular(
        manifest=manifest,
        data_files={"livestock": str(csv_path)},
    )

    assert result.status == "pass"
    assert result.summary.errors == 0


def test_invalid_field_contract_fails():
    manifest = """
metadataSpecVersion: v2
datasetTitle: Bad Schema
datasetSlug: bad-schema
datasetDescription: Bad manifest
source: Test
category: {ID: TS, name: Test}
collection: {ID: TS0001, name: Tests}
datasetKind: tabular
datasetTables:
  sample:
    dataDictionary:
      value:
        type: int
        range: [0, 10]
        min: 0
"""
    result = DataIOValidator().validate_tabular(manifest=manifest, data_files={})
    assert result.status == "fail"
    assert any(f.code == "invalid_manifest" for f in result.findings)


def test_unresolved_enum_ref_fails(tmp_path):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("year,kind\n2024,a\n", encoding="utf-8")
    manifest = f"""
metadataSpecVersion: v2
datasetTitle: Enum Manifest
datasetSlug: enum-schema
datasetDescription: Example
source: Test
category: {{ID: TS, name: Test}}
collection: {{ID: TS0001, name: Tests}}
datasetKind: tabular
datasetTables:
  sample:
    path: {csv_path}
    dataDictionary:
      year:
        type: date
        format: YYYY
        nullable: false
      kind:
        type: enum
        enumRef: unknownVocabulary
        nullable: false
"""
    result = DataIOValidator().validate_tabular(
        manifest=manifest,
        data_files={"sample": str(csv_path)},
    )
    assert any(f.code == "unknown_enum_reference" for f in result.findings)


def test_region_manifest_without_year_fails(tmp_path):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("district.ID\nstate_29\n", encoding="utf-8")
    manifest = f"""
metadataSpecVersion: v2
datasetTitle: Region Manifest
datasetSlug: region-schema
datasetDescription: Example
source: Test
category: {{ID: TS, name: Test}}
collection: {{ID: TS0001, name: Tests}}
datasetKind: tabular
datasetTables:
  sample:
    path: {csv_path}
    dataDictionary:
      district.ID:
        type: regionID
        nullable: false
"""
    result = DataIOValidator().validate_tabular(
        manifest=manifest,
        data_files={"sample": str(csv_path)},
    )
    assert any(f.code == "missing_temporal_context" for f in result.findings)


def test_geojson_invalid_feature_fails():
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "state_29",
                "properties": {"regionType": "unknown", "lastUpdated": "2024-01-01"},
                "geometry": {"type": "Polygon", "coordinates": []},
            }
        ],
    }
    result = DataIOValidator().validate_geojson(
        manifest=GEOJSON_MANIFEST,
        data=json.dumps(geojson),
    )
    assert result.status == "fail"
    assert any(f.code == "type_validation_failed" for f in result.findings)
