from __future__ import annotations

import json

import pytest

from dataio.validate.contracts.models import ValidationRequest
from dataio.validate.reports.models import Finding, ValidationResult
from dataio.validate.sdk import DataIOValidator
from dataio.validate.service import DataIOValidationService

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
datasetID: CS0007DS0112
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
        format: "%Y"
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
datasetSlug: gs0012ds0001-gs-states
datasetDescription: Region boundaries
source: Gazette
category:
  ID: GS
  name: Geospatial
collection:
  ID: GS0012
  name: Shapes
datasetID: GS0012DS0001
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
        format: "%Y-%m-%d"
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


def test_valid_tabular_manifest_and_inline_data_pass(tmp_path):
    csv_path = tmp_path / "livestock.csv"
    csv_text = "state.ID,year,species,count\nstate_29,2024,cattle,10\n"
    csv_path.write_text(csv_text, encoding="utf-8")
    manifest = VALID_TABULAR_MANIFEST.replace("REPLACEME", str(csv_path))

    result = DataIOValidator().validate_tabular(
        manifest=manifest,
        data_files={"livestock": csv_text},
    )

    assert result.status == "pass"
    assert result.summary.errors == 0


def test_valid_tabular_manifest_and_long_inline_data_pass(tmp_path):
    csv_path = tmp_path / "livestock.csv"
    header = "state.ID,year,species,count\n"
    rows = "".join("state_29,2024,cattle,10\n" for _ in range(500))
    csv_text = header + rows
    csv_path.write_text(csv_text, encoding="utf-8")
    manifest = VALID_TABULAR_MANIFEST.replace("REPLACEME", str(csv_path))

    result = DataIOValidator().validate_tabular(
        manifest=manifest,
        data_files={"livestock": csv_text},
    )

    assert result.status == "pass"
    assert result.summary.errors == 0


def test_invalid_field_contract_fails():
    manifest = """
metadataSpecVersion: v2
datasetTitle: Bad Schema
datasetSlug: ts0001ds0001-bad-schema
datasetDescription: Bad manifest
source: Test
category: {ID: TS, name: Test}
collection: {ID: TS0001, name: Tests}
datasetID: TS0001DS0001
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
datasetSlug: ts0001ds0001-enum-schema
datasetDescription: Example
source: Test
category: {{ID: TS, name: Test}}
collection: {{ID: TS0001, name: Tests}}
datasetID: TS0001DS0001
datasetKind: tabular
datasetTables:
  sample:
    path: {csv_path}
    dataDictionary:
      year:
        type: date
        format: "%Y"
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
datasetSlug: ts0001ds0001-region-schema
datasetDescription: Example
source: Test
category: {{ID: TS, name: Test}}
collection: {{ID: TS0001, name: Tests}}
datasetID: TS0001DS0001
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


def test_invalid_slug_fails():
    manifest = """
metadataSpecVersion: v2
datasetTitle: Slug Manifest
datasetSlug: bad-slug
datasetDescription: Example
source: Test
category: {ID: TS, name: Test}
collection: {ID: TS0001, name: Tests}
datasetID: TS0001DS0001
datasetKind: tabular
datasetTables:
  sample:
    dataDictionary:
      year:
        type: date
        format: "%Y"
        nullable: false
"""
    result = DataIOValidator().validate_tabular(manifest=manifest, data_files={})
    assert any(f.code == "invalid_dataset_slug" for f in result.findings)


def test_datetime_format_requires_timezone():
    manifest = """
metadataSpecVersion: v2
datasetTitle: Datetime Manifest
datasetSlug: ts0001ds0001-datetime-manifest
datasetDescription: Example
source: Test
category: {ID: TS, name: Test}
collection: {ID: TS0001, name: Tests}
datasetID: TS0001DS0001
datasetKind: geojson
datasetTables:
  features:
    dataDictionary:
      id:
        type: regionID
        nullable: false
      observedAt:
        type: dateTime
        format: "%Y-%m-%dT%H:%M:%S"
        nullable: false
"""
    result = DataIOValidator().validate_geojson(
        manifest=manifest,
        data={"type": "FeatureCollection", "features": []},
    )
    assert any(f.code == "invalid_manifest" for f in result.findings)


def test_platform_checker_can_add_deep_check_findings():
    manifest = """
metadataSpecVersion: v2
datasetTitle: Deep Check Manifest
datasetSlug: ts0001ds0001-deep-check-manifest
datasetDescription: Example
source: Test
category: {ID: TS, name: Test}
collection: {ID: TS0001, name: Wrong Collection}
datasetID: TS0001DS0001
datasetKind: tabular
datasetTables:
  sample:
    dataDictionary:
      year:
        type: date
        format: "%Y"
        nullable: false
"""
    service = DataIOValidationService(
        platform_manifest_checker=lambda _manifest, result: result.add_finding(
            Finding(
                severity="error",
                code="collection_name_mismatch",
                message="collection.name does not match the database record.",
                path="collection.name",
                rule_id="collection_name_matches_db",
            )
        )
    )

    result = service.validate(
        ValidationRequest(
            dataset_kind="tabular",
            manifest_source=manifest,
            data_files={},
            deep_check=True,
        )
    )
    assert any(f.code == "collection_name_mismatch" for f in result.findings)


def test_deep_check_requires_api_access():
    with pytest.raises(ValueError, match="deep_check requires API access"):
        DataIOValidator().validate_tabular(
            manifest=VALID_TABULAR_MANIFEST.replace("REPLACEME", "sample.csv"),
            data_files={"livestock": "state.ID,year,species,count\nstate_29,2024,cattle,10\n"},
            deep_check=True,
        )


def test_deep_check_uses_api_for_tabular(monkeypatch, tmp_path):
    csv_path = tmp_path / "livestock.csv"
    csv_path.write_text("state.ID,year,species,count\nstate_29,2024,cattle,10\n", encoding="utf-8")
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(
        VALID_TABULAR_MANIFEST.replace("REPLACEME", str(csv_path)),
        encoding="utf-8",
    )
    recorded: dict[str, object] = {}

    class DummyResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return ValidationResult(dataset_kind="tabular").model_dump()

    def fake_post(url: str, **kwargs):
        recorded["url"] = url
        recorded["files"] = kwargs["files"]
        recorded["data"] = kwargs["data"]
        return DummyResponse()

    validator = DataIOValidator(api_base_url="http://example.test/api/v1")
    monkeypatch.setattr(validator.session, "post", fake_post)

    result = validator.validate_tabular(
        manifest=str(manifest_path),
        data_files={"livestock": str(csv_path)},
        deep_check=True,
    )

    assert result.status == "pass"
    assert recorded["url"] == "http://example.test/api/v1/validate"
    assert json.loads(recorded["data"]["data_files"]) == {
        "livestock": "state.ID,year,species,count\nstate_29,2024,cattle,10\n"
    }
