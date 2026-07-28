from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from dataio.api.services import deterministic_draft_service, draft_service
from dataio.api.services.deterministic_draft_service import CuratorMetadataInput, TagsInput
from dataio.validate.reports.models import ValidationResult

FAKE_COLLECTION = SimpleNamespace(
    collection_id="CS0007", collection_name="Livestock Census (by DAHD)",
    category_id="CS", category_name="Census and Surveys",
)

CSV_TEXT = """state.ID,state.lgd_code,state.name,year,species,count
state_KA,29,Karnataka,2019,cattle,120
state_TG,36,Telangana,2019,buffalo,80
"""


def _curator_input(**overrides):
    defaults = dict(
        datasetDescription="State-level livestock counts.",
        source=["16th Livestock Census"],
        references=["https://example.gov/report.pdf"],
        tags=TagsInput(concept=["livestock"], epiType=["population"]),
        spatialCoverage="India",
        spatialResolution="state",
        temporalCoverage="2019",
        temporalResolution="annual",
        updateFrequency="Annual",
        comments=["Curator-supplied fact."],
        joinKeyColumns=[],
        tableDescriptions={"table": "State-level livestock counts for this table."},
        # state.ID/state.lgd_code/state.name are auto-filled by
        # infer_fixed_column_description - only year/species/count need a
        # curator-supplied description for the CSV_TEXT columns.
        columnDescriptions={
            "table": {
                "year": "Census reference year.",
                "species": "Livestock species.",
                "count": "Number of animals counted.",
            },
        },
    )
    defaults.update(overrides)
    return CuratorMetadataInput(**defaults)


def _patch_common(monkeypatch):
    monkeypatch.setattr(draft_service, "get_collection_by_identifier", lambda collection_id: FAKE_COLLECTION)
    monkeypatch.setattr(
        draft_service, "_validate_manifest",
        lambda manifest_dict, csv_paths_by_table: ValidationResult(dataset_kind="tabular"),
    )
    monkeypatch.setattr(draft_service, "suggest_next_dataset_id", lambda collection_id: "CS0007DS0999")
    monkeypatch.setattr(draft_service, "create_reserved_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(draft_service, "create_reserved_raw_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(
        "dataio.api.database.rds_id_helpers.suggest_next_raw_dataset_id_for_category",
        lambda category_id: "CSRDS0099",
    )


def _fake_create_manifest_draft(recorded):
    def create(**kwargs):
        recorded.update(kwargs)
        return SimpleNamespace(
            draft_id="11111111-1111-1111-1111-111111111111",
            status=SimpleNamespace(value="pending"),
            draft_yaml=kwargs["draft_yaml"],
            draft_json=kwargs["draft_json"],
            flagged_fields=kwargs["flagged_fields"],
        )
    return create


def test_generate_deterministic_draft_infers_types_and_persists_no_llm_model_id(tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "consolidated-livestock-census.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)
    recorded: dict = {}
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", _fake_create_manifest_draft(recorded))

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD",
        curator_input=_curator_input(
            tableDescriptions={"consolidated-livestock-census": "State-level livestock counts."},
            columnDescriptions={
                "consolidated-livestock-census": {
                    "year": "Census reference year.",
                    "species": "Livestock species.",
                    "count": "Number of animals counted.",
                },
            },
        ),
    )

    assert result.status == "pending"
    assert result.validation_status == "pass"
    assert recorded["llm_model_id"] is None  # deterministic, not LLM-authored
    assert result.draft_json["datasetID"] == "CS0007DS0999"
    assert result.draft_json["datasetOwner"] == "DAHD"
    assert result.draft_json["lastUpdated"] == date.today().isoformat()

    table = result.draft_json["tables"]["consolidated-livestock-census"]
    assert table["description"] == "State-level livestock counts."
    data_dictionary = table["data_dictionary"]
    assert data_dictionary["state.ID"]["type"] == "regionID"
    assert data_dictionary["state.name"]["type"] == "regionName"
    assert data_dictionary["state.lgd_code"]["type"] == "int"
    assert data_dictionary["year"]["type"] == "date"
    assert data_dictionary["year"]["format"] == "%Y"
    assert data_dictionary["species"]["type"] == "enum"

    # Fixed-pattern columns get an auto-filled description; the curator was
    # never asked for one.
    assert data_dictionary["state.ID"]["description"].startswith("LGD-based region identifier")
    assert data_dictionary["species"]["description"] == "Livestock species."


def test_generate_deterministic_draft_uses_curator_supplied_narrative_fields(tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}))

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD", curator_input=_curator_input(datasetDescription="Custom description."),
    )

    assert result.draft_json["datasetDescription"] == "Custom description."
    assert result.draft_json["source"] == ["16th Livestock Census"]
    assert result.draft_json["tags"] == {"concept": ["livestock"], "epiType": ["population"]}
    assert "Curator-supplied fact." in result.draft_json["comments"]


def test_generate_deterministic_draft_auto_applies_region_lgd_join_keys_when_curator_leaves_them_unset(
    tmp_path: Path, monkeypatch
):
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}))

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD", curator_input=_curator_input(joinKeyColumns=[]),
    )

    table = result.draft_json["tables"]["table"]
    # state.ID must never appear in joinKeys without its lgd_code sibling.
    assert "state.lgd_code" in table["joinKeys"]
    assert "state.ID" in table["joinKeys"]
    assert table["data_dictionary"]["state.ID"]["isJoinKey"] is True
    assert table["data_dictionary"]["state.ID"]["joinKeyType"] == "compositeComponent"
    assert table["data_dictionary"]["year"]["joinKeyType"] == "temporal"


def test_generate_deterministic_draft_respects_curator_confirmed_join_keys(tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}))

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD", curator_input=_curator_input(joinKeyColumns=["year"]),
    )

    assert result.draft_json["joinKeys"] == ["year"]
    table = result.draft_json["tables"]["table"]
    assert table["joinKeys"] == ["year"]
    assert "isJoinKey" not in table["data_dictionary"]["state.ID"]


def test_generate_deterministic_draft_includes_canonical_species_registry(tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}))

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD", curator_input=_curator_input(),
    )

    assert "canonicalSpecies" in result.draft_json["canonicalEnumDefinitions"]
    assert "canonicalBreed" not in result.draft_json["canonicalEnumDefinitions"]


def test_generate_deterministic_draft_cross_links_enum_values_to_canonical_registry(
    tmp_path: Path, monkeypatch
):
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")  # species column has values cattle/buffalo

    _patch_common(monkeypatch)
    monkeypatch.setattr(
        draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}),
    )

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007",
        created_by="engineer@artpark.in", data_owner_name="DAHD", curator_input=_curator_input(),
    )

    species_values = result.draft_json["enumDefinitions"]["speciesEnum"]["values"]
    assert species_values["cattle"]["canonical"] == "cattle"
    assert species_values["cattle"]["canonicalRollup"] == "cattle"
    assert species_values["buffalo"]["canonical"] == "buffalo"


def test_generate_deterministic_draft_broadens_join_keys_to_enum_dimension_columns(
    tmp_path: Path, monkeypatch
):
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)
    monkeypatch.setattr(
        draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}),
    )

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007",
        created_by="engineer@artpark.in", data_owner_name="DAHD",
        curator_input=_curator_input(joinKeyColumns=[]),
    )

    table = result.draft_json["tables"]["table"]
    assert "species" in table["joinKeys"]
    assert table["data_dictionary"]["species"]["isJoinKey"] is True
    assert table["data_dictionary"]["species"]["joinKeyType"] == "compositeComponent"


def test_generate_deterministic_draft_flags_missing_source_columns(tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")  # no sourceDocument/sourceTableID/sourcePage columns

    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}))

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD", curator_input=_curator_input(),
    )

    fields_flagged = {f["field"] for f in result.flagged_fields}
    assert {"sourceDocument", "sourceTableID", "sourcePage"} <= fields_flagged


def test_generate_deterministic_draft_detects_telangana_region_gap(tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")  # includes a 2019 Telangana row - matches real history

    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}))

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD", curator_input=_curator_input(),
    )

    assert any("Telangana" in c for c in result.draft_json["comments"])


def test_generate_deterministic_draft_requires_dataset_title_for_multiple_csvs(tmp_path: Path, monkeypatch):
    import pytest
    from fastapi import HTTPException

    csv_path_1 = tmp_path / "table-one.csv"
    csv_path_1.write_text(CSV_TEXT, encoding="utf-8")
    csv_path_2 = tmp_path / "table-two.csv"
    csv_path_2.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)

    with pytest.raises(HTTPException) as exc_info:
        deterministic_draft_service.generate_deterministic_draft(
            csv_paths=[str(csv_path_1), str(csv_path_2)], category_id="CS", collection_id="CS0007",
            created_by="engineer@artpark.in", data_owner_name="DAHD",
            curator_input=_curator_input(
                tableDescriptions={"table-one": "Table one.", "table-two": "Table two."},
                columnDescriptions={
                    "table-one": {"year": "y", "species": "s", "count": "c"},
                    "table-two": {"year": "y", "species": "s", "count": "c"},
                },
            ),
        )
    assert exc_info.value.status_code == 400
    assert "title" in exc_info.value.detail.lower()


def test_generate_deterministic_draft_uses_curator_supplied_title_for_multiple_csvs(
    tmp_path: Path, monkeypatch
):
    csv_path_1 = tmp_path / "table-one.csv"
    csv_path_1.write_text(CSV_TEXT, encoding="utf-8")
    csv_path_2 = tmp_path / "table-two.csv"
    csv_path_2.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)
    monkeypatch.setattr(
        draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}),
    )

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path_1), str(csv_path_2)], category_id="CS", collection_id="CS0007",
        created_by="engineer@artpark.in", data_owner_name="DAHD",
        curator_input=_curator_input(
            datasetTitle="my-combined-dataset",
            tableDescriptions={"table-one": "Table one.", "table-two": "Table two."},
            columnDescriptions={
                "table-one": {"year": "y", "species": "s", "count": "c"},
                "table-two": {"year": "y", "species": "s", "count": "c"},
            },
        ),
    )

    assert result.draft_json["datasetTitle"] == "my-combined-dataset"


def test_generate_deterministic_draft_ignores_dataset_title_for_a_single_csv(tmp_path: Path, monkeypatch):
    # A single-CSV dataset is always named after that CSV's own filename,
    # matching the established hand-authored convention - a curator-supplied
    # datasetTitle only takes effect with 2+ CSVs.
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}))

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD", curator_input=_curator_input(datasetTitle="ignored-title"),
    )

    assert result.draft_json["datasetTitle"] == "table"


def test_generate_deterministic_draft_disambiguates_colliding_enum_refs_across_tables(
    tmp_path: Path, monkeypatch
):
    # Regression: two tables each have their own "indicator" column with
    # completely disjoint values - _enum_ref_name gives both the same
    # "indicatorEnum" ref since it only looks at the bare column name.
    # Blindly merging must not let table two's values silently overwrite
    # table one's in the final enumDefinitions (which would make every row
    # of table one fail validation for a value that is actually valid).
    columns = "state.ID,state.lgd_code,state.name,year,indicator,count"
    csv_text_a = f"{columns}\nstate_KA,29,Karnataka,2019,alpha,1\n"
    csv_text_b = f"{columns}\nstate_KA,29,Karnataka,2019,gamma,1\n"
    csv_path_a = tmp_path / "table-a.csv"
    csv_path_a.write_text(csv_text_a, encoding="utf-8")
    csv_path_b = tmp_path / "table-b.csv"
    csv_path_b.write_text(csv_text_b, encoding="utf-8")

    _patch_common(monkeypatch)
    monkeypatch.setattr(
        draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}),
    )

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path_a), str(csv_path_b)], category_id="CS", collection_id="CS0007",
        created_by="engineer@artpark.in", data_owner_name="DAHD",
        curator_input=_curator_input(
            datasetTitle="two-table-dataset",
            tableDescriptions={"table-a": "Table A.", "table-b": "Table B."},
            columnDescriptions={
                "table-a": {"year": "y", "indicator": "i", "count": "c"},
                "table-b": {"year": "y", "indicator": "i", "count": "c"},
            },
        ),
    )

    data_dictionary_a = result.draft_json["tables"]["table-a"]["data_dictionary"]
    data_dictionary_b = result.draft_json["tables"]["table-b"]["data_dictionary"]
    assert data_dictionary_a["indicator"]["enumRef"] == "indicatorEnum"
    assert data_dictionary_b["indicator"]["enumRef"] == "indicatorEnum2"

    enum_definitions = result.draft_json["enumDefinitions"]
    assert set(enum_definitions["indicatorEnum"]["values"]) == {"alpha"}
    assert set(enum_definitions["indicatorEnum2"]["values"]) == {"gamma"}


def test_generate_deterministic_draft_merges_identical_enums_across_tables_under_one_name(
    tmp_path: Path, monkeypatch
):
    # Two tables sharing a column with the *same* observed values (a common,
    # legitimate case - e.g. shared locality categories) must still merge
    # under the one ref, not get needlessly disambiguated.
    columns = "state.ID,state.lgd_code,state.name,year,indicator,count"
    csv_text = f"{columns}\nstate_KA,29,Karnataka,2019,alpha,1\n"
    csv_path_a = tmp_path / "table-a.csv"
    csv_path_a.write_text(csv_text, encoding="utf-8")
    csv_path_b = tmp_path / "table-b.csv"
    csv_path_b.write_text(csv_text, encoding="utf-8")

    _patch_common(monkeypatch)
    monkeypatch.setattr(
        draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}),
    )

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path_a), str(csv_path_b)], category_id="CS", collection_id="CS0007",
        created_by="engineer@artpark.in", data_owner_name="DAHD",
        curator_input=_curator_input(
            datasetTitle="two-table-dataset",
            tableDescriptions={"table-a": "Table A.", "table-b": "Table B."},
            columnDescriptions={
                "table-a": {"year": "y", "indicator": "i", "count": "c"},
                "table-b": {"year": "y", "indicator": "i", "count": "c"},
            },
        ),
    )

    data_dictionary_a = result.draft_json["tables"]["table-a"]["data_dictionary"]
    data_dictionary_b = result.draft_json["tables"]["table-b"]["data_dictionary"]
    assert data_dictionary_a["indicator"]["enumRef"] == "indicatorEnum"
    assert data_dictionary_b["indicator"]["enumRef"] == "indicatorEnum"
    assert "indicatorEnum2" not in result.draft_json["enumDefinitions"]


def test_generate_deterministic_draft_raises_when_no_csv_paths(monkeypatch):
    import pytest
    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        deterministic_draft_service.generate_deterministic_draft(
            csv_paths=[], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
            data_owner_name="DAHD", curator_input=_curator_input(),
        )


def test_generate_deterministic_draft_raises_when_collection_does_not_exist(tmp_path: Path, monkeypatch):
    import pytest
    from fastapi import HTTPException

    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service, "get_collection_by_identifier", lambda collection_id: None)

    with pytest.raises(HTTPException) as exc_info:
        deterministic_draft_service.generate_deterministic_draft(
            csv_paths=[str(csv_path)], category_id="CS", collection_id="CS9999", created_by="engineer@artpark.in",
            data_owner_name="DAHD", curator_input=_curator_input(),
        )
    assert exc_info.value.status_code == 400


def test_generate_deterministic_draft_raises_when_table_description_missing(tmp_path: Path, monkeypatch):
    import pytest
    from fastapi import HTTPException

    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)

    with pytest.raises(HTTPException) as exc_info:
        deterministic_draft_service.generate_deterministic_draft(
            csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
            data_owner_name="DAHD", curator_input=_curator_input(tableDescriptions={}),
        )
    assert exc_info.value.status_code == 400
    assert "table" in exc_info.value.detail


def test_generate_deterministic_draft_raises_when_table_description_is_blank(tmp_path: Path, monkeypatch):
    import pytest
    from fastapi import HTTPException

    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)

    with pytest.raises(HTTPException) as exc_info:
        deterministic_draft_service.generate_deterministic_draft(
            csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
            data_owner_name="DAHD", curator_input=_curator_input(tableDescriptions={"table": "   "}),
        )
    assert exc_info.value.status_code == 400


def test_generate_deterministic_draft_raises_when_column_description_missing(tmp_path: Path, monkeypatch):
    import pytest
    from fastapi import HTTPException

    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)

    with pytest.raises(HTTPException) as exc_info:
        deterministic_draft_service.generate_deterministic_draft(
            csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
            data_owner_name="DAHD",
            curator_input=_curator_input(columnDescriptions={"table": {"year": "Census reference year."}}),
        )
    assert exc_info.value.status_code == 400
    assert "table.species" in exc_info.value.detail
    assert "table.count" in exc_info.value.detail
    # state.ID/state.lgd_code/state.name are fixed-pattern - never required.
    assert "state.ID" not in exc_info.value.detail


def test_generate_deterministic_draft_never_requires_a_fixed_pattern_column(tmp_path: Path, monkeypatch):
    """Region-identifier and source/provenance columns are auto-filled by
    infer_fixed_column_description regardless of what (if anything) the
    curator supplied for them - generation must not block on these even
    when columnDescriptions is completely empty for the table.
    """
    csv_path = tmp_path / "table.csv"
    csv_path.write_text(CSV_TEXT, encoding="utf-8")

    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", _fake_create_manifest_draft({}))

    result = deterministic_draft_service.generate_deterministic_draft(
        csv_paths=[str(csv_path)], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD",
        curator_input=_curator_input(
            columnDescriptions={
                "table": {
                    "year": "Census reference year.",
                    "species": "Livestock species.",
                    "count": "Number of animals counted.",
                    # state.ID/state.name/state.lgd_code deliberately omitted.
                },
            },
        ),
    )

    data_dictionary = result.draft_json["tables"]["table"]["data_dictionary"]
    assert data_dictionary["state.ID"]["description"].startswith("LGD-based region identifier")
    assert data_dictionary["state.name"]["description"] == "State or union territory name in title case as per LGD."
    assert data_dictionary["state.lgd_code"]["description"] == (
        "Standard numeric Local Government Directory (LGD) code for the state or union territory."
    )
