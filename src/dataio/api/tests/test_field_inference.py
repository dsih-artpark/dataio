from __future__ import annotations

from dataio.api.services.csv_profiler import ColumnProfile, CsvProfile
from dataio.api.services.field_inference import (
    apply_join_keys,
    find_region_lgd_pairs,
    infer_column_type,
    infer_data_dictionary,
    infer_fixed_column_description,
    infer_join_key_candidates,
    lookup_canonical_enum_definitions,
    match_canonical_values,
    suggest_spatial_coverage,
    suggest_spatial_resolution,
    suggest_temporal_coverage,
)


def _column(name, *, dtype="object", null_count=0, distinct_count=2, all_distinct_values=None, row_count=10):
    return ColumnProfile(
        name=name, dtype=dtype, null_count=null_count, null_fraction=(null_count / row_count) if row_count else 0.0,
        distinct_count=distinct_count, sample_values=(all_distinct_values or [])[:5],
        all_distinct_values=all_distinct_values,
    )


def _profile(columns, row_count=10):
    return CsvProfile(path="x.csv", row_count=row_count, columns=columns, sample_rows_csv="")


def test_infer_column_type_bare_year_column_is_date():
    column = _column("year", dtype="int64", all_distinct_values=["2019", "2020"])
    field = infer_column_type(column)
    assert field == {
        "type": "date", "format": "%Y", "nullable": False,
        "allowedValues": ["2019", "2020"], "temporal_axis": "year",
    }


def test_infer_column_type_all_values_four_digit_is_date_even_without_year_name():
    column = _column("survey.year", all_distinct_values=["1997", "2003", "2019"])
    assert infer_column_type(column)["type"] == "date"


def test_infer_column_type_year_column_gets_allowed_values_and_temporal_axis():
    column = _column("year", dtype="int64", all_distinct_values=["1997", "2003", "2019"])
    field = infer_column_type(column)
    assert field["allowedValues"] == ["1997", "2003", "2019"]
    assert field["temporal_axis"] == "year"


def test_infer_column_type_low_cardinality_numeric_column_stays_int_not_enum():
    # Regression: a numeric-dtype column with a small closed set of values
    # (e.g. a real "sourcePage" page-number column with ~150 distinct
    # values) must stay int, not become a bloated enum of every number seen
    # - "enum" is for closed sets of text categories, not numeric ranges.
    values = [str(n) for n in range(1, 50)]
    column = _column("sourcePage", dtype="int64", all_distinct_values=values)
    assert infer_column_type(column)["type"] == "int"


def test_infer_column_type_region_id_shaped_values():
    column = _column("state.ID", all_distinct_values=["state_KA", "state_TG"])
    field = infer_column_type(column)
    assert field["type"] == "regionID"


def test_infer_column_type_id_column_not_region_shaped_falls_through():
    # A plain numeric/freeform ".ID" column (e.g. a row primary key) must
    # NOT be typed regionID just because of the suffix.
    column = _column("record.ID", dtype="int64", all_distinct_values=["1", "2", "3"])
    field = infer_column_type(column)
    assert field["type"] != "regionID"


def test_infer_column_type_name_column_is_region_name():
    column = _column("state.name", all_distinct_values=["Karnataka", "Telangana"])
    assert infer_column_type(column)["type"] == "regionName"


def test_infer_column_type_lgd_code_column_is_int():
    column = _column("state.lgd_code", dtype="int64", all_distinct_values=["29", "36"])
    assert infer_column_type(column)["type"] == "int"


def test_infer_column_type_low_cardinality_becomes_enum_with_ref_name():
    column = _column("species", all_distinct_values=["cattle", "buffalo"])
    field = infer_column_type(column)
    assert field["type"] == "enum"
    assert field["enumRef"] == "speciesEnum"


def test_infer_column_type_enum_ref_name_camel_cases_dotted_and_underscored_names():
    column = _column("table.source_document", all_distinct_values=["a", "b"])
    field = infer_column_type(column)
    assert field["enumRef"] == "sourceDocumentEnum"


def test_infer_column_type_enum_ref_name_preserves_already_camel_case_names():
    # Regression: a column already in camelCase (e.g. the real
    # "sourceDocument"/"sourceTableID" columns in the live livestock census
    # CSVs) must round-trip unchanged, not collapse into one lowercased
    # token ("sourcedocumentEnum").
    assert infer_column_type(_column("sourceDocument", all_distinct_values=["a", "b"]))["enumRef"] == (
        "sourceDocumentEnum"
    )
    assert infer_column_type(_column("sourceTableID", all_distinct_values=["a", "b"]))["enumRef"] == (
        "sourceTableIDEnum"
    )


def test_infer_column_type_high_cardinality_numeric_is_int_or_float():
    int_column = _column("count", dtype="int64", distinct_count=500, all_distinct_values=None)
    float_column = _column("rate", dtype="float64", distinct_count=500, all_distinct_values=None)
    assert infer_column_type(int_column)["type"] == "int"
    assert infer_column_type(float_column)["type"] == "float"


def test_infer_column_type_high_cardinality_text_is_string():
    column = _column("free_text_notes", dtype="object", distinct_count=500, all_distinct_values=None)
    assert infer_column_type(column)["type"] == "string"


def test_infer_column_type_nullable_reflects_null_fraction():
    non_nullable = _column("state.ID", all_distinct_values=["state_KA"], null_count=0)
    nullable = _column("state.ID", all_distinct_values=["state_KA"], null_count=2)
    assert infer_column_type(non_nullable)["nullable"] is False
    assert infer_column_type(nullable)["nullable"] is True


def test_infer_data_dictionary_builds_enum_definitions_for_enum_columns():
    profile = _profile([
        _column("species", all_distinct_values=["cattle", "buffalo"]),
        _column("count", dtype="int64", distinct_count=500, all_distinct_values=None),
    ])
    data_dictionary, enum_definitions = infer_data_dictionary(profile)

    assert set(data_dictionary.keys()) == {"species", "count"}
    assert data_dictionary["species"]["type"] == "enum"
    assert "speciesEnum" in enum_definitions
    assert enum_definitions["speciesEnum"]["values"] == {
        "cattle": {"description": "cattle"}, "buffalo": {"description": "buffalo"},
    }
    assert data_dictionary["count"]["type"] == "int"  # high-cardinality column never became an enum


def test_find_region_lgd_pairs_only_pairs_when_lgd_sibling_exists():
    profile = _profile([
        _column("state.ID", all_distinct_values=["state_KA", "state_TG"]),
        _column("state.lgd_code", dtype="int64", all_distinct_values=["29", "36"]),
        _column("district.ID", all_distinct_values=["district_A"]),  # no district.lgd_code sibling
    ])
    data_dictionary, _ = infer_data_dictionary(profile)

    pairs = find_region_lgd_pairs(profile, data_dictionary)

    assert pairs == [("state.ID", "state.lgd_code")]


def test_find_region_lgd_pairs_ignores_id_columns_not_typed_region_id():
    profile = _profile([
        _column("record.ID", dtype="int64", all_distinct_values=["1", "2"]),
        _column("record.lgd_code", dtype="int64", all_distinct_values=["1", "2"]),
    ])
    data_dictionary, _ = infer_data_dictionary(profile)

    assert find_region_lgd_pairs(profile, data_dictionary) == []


def test_infer_join_key_candidates_combines_region_lgd_pairs_and_date_columns():
    profile = _profile([
        _column("state.ID", all_distinct_values=["state_KA", "state_TG"]),
        _column("state.lgd_code", dtype="int64", all_distinct_values=["29", "36"]),
        _column("year", dtype="int64", all_distinct_values=["2019", "2020"]),
        _column("count", dtype="int64", distinct_count=500, all_distinct_values=None),
    ])
    data_dictionary, _ = infer_data_dictionary(profile)

    candidates = infer_join_key_candidates(profile, data_dictionary)

    assert candidates == ["state.lgd_code", "state.ID", "year"]


def test_infer_join_key_candidates_excludes_source_provenance_year_columns():
    # Regression: "sourcepdf.year" (real column in the BAHS milk/meat CSVs)
    # records the source PDF's own publish year - constant per document, not
    # a dimension of the data - and must not be suggested as a join key just
    # because its bare name is "year".
    profile = _profile([
        _column("year", dtype="int64", all_distinct_values=["2019", "2020"]),
        _column("sourcepdf.year", dtype="int64", all_distinct_values=["2006"]),
    ])
    data_dictionary, _ = infer_data_dictionary(profile)

    candidates = infer_join_key_candidates(profile, data_dictionary)

    assert candidates == ["year"]


def test_apply_join_keys_marks_temporal_vs_composite_component():
    data_dictionary = {
        "year": {"type": "date", "format": "%Y", "nullable": False},
        "state.ID": {"type": "regionID", "nullable": False},
    }

    apply_join_keys(data_dictionary, ["year", "state.ID"])

    assert data_dictionary["year"]["isJoinKey"] is True
    assert data_dictionary["year"]["joinKeyType"] == "temporal"
    assert data_dictionary["state.ID"]["isJoinKey"] is True
    assert data_dictionary["state.ID"]["joinKeyType"] == "compositeComponent"


def test_apply_join_keys_ignores_unknown_column_names():
    data_dictionary = {"year": {"type": "date", "nullable": False}}
    # must not raise even if a candidate name isn't actually in the dict
    apply_join_keys(data_dictionary, ["year", "does_not_exist"])
    assert "does_not_exist" not in data_dictionary


def test_lookup_canonical_enum_definitions_matches_species_column():
    result = lookup_canonical_enum_definitions(["species"])

    assert "canonicalSpecies" in result
    assert "canonicalBreed" not in result
    assert result["canonicalSpecies"]["values"]["bovine"] == {"grain": "group", "components": ["cattle", "buffalo"]}


def test_lookup_canonical_enum_definitions_matches_breed_column_by_bare_name():
    result = lookup_canonical_enum_definitions(["cattle.breed"])

    assert "canonicalBreed" in result
    assert "canonicalSpecies" not in result


def test_lookup_canonical_enum_definitions_returns_empty_for_unrelated_columns():
    assert lookup_canonical_enum_definitions(["state.name", "year", "count"]) == {}


def test_infer_fixed_column_description_region_id_uses_prefix():
    # A prefix with no hardcoded text (e.g. "block") still uses the generic template.
    assert infer_fixed_column_description("block.ID") == "Prefixed LGD identifier for the block."


def test_infer_fixed_column_description_region_name_uses_prefix():
    assert infer_fixed_column_description("block.name") == (
        "Block name standardised to LGD classification."
    )


def test_infer_fixed_column_description_lgd_code_references_sibling_id():
    assert infer_fixed_column_description("block.lgd_code") == (
        "Numeric LGD code extracted from block.ID."
    )


def test_infer_fixed_column_description_district_trio_uses_hardcoded_text():
    assert infer_fixed_column_description("district.ID") == "LGD district code."
    assert infer_fixed_column_description("district.lgd_code") == (
        "Numeric LGD district code extracted from district.ID."
    )
    assert infer_fixed_column_description("district.name") == "District name as per LGD."


def test_infer_fixed_column_description_state_trio_uses_hardcoded_text():
    # state.ID/state.lgd_code/state.name are hardcoded (not the generic
    # <prefix>-templated text) since "state" is the overwhelmingly common
    # region-identifier prefix across real datasets.
    assert infer_fixed_column_description("state.ID") == (
        "LGD-based region identifier in the format state_<lgd_code> for states or "
        "ut_<lgd_code> for union territories (e.g., state_28, ut_1)."
    )
    assert infer_fixed_column_description("state.lgd_code") == (
        "Standard numeric Local Government Directory (LGD) code for the state or union territory."
    )
    assert infer_fixed_column_description("state.name") == (
        "State or union territory name in title case as per LGD."
    )


def test_infer_fixed_column_description_known_source_columns():
    assert infer_fixed_column_description("sourceDocument") is not None
    assert infer_fixed_column_description("sourceTableID") is not None
    assert infer_fixed_column_description("sourcePage") is not None
    assert infer_fixed_column_description("sourcePDFPage") is not None
    assert infer_fixed_column_description("sourceSheetRef") is not None
    assert infer_fixed_column_description("sourceGranularity") is not None


def test_infer_fixed_column_description_unknown_source_column_gets_generic_fallback():
    assert infer_fixed_column_description("sourceWeirdNewField") == (
        "Provenance metadata: sourceWeirdNewField."
    )


def test_infer_fixed_column_description_returns_none_for_domain_columns():
    assert infer_fixed_column_description("species") is None
    assert infer_fixed_column_description("count") is None
    assert infer_fixed_column_description("year") is None


def test_infer_join_key_candidates_includes_enum_dimension_columns():
    profile = _profile([
        _column("year", dtype="int64", all_distinct_values=["2019", "2020"]),
        _column("species", all_distinct_values=["cattle", "buffalo"]),
        _column("sourceDocument", all_distinct_values=["a.pdf", "b.pdf"]),
    ])
    data_dictionary, _ = infer_data_dictionary(profile)

    candidates = infer_join_key_candidates(profile, data_dictionary)

    assert candidates == ["year", "species"]  # sourceDocument (provenance) excluded


def test_match_canonical_values_matches_normalized_keys_and_carries_rollup():
    canonical_definition = {
        "values": {
            "crossbred_exotic": {"grain": "coarse", "components": ["exotic", "crossbred"]},
            "exotic": {"grain": "leaf", "rollup": "crossbred_exotic"},
            "indigenous": {"grain": "leaf"},
        }
    }

    values = ["crossbred/exotic", "exotic", "unmatched value"]
    result = match_canonical_values(values, canonical_definition)

    assert result["crossbred/exotic"] == {
        "canonical": "crossbred_exotic", "canonicalRollup": "crossbred_exotic",
    }
    assert result["exotic"] == {"canonical": "exotic", "canonicalRollup": "crossbred_exotic"}
    assert "unmatched value" not in result


def test_match_canonical_values_defaults_rollup_to_own_key_when_no_rollup_field():
    canonical_definition = {"values": {"indigenous": {"grain": "leaf"}}}

    result = match_canonical_values(["indigenous"], canonical_definition)

    assert result["indigenous"] == {"canonical": "indigenous", "canonicalRollup": "indigenous"}


def test_suggest_spatial_coverage_is_india_for_state_region_column():
    data_dictionary = {"state.name": {"type": "regionName"}, "count": {"type": "int"}}

    assert suggest_spatial_coverage(data_dictionary) == "India"


def test_suggest_spatial_coverage_none_without_state_or_ut_column():
    data_dictionary = {"district.name": {"type": "regionName"}, "count": {"type": "int"}}

    assert suggest_spatial_coverage(data_dictionary) is None


def test_suggest_spatial_resolution_picks_finest_grain_present():
    data_dictionary = {
        "state.name": {"type": "regionName"},
        "district.name": {"type": "regionName"},
        "count": {"type": "int"},
    }

    assert suggest_spatial_resolution(data_dictionary) == "district"


def test_suggest_spatial_resolution_none_without_region_name_column():
    data_dictionary = {"state.ID": {"type": "regionID"}, "count": {"type": "int"}}

    assert suggest_spatial_resolution(data_dictionary) is None


def test_suggest_spatial_resolution_unrecognized_prefix_still_returned():
    data_dictionary = {"block.name": {"type": "regionName"}}

    assert suggest_spatial_resolution(data_dictionary) == "block"


def test_suggest_temporal_coverage_joins_observed_years():
    data_dictionary = {
        "year": {"type": "date", "format": "%Y", "allowedValues": ["1997", "2003", "2019"]},
        "count": {"type": "int"},
    }

    assert suggest_temporal_coverage(data_dictionary) == "1997, 2003, 2019"


def test_suggest_temporal_coverage_none_without_year_column():
    data_dictionary = {"count": {"type": "int"}}

    assert suggest_temporal_coverage(data_dictionary) is None
