from __future__ import annotations

import pytest

from dataio.api.services.csv_profiler import ColumnProfile, CsvProfile
from dataio.api.services.digitization_log import DigitizationLog, Observation, NormalizationStep
from dataio.api.services.draft_prompt import build_batch_prompt, build_prompt, parse_llm_output


def _sample_profile(missing_source_columns=None):
    return CsvProfile(
        path="x.csv",
        row_count=10,
        columns=[
            ColumnProfile(
                name="state", dtype="object", null_count=0, null_fraction=0.0,
                distinct_count=3, sample_values=["Karnataka", "Kerala"],
            )
        ],
        missing_source_columns=missing_source_columns or [],
        sample_rows_csv="state,year\nKarnataka,2019\n",
    )


def _sample_deterministic_base(table_name: str, data_dictionary: dict, enum_definitions: dict | None = None) -> dict:
    """Matches the shape draft_service._infer_deterministic_base produces -
    a fixture for tests that need to exercise build_prompt/build_batch_prompt's
    deterministic-context rendering without going through the full inference
    pipeline.
    """
    return {
        "tables": {table_name: {"joinKeys": [], "data_dictionary": data_dictionary}},
        "enum_definitions": enum_definitions or {},
        "canonical_enum_definitions": {},
        "join_keys": [],
        "region_gap_comments": [],
    }


def test_build_prompt_includes_missing_source_columns():
    profile = _sample_profile(missing_source_columns=["sourceTableID", "sourcePage"])
    system_prompt, user_prompt = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "sourceTableID" in user_prompt
    assert "do NOT invent" in system_prompt or "Do NOT invent" in system_prompt


def test_build_prompt_includes_expected_observations_and_do_not_reflag_instruction():
    log = DigitizationLog(
        observations=[
            Observation(id="obs-1", description="Telangana absent pre-2014", resolution="expected"),
            Observation(id="obs-2", description="unexplained negative values", resolution="needs_investigation"),
        ],
        normalizationSteps=[
            NormalizationStep(id="norm-1", description="renamed column", field="district_name", changeType="rename"),
        ],
    )
    profile = _sample_profile()
    system_prompt, user_prompt = build_prompt(csv_profiles={"main": profile}, digitization_log=log)

    assert "obs-1" in user_prompt
    assert "Telangana" in user_prompt
    assert "norm-1" in user_prompt
    assert "obs-2" in user_prompt  # still surfaced, under "unresolved"
    assert "do not re-flag" in system_prompt.lower()


def test_build_prompt_never_embeds_raw_csv_row_data():
    """Full CSV text is never sent to the LLM anymore - only the bounded
    profile summary (stats + up to 20 sample rows, from _format_csv_profile)
    plus the deterministic-context annotation. This is the fix for the
    real production bug where a large table's full text alone blew past
    the model's token limit.
    """
    profile = _sample_profile()
    _, user_prompt = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "Full CSV contents" not in user_prompt


def test_build_prompt_includes_deterministic_context_when_provided():
    profile = _sample_profile()
    deterministic_base = _sample_deterministic_base(
        "main", {"state": {"type": "regionName", "nullable": False, "description": None}}
    )
    _, user_prompt = build_prompt(
        csv_profiles={"main": profile}, digitization_log=None, deterministic_base=deterministic_base
    )

    assert "columns needing a `description`" in user_prompt
    assert "state: type=regionName" in user_prompt


def test_build_prompt_omits_columns_that_already_have_a_fixed_description():
    """A column the deterministic pre-pass already gave a real description
    to (region identifiers, source-provenance columns) must not be listed
    as needing one - the LLM shouldn't waste a turn redoing settled work.
    """
    profile = _sample_profile()
    deterministic_base = _sample_deterministic_base(
        "main",
        {
            "state.ID": {"type": "regionID", "nullable": False, "description": "Prefixed LGD identifier."},
            "count": {"type": "int", "nullable": True, "description": None},
        },
    )
    _, user_prompt = build_prompt(
        csv_profiles={"main": profile}, digitization_log=None, deterministic_base=deterministic_base
    )

    assert "state.ID" not in user_prompt.split("columns needing a `description`")[1].split("\n\n")[0]
    assert "count: type=int" in user_prompt


def test_build_prompt_includes_enum_context_for_enum_typed_columns():
    profile = _sample_profile()
    deterministic_base = _sample_deterministic_base(
        "main",
        {"species": {"type": "enum", "enumRef": "speciesEnum", "nullable": False, "description": None}},
        enum_definitions={"speciesEnum": {"description": "x", "values": {"cattle": {"description": "cattle"}}}},
    )
    _, user_prompt = build_prompt(
        csv_profiles={"main": profile}, digitization_log=None, deterministic_base=deterministic_base
    )

    assert "Enum blocks used by this table needing definitions" in user_prompt
    assert "speciesEnum: values = ['cattle']" in user_prompt


def test_system_prompt_no_longer_asks_for_column_typing():
    """type/nullable/format/joinKeys/canonicalEnumDefinitions are all
    decided deterministically now (field_inference.py) - the LLM is told
    NOT to produce them (rather than given typing rules for how to produce
    them itself), and the old regionID/LGD-pairing typing rules are gone
    entirely.
    """
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "regionID" not in system_prompt
    assert "lgd_code" not in system_prompt
    assert "do NOT include" in system_prompt
    assert "isJoinKey, or joinKeyType inside any" in system_prompt


def test_system_prompt_tells_llm_not_to_produce_join_keys_or_canonical_enum_definitions():
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "do not produce joinkeys or canonicalenumdefinitions" in system_prompt.lower()


def test_system_prompt_no_longer_embeds_inl98_registry_text():
    """canonicalEnumDefinitions (including the hardcoded INL-98 livestock
    species/breed registry) is now looked up deterministically
    (field_inference.lookup_canonical_enum_definitions against
    canonical_enum_registry.yaml) - the LLM no longer needs, and must not
    be shown, this registry text at all.
    """
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "INL-98" not in system_prompt
    assert "canonicalSpecies" not in system_prompt


def test_build_prompt_surfaces_all_distinct_values_for_enum_candidate_columns():
    profile = CsvProfile(
        path="x.csv",
        row_count=3,
        columns=[
            ColumnProfile(
                name="utility", dtype="object", null_count=0, null_fraction=0.0,
                distinct_count=3, sample_values=["breeding only"],
                all_distinct_values=["breeding & work", "breeding only", "work only"],
            )
        ],
        sample_rows_csv="utility\nbreeding only\n",
    )
    _, user_prompt = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "breeding & work" in user_prompt
    assert "all distinct values" in user_prompt


def test_build_prompt_with_multiple_csvs_labels_each_table_by_its_own_name():
    """Each uploaded CSV becomes its own table, named after that CSV's own
    filename stem (see draft_service.generate_draft) - the prompt must show
    each table separately, under its own name, not merge them into one.
    """
    profile_a = _sample_profile()
    profile_b = CsvProfile(
        path="y.csv", row_count=5, columns=[], sample_rows_csv="species,count\ncattle,5\n",
    )
    _, user_prompt = build_prompt(
        csv_profiles={"table-one": profile_a, "table-two": profile_b}, digitization_log=None
    )

    assert "table-one" in user_prompt
    assert "table-two" in user_prompt
    assert "2 table(s)" in user_prompt


def test_system_prompt_requires_exactly_one_table_per_csv_and_conditional_dataset_title():
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "one entry per CSV table" in system_prompt or "one table per CSV" in system_prompt.lower()
    assert "more than one csv table" in system_prompt.lower()


def test_parse_llm_output_splits_manifest_and_flags():
    text = """---MANIFEST---
datasetTitle: Foo Dataset
datasetSlug: foo-dataset
---FLAGS---
flags:
  - field: sourceTableID
    reason: not present in CSV
"""
    manifest, flags = parse_llm_output(text)

    assert manifest["datasetTitle"] == "Foo Dataset"
    assert flags == [{"field": "sourceTableID", "reason": "not present in CSV"}]


def test_parse_llm_output_raises_on_missing_delimiters():
    with pytest.raises(ValueError):
        parse_llm_output("just some prose, not the expected format")


def test_parse_llm_output_handles_no_flags():
    text = """---MANIFEST---
datasetTitle: Foo Dataset
---FLAGS---
flags: []
"""
    manifest, flags = parse_llm_output(text)
    assert manifest["datasetTitle"] == "Foo Dataset"
    assert flags == []


def test_parse_llm_output_raises_value_error_on_malformed_manifest_yaml():
    # Reproduces the exact failure the user hit in production: a multi-line
    # plain-scalar list item (no block-scalar marker) directly followed by a
    # quoted-string list item, which yaml.safe_load rejects with
    # "expected <block end>, but found '<scalar>'".
    text = """---MANIFEST---
comments:
  - Livestock Census years are irregular
    and vary across states
  - "locality" includes a 'total' value alongside
    individual localities
---FLAGS---
flags: []
"""
    import yaml as _yaml
    with pytest.raises(_yaml.YAMLError):
        _yaml.safe_load(text.split("---MANIFEST---", 1)[1].split("---FLAGS---", 1)[0])

    with pytest.raises(ValueError):
        parse_llm_output(text)


def test_system_prompt_forbids_drafter_confidence_hedging_in_comments():
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    lowered = system_prompt.lower()
    assert "not fully confident" in lowered
    assert "not the drafting process" in lowered


def test_system_prompt_lists_fixed_top_level_keys_and_embeds_reference_template():
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "FIXED TOP-LEVEL KEYS" in system_prompt
    assert "metadata_field_reference.md" in system_prompt
    # the embedded real-example template, trimmed from CS0007DS0112
    assert "Reference example" in system_prompt
    assert "livestockSpecies" in system_prompt


def test_build_batch_prompt_only_gives_context_for_batch_tables():
    """A table outside the current batch must still be named (for
    cross-table context) but never get its profile stats or a request for
    a `tables:` entry - that's what keeps a later batch's call small.
    """
    profile_a = _sample_profile()
    profile_b = CsvProfile(path="y.csv", row_count=5, columns=[], sample_rows_csv="species,count\ncattle,5\n")
    deterministic_base = {
        "tables": {
            "table-one": {"joinKeys": [], "data_dictionary": {}},
            "table-two": {"joinKeys": [], "data_dictionary": {}},
        },
        "enum_definitions": {},
    }
    _, user_prompt = build_batch_prompt(
        csv_profiles={"table-one": profile_a, "table-two": profile_b},
        digitization_log=None,
        deterministic_base=deterministic_base,
        batch_table_names=["table-one"],
        include_dataset_level_fields=True,
    )

    assert "table-two" in user_prompt  # named for context
    assert "context only" in user_prompt
    assert "species,count\ncattle,5" not in user_prompt  # its profile/samples are withheld
    assert "state,year\nKarnataka,2019" in user_prompt  # batch's own table still gets its context


def test_build_batch_prompt_first_batch_requests_dataset_level_fields():
    profile = _sample_profile()
    deterministic_base = {"tables": {"main": {"joinKeys": [], "data_dictionary": {}}}, "enum_definitions": {}}
    _, user_prompt = build_batch_prompt(
        csv_profiles={"main": profile},
        digitization_log=None,
        deterministic_base=deterministic_base,
        batch_table_names=["main"],
        include_dataset_level_fields=True,
    )

    assert "FIRST call" in user_prompt
    assert "datasetDescription" in user_prompt


def test_build_batch_prompt_later_batch_omits_dataset_level_fields_request():
    profile = _sample_profile()
    deterministic_base = {"tables": {"main": {"joinKeys": [], "data_dictionary": {}}}, "enum_definitions": {}}
    _, user_prompt = build_batch_prompt(
        csv_profiles={"main": profile},
        digitization_log=None,
        deterministic_base=deterministic_base,
        batch_table_names=["main"],
        include_dataset_level_fields=False,
    )

    assert "NOT the first call" in user_prompt
    assert "Omit those keys entirely" in user_prompt
