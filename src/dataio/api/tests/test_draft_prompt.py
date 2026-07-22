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


def test_build_prompt_embeds_full_csv_text_when_provided():
    profile = _sample_profile()
    _, user_prompt = build_prompt(
        csv_profiles={"main": profile}, digitization_log=None, full_csv_texts={"main": "state,year\nA,1\n"}
    )
    assert "Full CSV contents" in user_prompt
    assert "state,year\nA,1" in user_prompt


def test_system_prompt_lists_exact_supported_types_and_rejects_integer():
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert '"int"' in system_prompt
    assert "integer" not in system_prompt.lower().replace('"integer" is invalid', "")
    assert "regionID" in system_prompt
    assert "%Y" in system_prompt


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


def test_system_prompt_requires_lgd_code_paired_with_region_id_in_join_keys():
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "lgd_code" in system_prompt
    assert "never include the regionID alone" in system_prompt or "never the regionID alone" in system_prompt


def test_system_prompt_forbids_drafter_confidence_hedging_in_comments():
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    lowered = system_prompt.lower()
    assert "not fully confident" in lowered
    assert "not the drafting process" in lowered


def test_system_prompt_mentions_canonical_enum_definitions_without_inventing_registry_id():
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "canonicalEnumDefinitions" in system_prompt
    assert "do not invent a registry id" in system_prompt.lower()


def test_system_prompt_reuses_established_inl98_registry_for_livestock_species():
    """Every real dataset with a species/breed column reuses the exact same
    INL-98 registry block (canonicalSpecies/canonicalBreed, with the
    bovine/ovine_and_other_mammals groupings) - the drafter must be told to
    reuse it verbatim rather than inventing a new block or leaving the ID
    for the curator to assign, since one already exists for this concept.
    """
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "INL-98" in system_prompt
    assert "canonicalSpecies" in system_prompt
    assert "canonicalBreed" in system_prompt
    assert "ovine_and_other_mammals" in system_prompt
    assert "bovine" in system_prompt


def test_system_prompt_lists_fixed_top_level_keys_and_embeds_reference_template():
    profile = _sample_profile()
    system_prompt, _ = build_prompt(csv_profiles={"main": profile}, digitization_log=None)

    assert "FIXED TOP-LEVEL KEYS" in system_prompt
    assert "metadata_field_reference.md" in system_prompt
    # the embedded real-example template, trimmed from CS0007DS0112/CS0026DS0111
    assert "Reference example" in system_prompt
    assert "livestockSpecies" in system_prompt


def test_build_batch_prompt_only_embeds_full_text_for_batch_tables():
    """A table outside the current batch must still be named (for
    cross-table context) but never get its full CSV content or a request
    for a `tables:` entry - that's what keeps a later batch's call small.
    """
    profile_a = _sample_profile()
    profile_b = CsvProfile(path="y.csv", row_count=5, columns=[], sample_rows_csv="species,count\ncattle,5\n")
    _, user_prompt = build_batch_prompt(
        csv_profiles={"table-one": profile_a, "table-two": profile_b},
        digitization_log=None,
        full_csv_texts={"table-one": "state,year\nA,1\n", "table-two": "species,count\ncattle,5\n"},
        batch_table_names=["table-one"],
        include_dataset_level_fields=True,
    )

    assert "table-two" in user_prompt  # named for context
    assert "context only" in user_prompt
    assert "species,count\ncattle,5" not in user_prompt  # but its full content is withheld
    assert "state,year\nA,1" in user_prompt  # batch's own table still gets full content


def test_build_batch_prompt_first_batch_requests_dataset_level_fields():
    profile = _sample_profile()
    _, user_prompt = build_batch_prompt(
        csv_profiles={"main": profile},
        digitization_log=None,
        full_csv_texts={"main": "state,year\nA,1\n"},
        batch_table_names=["main"],
        include_dataset_level_fields=True,
    )

    assert "FIRST call" in user_prompt
    assert "datasetDescription" in user_prompt


def test_build_batch_prompt_later_batch_omits_dataset_level_fields_request():
    profile = _sample_profile()
    _, user_prompt = build_batch_prompt(
        csv_profiles={"main": profile},
        digitization_log=None,
        full_csv_texts={"main": "state,year\nA,1\n"},
        batch_table_names=["main"],
        include_dataset_level_fields=False,
    )

    assert "NOT the first call" in user_prompt
    assert "Omit those keys entirely" in user_prompt
