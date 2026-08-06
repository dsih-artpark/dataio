from __future__ import annotations

import os
from datetime import date
from types import SimpleNamespace

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from dataio.api.services import draft_service
from dataio.api.services.csv_profiler import ColumnProfile, CsvProfile
from dataio.validate.reports.models import ValidationResult

LLM_RESPONSE = """---MANIFEST---
datasetTitle: Foo Dataset
datasetSlug: foo-dataset
tables:
  foo:
    description: table
---FLAGS---
flags:
  - field: sourceTableID
    reason: missing from CSV
"""

FAKE_COLLECTION = SimpleNamespace(
    collection_id="CS0007", collection_name="Livestock Census (by DAHD)",
    category_id="CS", category_name="Census and Surveys",
)


def _stub_csv_profile(missing_source_columns=None):
    return CsvProfile(
        path="foo.csv", row_count=5, columns=[], missing_source_columns=missing_source_columns or [],
        sample_rows_csv="a,b\n1,2\n",
    )


class FakeOpenRouterClient:
    model_id = "anthropic/claude-3.5-sonnet"

    def __init__(self, *args, **kwargs):
        pass

    def complete(self, *, system_prompt, user_prompt):
        return SimpleNamespace(text=LLM_RESPONSE, model=self.model_id, prompt_tokens=10, completion_tokens=20)

    def close(self):
        pass


def _patch_common(monkeypatch, missing_source_columns=None):
    monkeypatch.setattr(draft_service, "profile_csv", lambda path: _stub_csv_profile(missing_source_columns))
    monkeypatch.setattr(draft_service, "load_digitization_log", lambda path: None)
    monkeypatch.setattr(draft_service, "OpenRouterDraftClient", FakeOpenRouterClient)
    monkeypatch.setattr(
        draft_service, "_validate_manifest",
        lambda manifest_dict, csv_path: ValidationResult(dataset_kind="tabular"),
    )
    monkeypatch.setattr(
        "dataio.api.database.rds_id_helpers.suggest_next_raw_dataset_id_for_category",
        lambda category_id: "CSRDS0099",
    )
    monkeypatch.setattr(draft_service, "get_collection_by_identifier", lambda collection_id: FAKE_COLLECTION)


def test_complete_with_retry_recovers_after_two_malformed_attempts():
    """Reproduces a real production failure: the LLM's first attempt has an
    unquoted colon inside a plain scalar ("mapping values are not allowed
    here"), and the correction turn's response has the *same* class of bug
    worded slightly differently - a single retry wasn't enough, so the loop
    must keep going up to MAX_COMPLETION_ATTEMPTS rather than giving up
    after one correction.
    """
    responses = iter([
        '---MANIFEST---\ndescription: The source PDF (e.g. Table 15R: Buffaloes Male Rural), while old\n---FLAGS---\nflags: []\n',
        '---MANIFEST---\ndescription: Another bad one: still has a colon\n---FLAGS---\nflags: []\n',
        '---MANIFEST---\ndescription: "Finally quoted: correctly"\n---FLAGS---\nflags: []\n',
    ])

    class FlakyClient:
        def complete(self, *, system_prompt, user_prompt):
            return SimpleNamespace(text=next(responses))

    manifest, flags = draft_service._complete_with_retry(FlakyClient(), "system", "user")
    assert manifest["description"] == "Finally quoted: correctly"
    assert flags == []


def test_complete_with_retry_raises_last_error_after_exhausting_attempts():
    class AlwaysBrokenClient:
        def complete(self, *, system_prompt, user_prompt):
            return SimpleNamespace(text="not the expected format at all")

    import pytest
    with pytest.raises(ValueError):
        draft_service._complete_with_retry(AlwaysBrokenClient(), "system", "user")


def test_generate_draft_happy_path(monkeypatch):
    recorded = {}
    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service, "suggest_next_dataset_id", lambda collection_id: "CS0007DS0999")
    monkeypatch.setattr(draft_service, "create_reserved_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(draft_service, "create_reserved_raw_dataset_id", lambda *a, **kw: None)

    def fake_create_manifest_draft(**kwargs):
        recorded["create_kwargs"] = kwargs
        return SimpleNamespace(
            draft_id="11111111-1111-1111-1111-111111111111",
            status=SimpleNamespace(value="pending"),
            draft_yaml=kwargs["draft_yaml"],
            draft_json=kwargs["draft_json"],
            flagged_fields=kwargs["flagged_fields"],
        )

    monkeypatch.setattr(draft_service.database, "create_manifest_draft", fake_create_manifest_draft)

    result = draft_service.generate_draft(
        csv_paths=["foo.csv"], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD",
    )

    assert result.status == "pending"
    assert result.validation_status == "pass"
    assert "raw_dataset" not in result.draft_json  # real metadata.yaml never has this field
    assert recorded["create_kwargs"]["raw_dataset_id"] == "CSRDS0099"
    assert result.draft_json["datasetID"] == "CS0007DS0999"
    assert result.draft_json["datasetTitle"] == "foo"  # from the CSV filename, not the LLM
    assert result.draft_json["datasetSlug"] == "cs0007ds0999-foo-dataset"
    assert result.draft_json["metadataSpecVersion"] == "v2"
    assert result.draft_json["category"] == {"ID": "CS", "name": "Census and Surveys"}
    assert result.draft_json["collection"] == {"ID": "CS0007", "name": "Livestock Census (by DAHD)"}
    assert result.draft_json["datasetOwner"] == "DAHD"
    assert result.draft_json["lastUpdated"] == date.today().isoformat()
    # canonical key order: datasetTitle first, tables last
    assert list(result.draft_json.keys())[0] == "datasetTitle"
    assert list(result.draft_json.keys())[-1] == "tables"
    assert recorded["create_kwargs"]["dataset_id"] == "CS0007DS0999"
    assert recorded["create_kwargs"]["flagged_fields"] == [{"field": "sourceTableID", "reason": "missing from CSV"}]
    assert recorded["create_kwargs"]["source_csv_path"] == '{"foo": "foo.csv"}'


def test_generate_draft_with_multiple_csvs_builds_one_table_per_csv(monkeypatch):
    """Multiple CSVs uploaded for one draft each become their own table
    (named after that CSV's filename stem), matching the real multi-table
    convention used by e.g. CS0026DS0111 (bahs-milk-production-statistics).
    """
    recorded = {}
    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service, "suggest_next_dataset_id", lambda collection_id: "CS0007DS0999")
    monkeypatch.setattr(draft_service, "create_reserved_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(draft_service, "create_reserved_raw_dataset_id", lambda *a, **kw: None)

    def fake_create_manifest_draft(**kwargs):
        recorded["create_kwargs"] = kwargs
        return SimpleNamespace(
            draft_id="11111111-1111-1111-1111-111111111111",
            status=SimpleNamespace(value="pending"),
            draft_yaml=kwargs["draft_yaml"],
            draft_json=kwargs["draft_json"],
            flagged_fields=kwargs["flagged_fields"],
        )

    monkeypatch.setattr(draft_service.database, "create_manifest_draft", fake_create_manifest_draft)

    result = draft_service.generate_draft(
        csv_paths=["foo.csv", "bar.csv"], category_id="CS", collection_id="CS0007",
        created_by="engineer@artpark.in", data_owner_name="DAHD",
    )

    # with more than one table there's no single filename to default to, so
    # the LLM's own proposed datasetTitle (from LLM_RESPONSE) is used as-is
    assert result.draft_json["datasetTitle"] == "Foo Dataset"
    assert recorded["create_kwargs"]["source_csv_path"] == '{"foo": "foo.csv", "bar": "bar.csv"}'


def test_generate_draft_reuses_existing_dataset_id_without_reserving(monkeypatch):
    _patch_common(monkeypatch)

    def fail_if_called(*a, **kw):
        raise AssertionError("should not mint/reserve a new ID when one was already supplied")

    monkeypatch.setattr(draft_service, "suggest_next_dataset_id", fail_if_called)
    monkeypatch.setattr(draft_service, "create_reserved_dataset_id", fail_if_called)
    # dataset_id is reused below, but raw_dataset_id is not supplied, so rds_id
    # resolution/reservation still runs - a separate axis from dataset_id reuse.
    monkeypatch.setattr(draft_service, "create_reserved_raw_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", lambda **kwargs: SimpleNamespace(
        draft_id="1", status=SimpleNamespace(value="pending"),
        draft_yaml=kwargs["draft_yaml"], draft_json=kwargs["draft_json"], flagged_fields=kwargs["flagged_fields"],
    ))

    result = draft_service.generate_draft(
        csv_paths=["foo.csv"], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD", dataset_id="CS0007DS0112",
    )

    assert result.draft_json["datasetID"] == "CS0007DS0112"
    assert result.draft_json["datasetSlug"] == "cs0007ds0112-foo-dataset"


def test_generate_draft_reuses_existing_raw_dataset_id_without_reserving(monkeypatch):
    """regenerate_draft passes the original draft's raw_dataset_id through,
    so a redraft of the same dataset doesn't reserve (and leak) a second one.
    """
    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service, "suggest_next_dataset_id", lambda collection_id: "CS0007DS0999")
    monkeypatch.setattr(draft_service, "create_reserved_dataset_id", lambda *a, **kw: None)

    def fail_if_called(*a, **kw):
        raise AssertionError("should not mint/reserve a new rds_id when one was already supplied")

    monkeypatch.setattr(
        "dataio.api.database.rds_id_helpers.suggest_next_raw_dataset_id_for_category", fail_if_called
    )
    monkeypatch.setattr(draft_service, "create_reserved_raw_dataset_id", fail_if_called)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", lambda **kwargs: SimpleNamespace(
        draft_id="1", status=SimpleNamespace(value="pending"),
        draft_yaml=kwargs["draft_yaml"], draft_json=kwargs["draft_json"], flagged_fields=kwargs["flagged_fields"],
    ))

    draft_service.generate_draft(
        csv_paths=["foo.csv"], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD", raw_dataset_id="CSRDS0016",
    )


def test_generate_draft_flags_missing_source_columns(monkeypatch):
    _patch_common(monkeypatch, missing_source_columns=["sourceTableID", "sourcePage"])
    monkeypatch.setattr(draft_service, "suggest_next_dataset_id", lambda collection_id: "CS0007DS0999")
    monkeypatch.setattr(draft_service, "create_reserved_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(draft_service, "create_reserved_raw_dataset_id", lambda *a, **kw: None)

    captured = {}

    def fake_create_manifest_draft(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            draft_id="11111111-1111-1111-1111-111111111111",
            status=SimpleNamespace(value="pending"),
            draft_yaml=kwargs["draft_yaml"],
            draft_json=kwargs["draft_json"],
            flagged_fields=kwargs["flagged_fields"],
        )

    monkeypatch.setattr(draft_service.database, "create_manifest_draft", fake_create_manifest_draft)

    draft_service.generate_draft(
        csv_paths=["foo.csv"], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD",
    )

    # sourceTableID was already flagged by the LLM (kept, not duplicated);
    # sourcePage wasn't, so the deterministic safety net adds it even though
    # the LLM didn't
    fields_flagged = {f["field"] for f in captured["flagged_fields"]}
    assert fields_flagged == {"sourceTableID", "sourcePage"}
    source_table_flags = [f for f in captured["flagged_fields"] if f["field"] == "sourceTableID"]
    assert len(source_table_flags) == 1
    assert source_table_flags[0]["reason"] == "missing from CSV"


def test_batch_tables_groups_by_cumulative_size():
    context_sizes = {"a": 40, "b": 40, "c": 40}

    batches = draft_service._batch_tables(["a", "b", "c"], context_sizes, char_budget=100)

    # a+b = 80 (fits), c alone starts a new batch (80 + 40 > 100)
    assert batches == [["a", "b"], ["c"]]


def test_batch_tables_puts_oversized_single_table_in_its_own_batch():
    context_sizes = {"huge": 500, "small": 10}

    batches = draft_service._batch_tables(["huge", "small"], context_sizes, char_budget=100)

    # "huge" alone already exceeds the budget, but is never dropped/split -
    # it still gets its own solo batch with its full context intact.
    assert batches == [["huge"], ["small"]]


def test_batch_tables_single_table_under_budget_is_one_batch():
    context_sizes = {"only": 10}

    assert draft_service._batch_tables(["only"], context_sizes, char_budget=100) == [["only"]]


def test_build_csv_paths_by_table_rejects_duplicate_filename_stems():
    import pytest
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        draft_service.build_csv_paths_by_table(["dir_a/data.csv", "dir_b/data.csv"])

    assert exc_info.value.status_code == 400
    assert "data" in exc_info.value.detail


def test_build_csv_paths_by_table_allows_unique_stems():
    result = draft_service.build_csv_paths_by_table(["dir_a/foo.csv", "dir_b/bar.csv"])

    assert result == {"foo": "dir_a/foo.csv", "bar": "dir_b/bar.csv"}


def test_merge_batch_manifests_single_batch_passes_through_unchanged():
    manifest = {"tables": {"main": {}}}
    flags = [{"field": "x", "reason": "y"}]

    result = draft_service._merge_batch_manifests([(manifest, flags)])

    assert result == (manifest, flags)


def test_merge_batch_manifests_combines_disjoint_tables_and_concatenates_flags():
    batch_results = [
        ({"tables": {"foo": {"description": "foo table"}}}, [{"field": "foo_col", "reason": "r1"}]),
        ({"tables": {"bar": {"description": "bar table"}}}, [{"field": "bar_col", "reason": "r2"}]),
    ]

    manifest, flags = draft_service._merge_batch_manifests(batch_results)

    assert manifest["tables"] == {
        "foo": {"description": "foo table"},
        "bar": {"description": "bar table"},
    }
    assert flags == [{"field": "foo_col", "reason": "r1"}, {"field": "bar_col", "reason": "r2"}]


def test_merge_batch_manifests_takes_single_source_fields_from_first_batch_only():
    batch_results = [
        ({"tables": {}, "datasetDescription": "the real description"}, []),
        ({"tables": {}, "datasetDescription": "a second call must not override this"}, []),
    ]

    manifest, _ = draft_service._merge_batch_manifests(batch_results)

    assert manifest["datasetDescription"] == "the real description"


def test_merge_batch_manifests_unions_list_fields_without_duplicates():
    batch_results = [
        ({"tables": {}, "joinKeys": ["state.ID", "year"], "comments": ["shared fact"]}, []),
        ({"tables": {}, "joinKeys": ["year", "species"], "comments": ["shared fact", "second-batch fact"]}, []),
    ]

    manifest, _ = draft_service._merge_batch_manifests(batch_results)

    assert manifest["joinKeys"] == ["state.ID", "year", "species"]
    assert manifest["comments"] == ["shared fact", "second-batch fact"]


def test_merge_batch_manifests_unions_tags_and_enum_definitions():
    batch_results = [
        (
            {
                "tables": {},
                "tags": {"concept": ["livestock"], "epiType": ["population"]},
                "enumDefinitions": {"species": {"description": "d1", "values": {}}},
            },
            [],
        ),
        (
            {
                "tables": {},
                "tags": {"concept": ["livestock", "milk"], "epiType": []},
                "enumDefinitions": {"breed": {"description": "d2", "values": {}}},
            },
            [],
        ),
    ]

    manifest, _ = draft_service._merge_batch_manifests(batch_results)

    assert manifest["tags"] == {"concept": ["livestock", "milk"], "epiType": ["population"]}
    assert set(manifest["enumDefinitions"].keys()) == {"species", "breed"}


def test_generate_draft_splits_dataset_into_multiple_batches_when_over_char_budget(monkeypatch):
    """A dataset whose combined prompt-context size exceeds BATCH_CHAR_BUDGET
    must be drafted over multiple LLM calls (one per table here, given the
    tiny monkeypatched budget) rather than one oversized call - and the
    batches' results must be merged into one manifest.
    """
    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service, "suggest_next_dataset_id", lambda collection_id: "CS0007DS0999")
    monkeypatch.setattr(draft_service, "create_reserved_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(draft_service, "create_reserved_raw_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(draft_service, "BATCH_CHAR_BUDGET", 10)
    monkeypatch.setattr(draft_service, "estimate_table_context_size", lambda table_name, profile, table_base=None: 20)

    calls = []

    class FakeBatchClient:
        model_id = "anthropic/claude-sonnet-5"

        def __init__(self, *a, **kw):
            pass

        def complete(self, *, system_prompt, user_prompt):
            index = len(calls)
            calls.append(user_prompt)
            table_name = ["foo", "bar", "baz"][index]
            dataset_fields = "datasetDescription: shared description\n" if index == 0 else ""
            text = (
                "---MANIFEST---\n"
                f"{dataset_fields}"
                "tables:\n"
                f"  {table_name}:\n"
                "    description: table\n"
                "---FLAGS---\n"
                "flags:\n"
                f"  - field: {table_name}_col\n"
                f"    reason: flagged in batch {index}\n"
            )
            return SimpleNamespace(text=text, model=self.model_id, prompt_tokens=1, completion_tokens=1)

        def close(self):
            pass

    monkeypatch.setattr(draft_service, "OpenRouterDraftClient", FakeBatchClient)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", lambda **kwargs: SimpleNamespace(
        draft_id="1", status=SimpleNamespace(value="pending"),
        draft_yaml=kwargs["draft_yaml"], draft_json=kwargs["draft_json"], flagged_fields=kwargs["flagged_fields"],
    ))

    result = draft_service.generate_draft(
        csv_paths=["foo.csv", "bar.csv", "baz.csv"], category_id="CS", collection_id="CS0007",
        created_by="engineer@artpark.in", data_owner_name="DAHD",
    )

    assert len(calls) == 3  # one LLM call per table, since each table alone exceeds the tiny budget
    assert set(result.draft_json["tables"].keys()) == {"foo", "bar", "baz"}
    assert result.draft_json["datasetDescription"] == "shared description"  # only the first batch supplied it
    assert {f["field"] for f in result.flagged_fields} == {"foo_col", "bar_col", "baz_col"}


def test_infer_deterministic_base_types_columns_and_matches_canonical_enum():
    profile = CsvProfile(
        path="fake.csv",
        row_count=10,
        columns=[
            ColumnProfile(
                name="species", dtype="object", null_count=0, null_fraction=0.0,
                distinct_count=2, sample_values=["cattle", "buffalo"],
                all_distinct_values=["cattle", "buffalo"],
            ),
            ColumnProfile(
                name="count", dtype="int64", null_count=1, null_fraction=0.1,
                distinct_count=5, sample_values=["1", "2"],
            ),
        ],
        sample_rows_csv="species,count\ncattle,1\n",
    )

    base = draft_service._infer_deterministic_base({"main": profile}, {"main": "fake.csv"})

    data_dictionary = base["tables"]["main"]["data_dictionary"]
    assert data_dictionary["species"]["type"] == "enum"
    assert data_dictionary["species"]["enumRef"] == "speciesEnum"
    assert data_dictionary["species"]["description"] is None  # no fixed description; LLM fills it in
    assert data_dictionary["count"]["type"] == "int"
    assert data_dictionary["count"]["nullable"] is True

    assert "canonicalSpecies" in base["canonical_enum_definitions"]
    assert base["enum_definitions"]["speciesEnum"]["values"]["cattle"]["canonical"] == "cattle"


def test_infer_deterministic_base_fills_fixed_structural_descriptions():
    profile = CsvProfile(
        path="fake.csv",
        row_count=10,
        columns=[
            ColumnProfile(
                name="state.ID", dtype="object", null_count=0, null_fraction=0.0,
                distinct_count=2, sample_values=["state_KA"], all_distinct_values=["state_KA", "state_29"],
            ),
        ],
        sample_rows_csv="state.ID\nstate_KA\n",
    )

    base = draft_service._infer_deterministic_base({"main": profile}, {"main": "fake.csv"})

    field = base["tables"]["main"]["data_dictionary"]["state.ID"]
    assert field["type"] == "regionID"
    assert field["description"] == (
        "LGD-based region identifier in the format state_<lgd_code> for states or "
        "ut_<lgd_code> for union territories (e.g., state_28, ut_1)."
    )


def test_merge_narrative_into_base_overlays_descriptions_and_keeps_fixed_ones():
    base = {
        "tables": {
            "main": {
                "joinKeys": ["state.ID"],
                "data_dictionary": {
                    "state.ID": {"type": "regionID", "nullable": False, "description": "fixed description"},
                    "count": {"type": "int", "nullable": True, "description": None},
                },
            }
        },
        "enum_definitions": {},
        "canonical_enum_definitions": {},
        "join_keys": ["state.ID"],
        "region_gap_comments": ["[region history] Telangana was carved out of Andhra Pradesh."],
    }
    narrative = {
        "datasetDescription": "A dataset.",
        "comments": ["units are counts"],
        "tables": {
            "main": {
                "description": "table narrative",
                "source": "some source",
                "data_dictionary": {
                    "state.ID": {"description": "LLM should not override this"},
                    "count": {"description": "Number of animals counted."},
                },
            }
        },
    }

    merged = draft_service._merge_narrative_into_base(base, narrative)

    assert merged["datasetDescription"] == "A dataset."
    assert merged["joinKeys"] == ["state.ID"]
    assert merged["comments"] == ["[region history] Telangana was carved out of Andhra Pradesh.", "units are counts"]
    assert merged["tables"]["main"]["description"] == "table narrative"
    assert merged["tables"]["main"]["source"] == "some source"
    assert merged["tables"]["main"]["data_dictionary"]["state.ID"]["description"] == "fixed description"
    assert merged["tables"]["main"]["data_dictionary"]["count"]["description"] == "Number of animals counted."


def test_merge_narrative_into_base_falls_back_to_stub_when_llm_omits_a_description():
    base = {
        "tables": {"main": {"joinKeys": [], "data_dictionary": {"count": {"type": "int", "description": None}}}},
        "enum_definitions": {},
        "canonical_enum_definitions": {},
        "join_keys": [],
        "region_gap_comments": [],
    }
    narrative = {"tables": {"main": {"data_dictionary": {}}}}

    merged = draft_service._merge_narrative_into_base(base, narrative)

    assert merged["tables"]["main"]["data_dictionary"]["count"]["description"] == "'count' column."


def test_merge_narrative_into_base_overlays_enum_definitions_preserving_canonical_matches():
    base = {
        "tables": {"main": {"joinKeys": [], "data_dictionary": {}}},
        "enum_definitions": {
            "speciesEnum": {
                "description": "Values observed in the 'species' column.",
                "values": {"cattle": {"description": "cattle", "canonical": "cattle", "canonicalRollup": "cattle"}},
            }
        },
        "canonical_enum_definitions": {"canonicalSpecies": {"description": "x", "values": {}}},
        "join_keys": [],
        "region_gap_comments": [],
    }
    narrative = {
        "enumDefinitions": {
            "speciesEnum": {
                "description": "Livestock species.",
                "values": {"cattle": {"description": "Domestic cattle."}},
            }
        }
    }

    merged = draft_service._merge_narrative_into_base(base, narrative)

    assert merged["enumDefinitions"]["speciesEnum"]["description"] == "Livestock species."
    value = merged["enumDefinitions"]["speciesEnum"]["values"]["cattle"]
    assert value["description"] == "Domestic cattle."
    assert value["canonical"] == "cattle"  # deterministic linkage preserved through the merge
    assert merged["canonicalEnumDefinitions"] == {"canonicalSpecies": {"description": "x", "values": {}}}


def test_generate_draft_prompt_size_does_not_scale_with_row_count(monkeypatch):
    """Reproduces the fix for the real production bug: a table with a huge
    row_count (the actual cause of a ~2M-token prompt that blew past the
    model's 1M-token cap) must not blow up prompt size anymore, since the
    LLM is never shown raw row data - only the bounded profile summary
    (sample_rows_csv is always <=20 rows, all_distinct_values is capped),
    regardless of the file's real row count.
    """
    huge_profile = CsvProfile(
        path="huge.csv", row_count=5_000_000, columns=[], missing_source_columns=[],
        sample_rows_csv="a,b\n1,2\n" * 20,
    )
    monkeypatch.setattr(draft_service, "profile_csv", lambda path: huge_profile)
    monkeypatch.setattr(draft_service, "load_digitization_log", lambda path: None)
    monkeypatch.setattr(
        draft_service, "_validate_manifest",
        lambda manifest_dict, csv_path: ValidationResult(dataset_kind="tabular"),
    )
    monkeypatch.setattr(
        "dataio.api.database.rds_id_helpers.suggest_next_raw_dataset_id_for_category",
        lambda category_id: "CSRDS0099",
    )
    monkeypatch.setattr(draft_service, "get_collection_by_identifier", lambda collection_id: FAKE_COLLECTION)
    monkeypatch.setattr(draft_service, "suggest_next_dataset_id", lambda collection_id: "CS0007DS0999")
    monkeypatch.setattr(draft_service, "create_reserved_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(draft_service, "create_reserved_raw_dataset_id", lambda *a, **kw: None)

    captured_prompts = []

    class RecordingClient:
        model_id = "anthropic/claude-3.5-sonnet"

        def __init__(self, *a, **kw):
            pass

        def complete(self, *, system_prompt, user_prompt):
            captured_prompts.append(user_prompt)
            return SimpleNamespace(text=LLM_RESPONSE, model=self.model_id, prompt_tokens=1, completion_tokens=1)

        def close(self):
            pass

    monkeypatch.setattr(draft_service, "OpenRouterDraftClient", RecordingClient)
    monkeypatch.setattr(draft_service.database, "create_manifest_draft", lambda **kwargs: SimpleNamespace(
        draft_id="1", status=SimpleNamespace(value="pending"),
        draft_yaml=kwargs["draft_yaml"], draft_json=kwargs["draft_json"], flagged_fields=kwargs["flagged_fields"],
    ))

    draft_service.generate_draft(
        csv_paths=["foo.csv"], category_id="CS", collection_id="CS0007", created_by="engineer@artpark.in",
        data_owner_name="DAHD",
    )

    assert len(captured_prompts) == 1
    # A 5M-row table's prompt must stay small (well under 50KB) - if this
    # ever regresses back to embedding full CSV text, this assertion fails
    # loudly instead of only surfacing as a real production 502.
    assert len(captured_prompts[0]) < 50_000


def test_generate_draft_raises_when_collection_does_not_exist(monkeypatch):
    from fastapi import HTTPException

    import pytest

    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service, "get_collection_by_identifier", lambda collection_id: None)
    monkeypatch.setattr(draft_service, "suggest_next_dataset_id", lambda collection_id: "CS0007DS0999")
    monkeypatch.setattr(draft_service, "create_reserved_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(draft_service, "create_reserved_raw_dataset_id", lambda *a, **kw: None)

    with pytest.raises(HTTPException) as exc_info:
        draft_service.generate_draft(
            csv_paths=["foo.csv"], category_id="CS", collection_id="CS9999", created_by="engineer@artpark.in",
            data_owner_name="DAHD",
        )
    assert exc_info.value.status_code == 400
