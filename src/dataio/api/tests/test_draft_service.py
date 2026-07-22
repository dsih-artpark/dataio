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
from dataio.api.services.csv_profiler import CsvProfile
from dataio.validate.reports.models import ValidationResult

LLM_RESPONSE = """---MANIFEST---
datasetTitle: Foo Dataset
datasetSlug: foo-dataset
tables:
  main:
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
    monkeypatch.setattr(draft_service, "read_full_csv_text", lambda path: "a,b\n1,2\n")
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
    full_csv_texts = {"a": "x" * 40, "b": "x" * 40, "c": "x" * 40}

    batches = draft_service._batch_tables(["a", "b", "c"], full_csv_texts, char_budget=100)

    # a+b = 80 (fits), c alone starts a new batch (80 + 40 > 100)
    assert batches == [["a", "b"], ["c"]]


def test_batch_tables_puts_oversized_single_table_in_its_own_batch():
    full_csv_texts = {"huge": "x" * 500, "small": "x" * 10}

    batches = draft_service._batch_tables(["huge", "small"], full_csv_texts, char_budget=100)

    # "huge" alone already exceeds the budget, but is never dropped/split -
    # it still gets its own solo batch with its full content intact.
    assert batches == [["huge"], ["small"]]


def test_batch_tables_single_table_under_budget_is_one_batch():
    full_csv_texts = {"only": "x" * 10}

    assert draft_service._batch_tables(["only"], full_csv_texts, char_budget=100) == [["only"]]


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
    """A dataset whose combined full-CSV-text exceeds BATCH_CHAR_BUDGET must
    be drafted over multiple LLM calls (one per table here, given the tiny
    monkeypatched budget) rather than one oversized call - and the batches'
    results must be merged into one manifest.
    """
    _patch_common(monkeypatch)
    monkeypatch.setattr(draft_service, "suggest_next_dataset_id", lambda collection_id: "CS0007DS0999")
    monkeypatch.setattr(draft_service, "create_reserved_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(draft_service, "create_reserved_raw_dataset_id", lambda *a, **kw: None)
    monkeypatch.setattr(draft_service, "BATCH_CHAR_BUDGET", 10)
    monkeypatch.setattr(draft_service, "read_full_csv_text", lambda path: "x" * 20)

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
