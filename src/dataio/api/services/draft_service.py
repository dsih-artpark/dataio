"""Orchestrates a single LLM metadata-drafting run: profile the CSV, load
the digitization log, build the prompt, call the LLM, parse and validate
its output, and store the result as a pending draft row. Called by both
the `dataio draft generate` CLI command and the admin
regenerate-flagged-field endpoint - there is exactly one implementation of
"how a draft gets made".
"""

from __future__ import annotations

import csv
import json
import re
from datetime import date
from pathlib import Path

import yaml
from fastapi import HTTPException
from pydantic import BaseModel

from dataio.api.database import functions as database
from dataio.api.database.functions import (
    create_reserved_dataset_id,
    create_reserved_raw_dataset_id,
    get_collection_by_identifier,
    suggest_next_dataset_id,
)
from dataio.api.database.rds_id_helpers import resolve_rds_id
from dataio.api.services.csv_profiler import CsvProfile, profile_csv
from dataio.api.services.digitization_log import load_digitization_log
from dataio.api.services.draft_prompt import (
    build_batch_prompt,
    build_prompt,
    estimate_table_context_size,
    parse_llm_output,
)
from dataio.api.services.field_inference import infer_fixed_column_description, infer_table_structure
from dataio.api.services.manifest_v2_conversion import convert_v2_manifest_to_contract
from dataio.api.services.openrouter_draft_client import OpenRouterDraftClient
from dataio.validate.sdk import DataIOValidator

# Every real metadata.yaml uses this literal value - it's a schema version
# marker, not something that varies per dataset or that the LLM has any
# basis for guessing.
METADATA_SPEC_VERSION = "v2"

# Matches the key order every real metadata.yaml uses (see e.g.
# CS0007DS0112) - the deterministic fields (datasetID, category, etc.) get
# set on the dict after the LLM responds, which would otherwise leave them
# appended at the end rather than in their conventional position.
CANONICAL_KEY_ORDER = (
    "datasetTitle", "datasetSlug", "datasetID", "metadataSpecVersion", "source",
    "category", "collection", "datasetDescription", "datasetOwner", "tags",
    "spatialCoverage", "spatialResolution", "temporalCoverage", "temporalResolution",
    "updateFrequency", "lastUpdated", "canonicalEnumDefinitions", "enumDefinitions",
    "joinKeys", "comments", "references", "tables",
)

# Same convention, one level down: a tables.<name> entry (see e.g.
# CS0007DS0112's own "tables:" block).
TABLE_KEY_ORDER = ("description", "source", "joinKeys", "data_dictionary")

# Same convention, one level further down: a single data_dictionary.<column>
# field spec - matches the order field_inference.py naturally builds one in
# (type/format/enumRef first, then nullable, then description, then
# isJoinKey/joinKeyType once apply_join_keys runs, then the few
# curator/review-time additions last).
FIELD_KEY_ORDER = (
    "type", "format", "enumRef", "nullable", "description", "isJoinKey",
    "joinKeyType", "allowedValues", "temporal_axis", "additive", "aggregation", "min",
)


def _reorder_dict(value: dict, key_order: tuple[str, ...]) -> dict:
    ordered = {key: value[key] for key in key_order if key in value}
    remaining = {key: val for key, val in value.items() if key not in ordered}
    return {**ordered, **remaining}


def _csv_header_columns(csv_path: str) -> list[str]:
    """Just the header row - the true source of a table's "natural" column
    order (what infer_data_dictionary produces at generation time, before
    it's ever round-tripped through the DB). Best-effort: an unreadable/
    missing path just means column order can't be restored, not that the
    save should fail.
    """
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            return next(csv.reader(f))
    except (OSError, StopIteration):
        return []


def _reorder_manifest_keys(
    manifest_dict: dict, csv_paths_by_table: dict[str, str] | None = None
) -> dict:
    """Restores the hand-authored key/column order at every level of the v2
    manifest (top-level, each table, each data_dictionary field and its
    column order) - needed not just once at generation time but on every
    subsequent save, since a JSONB column (draft_json's storage type) does
    not preserve object key order at any nesting depth, so a curator-edited
    draft that round-tripped through the DB would otherwise drift away from
    this order. csv_paths_by_table is optional and only affects
    data_dictionary column order (restored to each CSV's own header order);
    everything else reorders regardless.
    """
    ordered = _reorder_dict(manifest_dict, CANONICAL_KEY_ORDER)
    tables = ordered.get("tables")
    if isinstance(tables, dict):
        ordered["tables"] = {
            name: _reorder_table(table, (csv_paths_by_table or {}).get(name))
            for name, table in tables.items()
        }
    return ordered


def _reorder_table(table: dict, csv_path: str | None = None) -> dict:
    if not isinstance(table, dict):
        return table
    reordered = _reorder_dict(table, TABLE_KEY_ORDER)
    data_dictionary = reordered.get("data_dictionary")
    if isinstance(data_dictionary, dict):
        if csv_path:
            header_columns = _csv_header_columns(csv_path)
            if header_columns:
                data_dictionary = _reorder_dict(data_dictionary, tuple(header_columns))
        reordered["data_dictionary"] = {
            column: _reorder_dict(field, FIELD_KEY_ORDER) if isinstance(field, dict) else field
            for column, field in data_dictionary.items()
        }
    return reordered


def _encode_csv_paths(csv_paths_by_table: dict[str, str]) -> str:
    """The draft row's source_csv_path column is a single Text field, but a
    draft can now span multiple CSVs (one per table) - JSON-encode the
    table_name -> path mapping into it rather than adding a migration for
    what's still logically "where did this draft's source data come from".
    """
    return json.dumps(csv_paths_by_table)


def decode_csv_paths(source_csv_path: str, table_names: list[str] | None = None) -> dict[str, str]:
    """Inverse of _encode_csv_paths. Falls back to treating source_csv_path
    as a single bare path (pre-multi-CSV drafts, which only ever had one
    table) paired with the first known table name.
    """
    try:
        decoded = json.loads(source_csv_path)
        if isinstance(decoded, dict):
            return decoded
    except (json.JSONDecodeError, TypeError):
        pass
    return {table_names[0]: source_csv_path} if table_names else {}


class DraftRecord(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    draft_id: str
    status: str
    draft_yaml: str
    draft_json: dict
    flagged_fields: list[dict]
    validation_status: str


MAX_COMPLETION_ATTEMPTS = 3


def _complete_with_retry(client: OpenRouterDraftClient, system_prompt: str, user_prompt: str) -> tuple[dict, list]:
    """Calls the LLM and parses its output, retrying with a corrective
    follow-up turn (up to MAX_COMPLETION_ATTEMPTS total calls) if the
    response isn't valid YAML in the expected shape - one-shot completions
    have no structured-output guarantee via OpenRouter's OpenAI-compatible
    endpoint, so the parser has to be defensive rather than assume
    well-formed output, and a single retry isn't always enough (e.g. an
    unquoted colon inside a free-text value can slip past one correction
    and recur in slightly different wording on the next attempt).
    """
    prompt = user_prompt
    last_error: ValueError | None = None
    for attempt in range(MAX_COMPLETION_ATTEMPTS):
        completion = client.complete(system_prompt=system_prompt, user_prompt=prompt)
        try:
            return parse_llm_output(completion.text)
        except ValueError as exc:
            last_error = exc
            prompt = (
                user_prompt
                + "\n\nYour previous response was not in the required "
                "---MANIFEST---/---FLAGS--- format, or was not valid YAML: "
                f"{exc}. Resend your answer, following that format exactly, "
                "with no other commentary. Double-quote any string value "
                "containing a colon followed by a space (e.g. a table title "
                "like \"Table 15R: Buffaloes Male Rural\") - an unquoted "
                "colon+space inside a plain scalar is parsed as a new "
                "mapping key and breaks the document. Long free-text values "
                "must be written as a single-line quoted string or a YAML "
                "block scalar (using '|' or '>'), never as plain multi-line "
                "text that isn't quoted or block-scalared."
            )
    raise last_error  # every attempt failed to parse; surface the last error


# Target combined per-table prompt-context size per LLM call, well under
# Claude Sonnet 5's ~200K token (~800K char) context window - leaves
# generous headroom for the fixed system prompt and the completion itself.
# Each table's context (see draft_prompt.estimate_table_context_size) is
# bounded by CsvProfile's own caps (up to 200 distinct values, 20 sample
# rows) regardless of the table's real row count - unlike the raw full-CSV
# text this budget used to measure, so multi-batch splitting (see
# _batch_tables) should now only matter for datasets with unusually many
# tables/columns, not large row counts.
BATCH_CHAR_BUDGET = 400_000


def _batch_tables(
    table_names: list[str], context_sizes: dict[str, int], char_budget: int = BATCH_CHAR_BUDGET
) -> list[list[str]]:
    """Greedily groups table names (in their given order) into batches so
    each batch's combined prompt-context size stays under char_budget.
    Never drops or shrinks a table's context - only limits how many tables
    share one LLM call. A single table whose own context alone exceeds
    char_budget still gets its own solo batch (sent regardless, even if
    that one call ends up over budget - there's no smaller-than-one-table
    granularity to split further).
    """
    batches: list[list[str]] = []
    current: list[str] = []
    current_size = 0
    for table_name in table_names:
        size = context_sizes.get(table_name, 0)
        if current and current_size + size > char_budget:
            batches.append(current)
            current = []
            current_size = 0
        current.append(table_name)
        current_size += size
    if current:
        batches.append(current)
    return batches


# Dataset-wide narrative fields only the first batch is asked to produce
# (see build_batch_prompt) - taken as-is from batch 0's manifest, unchanged.
_SINGLE_SOURCE_DATASET_FIELDS = (
    "datasetTitle", "datasetDescription", "spatialCoverage", "spatialResolution",
    "temporalCoverage", "temporalResolution", "updateFrequency",
)
# Dataset-wide list-shaped fields every batch may contribute to - unioned
# (order-preserving, de-duplicated) across all batches instead of taken
# from one, since every batch may have relevant additions from its own tables.
_MERGED_LIST_FIELDS = ("source", "references", "joinKeys", "comments")


def _merge_batch_manifests(batch_results: list[tuple[dict, list]]) -> tuple[dict, list]:
    """Combines each batch's (manifest_dict, flags) - produced by separate
    LLM calls over disjoint subsets of a dataset's tables (see
    _batch_tables/build_batch_prompt) - into the single manifest_dict/flags
    pair the rest of generate_draft expects. `tables:` entries are disjoint
    per batch (each table is drafted by exactly one batch) so they're
    simply combined; single-source dataset-wide fields come from the first
    batch only (the only one asked to produce them); list-shaped and
    dict-shaped dataset-wide fields are unioned across all batches.
    """
    if len(batch_results) == 1:
        return batch_results[0]

    merged: dict = {field: [] for field in _MERGED_LIST_FIELDS}
    seen_list_values: dict[str, set] = {field: set() for field in _MERGED_LIST_FIELDS}
    merged_tags_concept: list = []
    merged_tags_epi_type: list = []
    merged_enum_defs: dict = {}
    merged_canonical_enum_defs: dict = {}
    merged_tables: dict = {}
    all_flags: list = []

    for field in _SINGLE_SOURCE_DATASET_FIELDS:
        value = batch_results[0][0].get(field)
        if value is not None:
            merged[field] = value

    for manifest_dict, flags in batch_results:
        all_flags.extend(flags)
        merged_tables.update(manifest_dict.get("tables") or {})

        tags = manifest_dict.get("tags") or {}
        for concept in tags.get("concept") or []:
            if concept not in merged_tags_concept:
                merged_tags_concept.append(concept)
        for epi_type in tags.get("epiType") or []:
            if epi_type not in merged_tags_epi_type:
                merged_tags_epi_type.append(epi_type)

        for name, definition in (manifest_dict.get("enumDefinitions") or {}).items():
            merged_enum_defs.setdefault(name, definition)
        for name, definition in (manifest_dict.get("canonicalEnumDefinitions") or {}).items():
            merged_canonical_enum_defs.setdefault(name, definition)

        for field in _MERGED_LIST_FIELDS:
            for item in manifest_dict.get(field) or []:
                dedup_key = item if isinstance(item, str) else yaml.safe_dump(item, sort_keys=True)
                if dedup_key not in seen_list_values[field]:
                    seen_list_values[field].add(dedup_key)
                    merged[field].append(item)

    merged["tags"] = {"concept": merged_tags_concept, "epiType": merged_tags_epi_type}
    if merged_enum_defs:
        merged["enumDefinitions"] = merged_enum_defs
    if merged_canonical_enum_defs:
        merged["canonicalEnumDefinitions"] = merged_canonical_enum_defs
    merged["tables"] = merged_tables

    return merged, all_flags


def _ensure_missing_source_columns_flagged(flags: list[dict], missing_source_columns: list[str]) -> list[dict]:
    """Belt-and-suspenders: don't rely solely on the LLM having followed the
    "don't invent source facts" instruction. Any column profile_csv found
    missing gets a flag added deterministically if the LLM didn't already
    add one for that field.
    """
    already_flagged = {f.get("field") for f in flags}
    for column in missing_source_columns:
        if column not in already_flagged:
            flags.append({"field": column, "reason": f"'{column}' is not present in the source CSV."})
    return flags


def _resolve_category_and_collection(collection_id: str) -> tuple[dict, dict]:
    """Real metadata.yaml files need category/collection ID *and* a real
    display name (e.g. category.name "Census and Surveys") - not something
    the LLM can know. Looked up from the same collections table the rest
    of the admin UI already uses, deterministically, like datasetID.
    """
    collection = get_collection_by_identifier(collection_id)
    if not collection:
        raise HTTPException(status_code=400, detail=f"Collection '{collection_id}' does not exist.")
    category = {"ID": collection.category_id, "name": collection.category_name}
    collection_field = {"ID": collection.collection_id, "name": collection.collection_name}
    return category, collection_field


def _resolve_dataset_id(dataset_id: str | None, collection_id: str, created_by: str) -> str:
    """Existing dataset_id is used as-is (updating that dataset's metadata).
    Otherwise mints and reserves a new one now, not at approval time - the
    validator requires a real, correctly-formatted datasetID on every
    manifest it checks, draft or not. Reserving it (not just computing it)
    stops two concurrent draft generations from being handed the same ID;
    create_dataset already releases the reservation automatically once the
    dataset is actually created.
    """
    if dataset_id:
        return dataset_id
    new_id = suggest_next_dataset_id(collection_id)
    create_reserved_dataset_id(new_id, collection_id, "Reserved for LLM-drafted metadata.yaml", created_by)
    return new_id


def _resolve_and_reserve_raw_dataset_id(
    raw_dataset_id: str | None, category_id: str, collection_id: str, created_by: str
) -> str:
    """Same pattern as _resolve_dataset_id, for rds_id: an existing
    raw_dataset_id (passed by regenerate_draft, reusing the original
    draft's id) is used as-is; otherwise a fresh one is resolved and
    reserved immediately - suggest_next_raw_dataset_id_for_category alone
    has no side effects (it also backs the read-only "Next ID" admin tool,
    which must never reserve anything), so two concurrent draft generations
    in the same category would otherwise be handed the same suggestion.
    """
    if raw_dataset_id:
        return raw_dataset_id
    new_id = resolve_rds_id({"category": {"ID": category_id}, "collection": {"ID": collection_id}})
    create_reserved_raw_dataset_id(new_id, category_id, "Reserved for LLM-drafted metadata.yaml", created_by)
    return new_id


_SLUG_ID_PREFIX_RE = re.compile(r"^[a-z]{2}\d{4}ds\d{4}-?", re.IGNORECASE)
_NON_SLUG_CHARS_RE = re.compile(r"[^a-z0-9]+")


def _slugify(text: str) -> str:
    return _NON_SLUG_CHARS_RE.sub("-", text.lower()).strip("-")


def _build_dataset_slug(dataset_id: str, llm_slug: str | None, dataset_title: str | None) -> str:
    """Deterministically builds a slug matching the validator's
    ^[a-z]{2}\\d{4}ds\\d{4}-[a-z0-9-]+$ pattern, rather than trusting the LLM
    to reproduce that exact shape (including an ID it was told not to
    guess at). Uses whatever descriptive words the LLM provided, stripping
    any dataset-ID-looking prefix it may have added anyway.
    """
    candidate = _SLUG_ID_PREFIX_RE.sub("", (llm_slug or "").strip())
    suffix = _slugify(candidate) or _slugify(dataset_title or "") or "dataset"
    return f"{dataset_id.lower()}-{suffix}"


def _validate_manifest(manifest_dict: dict, csv_paths_by_table: dict[str, str]):
    """Converts the v2-schema draft (tables/data_dictionary) into the shape
    DataIOValidator expects (datasetTables/dataDictionary) and validates
    with that SAME existing validator - the one every real dataset in this
    system was actually checked against (confirmed by converting the real
    CS0007DS0112 metadata.yaml this exact way and comparing it to the live
    manifest_yaml already stored in Postgres for that dataset: identical
    shape). No separate/parallel validator. One data file per table, keyed
    by table name (a dataset may have more than one CSV/table).
    """
    contract_manifest = convert_v2_manifest_to_contract(manifest_dict)
    manifest_yaml = yaml.safe_dump(contract_manifest, sort_keys=False, allow_unicode=True)
    table_names = list(contract_manifest.get("datasetTables", {}).keys())
    data_files = {name: csv_paths_by_table[name] for name in table_names if name in csv_paths_by_table}
    return DataIOValidator().validate_tabular(
        manifest=manifest_yaml,
        data_files=data_files,
        deep_check=False,
        full_scan=True,
    )


def _finalize_draft(
    *,
    manifest_dict: dict,
    flags: list[dict],
    csv_paths_by_table: dict[str, str],
    csv_profiles: dict[str, CsvProfile],
    category_id: str,
    collection_id: str,
    created_by: str,
    data_owner_name: str,
    dataset_id: str | None,
    raw_dataset_id: str | None,
    digitization_log_path: str | None,
    superseded_by_draft_id: str | None,
    llm_model_id: str | None,
) -> DraftRecord:
    """Shared tail for every draft-generation path (LLM or deterministic):
    resolves category/collection and ds_id/rds_id, sets the fields no
    generator has any basis for guessing (datasetID, category, collection,
    datasetOwner, lastUpdated, datasetSlug), validates against the real CSV
    data, and persists a pending draft row. Callers only need to have
    already produced a v2-schema manifest_dict + flags by whatever means
    (LLM completion, rule-based inference, ...) - everything from here on
    (ID reservation, key ordering, validation, persistence) is identical
    regardless of how the manifest content was produced.
    """
    all_missing_source_columns = sorted(
        {column for profile in csv_profiles.values() for column in profile.missing_source_columns}
    )
    flags = _ensure_missing_source_columns_flagged(flags, all_missing_source_columns)

    # Validate the collection exists before reserving anything below - a
    # request for a nonexistent collection must fail with nothing left
    # reserved behind it.
    category, collection_field = _resolve_category_and_collection(collection_id)

    # rds_id is tracked on the draft row, not inside the manifest itself -
    # real metadata.yaml never contains a raw_dataset/rds_id field, that
    # belongs only in info.yml, generated later. Resolved+reserved (or
    # reused as-is if raw_dataset_id was passed in, e.g. by regenerate_draft)
    # the same way resolved_dataset_id is below.
    rds_id = _resolve_and_reserve_raw_dataset_id(raw_dataset_id, category_id, collection_id, created_by)

    resolved_dataset_id = _resolve_dataset_id(dataset_id, collection_id, created_by)

    # datasetTitle: for a single-CSV dataset, use that CSV's own filename -
    # matches the established convention (e.g. CS0007DS0112's datasetTitle
    # is literally "consolidated-livestock-census-1997-2019", its one and
    # only table/CSV name). With more than one CSV there's no single file to
    # name it after (e.g. CS0026DS0111's datasetTitle "bahs-milk-production-
    # statistics-1950-2024" names none of its ~20 table CSVs), so the
    # generator's own proposed dataset-level title is used instead, falling
    # back to the first table's name only if none was produced. Set before
    # building the slug so its fallback has a real title to work from.
    table_names = list(csv_paths_by_table.keys())
    if len(table_names) == 1:
        manifest_dict["datasetTitle"] = table_names[0]
    else:
        manifest_dict["datasetTitle"] = manifest_dict.get("datasetTitle") or table_names[0]
    manifest_dict["datasetID"] = resolved_dataset_id
    manifest_dict["datasetSlug"] = _build_dataset_slug(
        resolved_dataset_id, manifest_dict.get("datasetSlug"), manifest_dict.get("datasetTitle")
    )
    manifest_dict["metadataSpecVersion"] = METADATA_SPEC_VERSION
    manifest_dict["category"] = category
    manifest_dict["collection"] = collection_field
    manifest_dict["datasetOwner"] = data_owner_name
    manifest_dict["lastUpdated"] = date.today().isoformat()
    manifest_dict = _reorder_manifest_keys(manifest_dict)

    manifest_yaml = yaml.safe_dump(manifest_dict, sort_keys=False, allow_unicode=True)
    validation_result = _validate_manifest(manifest_dict, csv_paths_by_table)

    draft = database.create_manifest_draft(
        dataset_id=resolved_dataset_id,
        collection_id=collection_id,
        category_id=category_id,
        source_csv_path=_encode_csv_paths(csv_paths_by_table),
        digitization_log_path=digitization_log_path,
        raw_dataset_id=rds_id,
        draft_yaml=manifest_yaml,
        draft_json=manifest_dict,
        flagged_fields=flags,
        validation_result=validation_result.model_dump(),
        llm_model_id=llm_model_id,
        created_by=created_by,
        superseded_by_draft_id=superseded_by_draft_id,
    )

    return DraftRecord(
        draft_id=str(draft.draft_id),
        status=draft.status.value,
        draft_yaml=draft.draft_yaml,
        draft_json=draft.draft_json,
        flagged_fields=draft.flagged_fields,
        validation_status=validation_result.status,
    )


def _infer_deterministic_base(
    csv_profiles: dict[str, CsvProfile], csv_paths_by_table: dict[str, str]
) -> dict:
    """Runs the same deterministic structure inference the fully rule-based
    drafter uses (field_inference.infer_table_structure) for every table,
    before the LLM is ever called - type/nullable/format/joinKeys/
    canonicalEnumDefinitions/canonical-value-matching and region-history
    comments are all mechanical functions of the column profile stats
    profile_csv already computed, not something an LLM needs to read raw
    CSV text to guess at. Only per-column `description` is left as None
    here for columns with no fixed, structural description (see
    infer_fixed_column_description) - the LLM's narrative pass fills those
    in afterward (see _merge_narrative_into_base).
    """
    tables: dict[str, dict] = {}
    dataset_enum_definitions: dict[str, dict] = {}
    dataset_canonical_enum_definitions: dict[str, dict] = {}
    region_gap_comments: list[str] = []

    for table_name, profile in csv_profiles.items():
        data_dictionary, join_keys, table_region_comments = infer_table_structure(
            profile,
            csv_paths_by_table[table_name],
            dataset_enum_definitions,
            dataset_canonical_enum_definitions,
        )
        for comment in table_region_comments:
            if comment not in region_gap_comments:
                region_gap_comments.append(comment)

        for column_name, field in data_dictionary.items():
            field["description"] = infer_fixed_column_description(column_name)

        tables[table_name] = {"joinKeys": join_keys, "data_dictionary": data_dictionary}

    dataset_join_keys = sorted({column for table in tables.values() for column in table["joinKeys"]})

    return {
        "tables": tables,
        "enum_definitions": dataset_enum_definitions,
        "canonical_enum_definitions": dataset_canonical_enum_definitions,
        "join_keys": dataset_join_keys,
        "region_gap_comments": region_gap_comments,
    }


def _merge_narrative_into_base(base: dict, narrative_manifest: dict) -> dict:
    """Overlays the LLM's narrative-only output (descriptions, enum
    definitions, dataset/table-level fields, comments) onto `base` - the
    deterministic type/joinKeys/canonicalEnumDefinitions/region-comments
    skeleton from _infer_deterministic_base. Column description precedence:
    a fixed structural description (already set in `base`) always wins;
    otherwise the LLM's description is used; otherwise a generic stub
    (matching infer_data_dictionary's own placeholder shape) in case the
    LLM omits a column it wasn't specifically asked to redo work on.
    """
    merged = dict(narrative_manifest)
    merged["joinKeys"] = base["join_keys"]
    if base["canonical_enum_definitions"]:
        merged["canonicalEnumDefinitions"] = base["canonical_enum_definitions"]
    merged["comments"] = [*base["region_gap_comments"], *(narrative_manifest.get("comments") or [])]

    narrative_tables = narrative_manifest.get("tables") or {}
    merged_tables: dict[str, dict] = {}
    for table_name, base_table in base["tables"].items():
        narrative_table = narrative_tables.get(table_name) or {}
        data_dictionary = base_table["data_dictionary"]
        narrative_data_dictionary = narrative_table.get("data_dictionary") or {}
        for column_name, field in data_dictionary.items():
            if field.get("description"):
                continue
            llm_description = (narrative_data_dictionary.get(column_name) or {}).get("description")
            field["description"] = llm_description or f"'{column_name}' column."
        merged_tables[table_name] = {
            "description": narrative_table.get("description", ""),
            "source": narrative_table.get("source", ""),
            "joinKeys": base_table["joinKeys"],
            "data_dictionary": data_dictionary,
        }
    merged["tables"] = merged_tables

    if base["enum_definitions"]:
        narrative_enum_defs = narrative_manifest.get("enumDefinitions") or {}
        merged_enum_defs: dict[str, dict] = {}
        for block, base_def in base["enum_definitions"].items():
            narrative_def = narrative_enum_defs.get(block) or {}
            narrative_values = narrative_def.get("values") or {}
            values = {}
            for value, base_value in base_def["values"].items():
                llm_value_description = (narrative_values.get(value) or {}).get("description")
                values[value] = {
                    **base_value,
                    "description": llm_value_description or base_value["description"],
                }
            merged_enum_defs[block] = {
                "description": narrative_def.get("description") or base_def["description"],
                "values": values,
            }
        merged["enumDefinitions"] = merged_enum_defs

    return merged


def build_csv_paths_by_table(csv_paths: list[str]) -> dict[str, str]:
    """One table per CSV, keyed by that CSV's own filename stem verbatim -
    same convention the dataset-import flow already uses (csv_by_stem in
    web_admin_service._parse_dataset_package). Shared by both drafting
    paths (see deterministic_draft_service.generate_deterministic_draft).

    Rejects duplicate stems up front (e.g. two CSVs both named "data.csv"
    from different folders, or the same file picked twice) - a bare dict
    comprehension would otherwise silently drop all but the last one with
    no error, and the curator would have no idea a table went missing.
    """
    stems = [Path(p).stem for p in csv_paths]
    duplicates = sorted({stem for stem in stems if stems.count(stem) > 1})
    if duplicates:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Duplicate table name(s) from CSV filenames: {', '.join(duplicates)}. "
                "Rename the files so each table name is unique."
            ),
        )
    return {Path(p).stem: str(p) for p in csv_paths}


def generate_draft(
    *,
    csv_paths: list[str],
    category_id: str,
    collection_id: str,
    created_by: str,
    data_owner_name: str,
    dataset_id: str | None = None,
    digitization_log_path: str | None = None,
    superseded_by_draft_id: str | None = None,
    raw_dataset_id: str | None = None,
) -> DraftRecord:
    if not csv_paths:
        raise HTTPException(status_code=400, detail="At least one CSV file is required.")

    csv_paths_by_table = build_csv_paths_by_table(csv_paths)
    csv_profiles = {table_name: profile_csv(p) for table_name, p in csv_paths_by_table.items()}
    digitization_log = load_digitization_log(digitization_log_path)

    # Type/nullable/joinKeys/canonicalEnumDefinitions/region-history
    # comments are all decided deterministically from the column profile
    # stats before the LLM is ever called - the LLM only drafts
    # descriptions and enum definitions (see _merge_narrative_into_base).
    # This also bounds prompt size per table regardless of row count, since
    # raw CSV text is never sent at all anymore (see estimate_table_context_size).
    deterministic_base = _infer_deterministic_base(csv_profiles, csv_paths_by_table)

    table_context_sizes = {
        table_name: estimate_table_context_size(table_name, profile, deterministic_base["tables"][table_name])
        for table_name, profile in csv_profiles.items()
    }
    batches = _batch_tables(list(csv_paths_by_table.keys()), table_context_sizes, BATCH_CHAR_BUDGET)

    client = OpenRouterDraftClient()
    try:
        if len(batches) == 1:
            system_prompt, user_prompt = build_prompt(
                csv_profiles=csv_profiles,
                digitization_log=digitization_log,
                deterministic_base=deterministic_base,
            )
            batch_results = [_complete_with_retry(client, system_prompt, user_prompt)]
        else:
            batch_results = []
            for batch_index, batch_table_names in enumerate(batches):
                system_prompt, user_prompt = build_batch_prompt(
                    csv_profiles=csv_profiles,
                    digitization_log=digitization_log,
                    deterministic_base=deterministic_base,
                    batch_table_names=batch_table_names,
                    include_dataset_level_fields=(batch_index == 0),
                )
                batch_results.append(_complete_with_retry(client, system_prompt, user_prompt))
    finally:
        client.close()

    narrative_manifest, flags = _merge_batch_manifests(batch_results)
    manifest_dict = _merge_narrative_into_base(deterministic_base, narrative_manifest)

    return _finalize_draft(
        manifest_dict=manifest_dict,
        flags=flags,
        csv_paths_by_table=csv_paths_by_table,
        csv_profiles=csv_profiles,
        category_id=category_id,
        collection_id=collection_id,
        created_by=created_by,
        data_owner_name=data_owner_name,
        dataset_id=dataset_id,
        raw_dataset_id=raw_dataset_id,
        digitization_log_path=digitization_log_path,
        superseded_by_draft_id=superseded_by_draft_id,
        llm_model_id=client.model_id,
    )
