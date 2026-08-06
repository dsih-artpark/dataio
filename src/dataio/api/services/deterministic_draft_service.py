"""Orchestrates a fully deterministic (no-LLM) metadata-drafting run: the
self-service replacement for draft_service.generate_draft agreed with
Lijith (2026-07-24 Data-Platform Discussion) - profile the CSV(s), infer
per-column types/enums and join-key candidates via field_inference.py,
cross-check region history via region_gap_detector.py, combine with the
curator's own directly-supplied fields (CuratorMetadataInput - replacing
freeform digitization_log.yaml notes), and hand off to the SAME shared
ID-resolution/validation/persistence tail draft_service.py already uses
(_finalize_draft) - so a deterministic draft goes through the exact same
pending/approved/rejected lifecycle as an LLM one, just with llm_model_id
left unset.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import HTTPException
from pydantic import BaseModel, Field

from dataio.api.services import draft_service
from dataio.api.services.csv_profiler import profile_csv
from dataio.api.services.field_inference import infer_fixed_column_description, infer_table_structure


class TagsInput(BaseModel):
    concept: list[str] = Field(default_factory=list)
    epiType: list[str] = Field(default_factory=list)


class CuratorMetadataInput(BaseModel):
    """The manifest fields no deterministic rule can derive from the CSV
    alone - supplied directly by the curator through the intake form. This
    replaces the freeform digitization_log.yaml notes Lijith flagged as
    not upload-ready for a non-technical user: these are the exact fixed
    top-level keys draft_prompt.MANIFEST_SCHEMA_HINT documents as
    curator-owned, just captured as structured form fields instead of
    prose an LLM has to interpret. `joinKeyColumns` is the curator's
    confirmation of the candidates field_inference.infer_join_key_candidates
    suggests - the one genuinely judgment-based field, so it's a human
    confirming a deterministic suggestion, not free generation.
    """

    datasetDescription: str
    source: list[str] = Field(default_factory=list)
    references: list[str] = Field(default_factory=list)
    tags: TagsInput = Field(default_factory=TagsInput)
    spatialCoverage: str
    spatialResolution: str
    temporalCoverage: str
    temporalResolution: str
    updateFrequency: str
    comments: list[str] = Field(default_factory=list)
    joinKeyColumns: list[str] = Field(default_factory=list)
    # One required entry per table (keyed by table name - the CSV filename
    # stem, same convention csv_paths_by_table uses), since no rule can
    # derive real table-level narrative from a CSV alone. Validated in
    # generate_deterministic_draft, not here, since the set of table names
    # isn't known until the CSVs are seen.
    tableDescriptions: dict[str, str] = Field(default_factory=dict)
    # One required entry per column not covered by
    # field_inference.infer_fixed_column_description (keyed table_name ->
    # column_name -> description) - a region-identifier or source/provenance
    # column means the same thing in every dataset and is auto-filled
    # instead; everything else is domain-specific and only a curator can
    # meaningfully describe it. Also validated in generate_deterministic_draft.
    columnDescriptions: dict[str, dict[str, str]] = Field(default_factory=dict)
    # Only meaningful (and required) for a multi-CSV dataset - _finalize_draft
    # always names a single-CSV dataset after that CSV's own filename
    # regardless of this value (matches the established hand-authored
    # convention). With more than one CSV there's no single file to name the
    # dataset after, so the curator must supply one explicitly instead of a
    # table getting picked arbitrarily.
    datasetTitle: str = ""


def generate_deterministic_draft(
    *,
    csv_paths: list[str],
    category_id: str,
    collection_id: str,
    created_by: str,
    data_owner_name: str,
    curator_input: CuratorMetadataInput,
    dataset_id: str | None = None,
    digitization_log_path: str | None = None,
    raw_dataset_id: str | None = None,
    superseded_by_draft_id: str | None = None,
) -> draft_service.DraftRecord:
    if not csv_paths:
        raise HTTPException(status_code=400, detail="At least one CSV file is required.")

    # Same one-table-per-CSV convention as generate_draft.
    csv_paths_by_table = {Path(p).stem: str(p) for p in csv_paths}

    missing_descriptions = [
        name for name in csv_paths_by_table
        if not curator_input.tableDescriptions.get(name, "").strip()
    ]
    if missing_descriptions:
        raise HTTPException(
            status_code=400,
            detail=f"Missing table description for: {', '.join(sorted(missing_descriptions))}.",
        )
    if len(csv_paths_by_table) > 1 and not curator_input.datasetTitle.strip():
        raise HTTPException(
            status_code=400,
            detail="Dataset title is required when uploading more than one CSV.",
        )

    csv_profiles = {table_name: profile_csv(p) for table_name, p in csv_paths_by_table.items()}

    tables: dict[str, dict] = {}
    dataset_enum_definitions: dict[str, dict] = {}
    dataset_canonical_enum_definitions: dict[str, dict] = {}
    region_gap_comments: list[str] = []
    missing_column_descriptions: list[str] = []

    for table_name, profile in csv_profiles.items():
        # The curator confirms/adjusts join keys once, dataset-wide (see
        # CuratorMetadataInput docstring) - a table only gets the subset of
        # those columns it actually has (infer_table_structure handles the
        # filtering). Type inference, enum-block collision-safe merging,
        # canonical-registry cross-linking, and region-gap detection are
        # all shared with the LLM drafter's pre-pass - see
        # field_inference.infer_table_structure.
        data_dictionary, table_join_keys, table_region_comments = infer_table_structure(
            profile,
            csv_paths_by_table[table_name],
            dataset_enum_definitions,
            dataset_canonical_enum_definitions,
            join_key_columns_override=curator_input.joinKeyColumns,
        )
        for comment in table_region_comments:
            if comment not in region_gap_comments:
                region_gap_comments.append(comment)

        curator_column_descriptions = curator_input.columnDescriptions.get(table_name, {})
        for column_name, field in data_dictionary.items():
            fixed_description = infer_fixed_column_description(column_name)
            if fixed_description is not None:
                field["description"] = fixed_description
                continue
            curator_description = curator_column_descriptions.get(column_name, "").strip()
            if curator_description:
                field["description"] = curator_description
            else:
                missing_column_descriptions.append(f"{table_name}.{column_name}")

        tables[table_name] = {
            "description": curator_input.tableDescriptions[table_name],
            "source": curator_input.source[0] if curator_input.source else "",
            "joinKeys": table_join_keys,
            "data_dictionary": data_dictionary,
        }

    if missing_column_descriptions:
        raise HTTPException(
            status_code=400,
            detail=f"Missing description for: {', '.join(sorted(missing_column_descriptions))}.",
        )

    dataset_join_keys = curator_input.joinKeyColumns or sorted(
        {column for table in tables.values() for column in table["joinKeys"]}
    )

    manifest_dict: dict = {
        "datasetDescription": curator_input.datasetDescription,
        "source": curator_input.source,
        "tags": {"concept": curator_input.tags.concept, "epiType": curator_input.tags.epiType},
        "spatialCoverage": curator_input.spatialCoverage,
        "spatialResolution": curator_input.spatialResolution,
        "temporalCoverage": curator_input.temporalCoverage,
        "temporalResolution": curator_input.temporalResolution,
        "updateFrequency": curator_input.updateFrequency,
        "joinKeys": dataset_join_keys,
        "comments": [*curator_input.comments, *region_gap_comments],
        "references": curator_input.references,
        "tables": tables,
    }
    if dataset_enum_definitions:
        manifest_dict["enumDefinitions"] = dataset_enum_definitions
    if dataset_canonical_enum_definitions:
        manifest_dict["canonicalEnumDefinitions"] = dataset_canonical_enum_definitions
    if curator_input.datasetTitle.strip():
        # Ignored by _finalize_draft for a single-CSV dataset (it always uses
        # that CSV's own filename instead) - only takes effect with 2+ CSVs.
        manifest_dict["datasetTitle"] = curator_input.datasetTitle.strip()

    return draft_service._finalize_draft(
        manifest_dict=manifest_dict,
        flags=[],
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
        llm_model_id=None,
    )
