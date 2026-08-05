"""Builds info.yml from an already-generated/approved manifest draft.

info.yml is the second file (alongside metadata.yaml) the real dataset
import step expects (see web_admin_service._parse_dataset_package) - but
nothing generates it today, so a curator has to hand-write one from
scratch after approving a draft, duplicating fields (ds_id, title,
description, temporal coverage, raw_dataset.source, ...) that already
exist in draft_json or on the draft row itself. This module derives every
field it can from those existing, already-curator-reviewed sources -
access_level is the one field with no source anywhere upstream, so it's
taken as an explicit parameter (see DraftReviewService.generate_info_yaml).

Deterministic only: string/dict lookups over already-generated
draft_json - no LLM, no network call, no CSV row/cell data.
"""

from __future__ import annotations

import yaml

# Matches the key order every hand-authored info.yml uses (see e.g.
# data/CS0007DS0112-.../info.yml).
_KEY_ORDER = (
    "ds_id",
    "collection_id",
    "title",
    "data_owner_name",
    "description",
    "temporal_coverage_start_date",
    "temporal_coverage_end_date",
    "temporal_resolution",
    "spatial_resolution",
    "access_level",
    "raw_dataset",
)

# The only date format field_inference.py currently ever produces (see
# infer_column_type's looks_like_year branch) - extend this mapping if
# infer_column_type ever learns to type a month/day-level date column.
_DATE_FORMAT_TO_TEMPORAL_RESOLUTION = {
    "%Y": "YEAR",
}


def _derive_temporal_coverage(tables: dict | None) -> tuple[str | None, str | None, str | None]:
    """Scans every table's data_dictionary for a date-typed field with
    allowedValues (the year field(s) field_inference.py already computes
    allowedValues/temporal_axis for) and derives (start_date, end_date,
    temporal_resolution) from the first one found. Datasets in this system
    only ever have one temporal axis in practice (a single "year" grain
    shared across all tables), so the first match is taken as authoritative
    rather than trying to reconcile disagreeing fields across tables.
    """
    for table in (tables or {}).values():
        for field in (table.get("data_dictionary") or {}).values():
            allowed_values = field.get("allowedValues")
            if field.get("type") != "date" or not allowed_values:
                continue
            start_date = min(allowed_values)
            end_date = max(allowed_values)
            resolution = _DATE_FORMAT_TO_TEMPORAL_RESOLUTION.get(field.get("format"))
            return start_date, end_date, resolution
    return None, None, None


def build_info_yaml(
    draft_json: dict,
    *,
    dataset_id: str,
    collection_id: str,
    raw_dataset_id: str | None,
    access_level: str,
) -> str:
    """Deterministic: every field here is a direct lookup/transform of
    draft_json or the already-resolved draft_id/collection_id/
    raw_dataset_id - the same fields a curator already reviewed on the
    Draft Review screen, never a fresh guess. access_level is the sole
    exception (see module docstring) - passed in by the curator via the
    review screen, not derivable from anything upstream.
    """
    start_date, end_date, temporal_resolution = _derive_temporal_coverage(
        draft_json.get("tables")
    )
    spatial_resolution = draft_json.get("spatialResolution")

    info: dict = {
        "ds_id": dataset_id,
        "collection_id": collection_id,
        "title": draft_json.get("datasetTitle"),
        "data_owner_name": draft_json.get("datasetOwner"),
        "description": draft_json.get("datasetDescription"),
        "temporal_coverage_start_date": start_date,
        "temporal_coverage_end_date": end_date,
        "temporal_resolution": temporal_resolution,
        "spatial_resolution": spatial_resolution.upper() if spatial_resolution else None,
        "access_level": access_level,
        "raw_dataset": {
            "rds_id": raw_dataset_id,
            "source": "; ".join(draft_json.get("source") or []) or None,
        },
    }

    # Drop keys whose value is None - a curator filling these in by hand
    # should see an absent field to fill in, not `field: null`.
    ordered = {key: info[key] for key in _KEY_ORDER if key in info and info[key] is not None}
    if "raw_dataset" in ordered:
        ordered["raw_dataset"] = {
            key: value for key, value in ordered["raw_dataset"].items() if value is not None
        }
        if not ordered["raw_dataset"]:
            del ordered["raw_dataset"]

    return yaml.safe_dump(ordered, sort_keys=False, allow_unicode=True)
