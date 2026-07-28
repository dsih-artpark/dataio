"""Deterministic (no-LLM) inference of data_dictionary/enumDefinitions
entries from a CsvProfile.

Implements in code the same rules that, in the LLM drafter, only existed as
instructions to the model (draft_prompt.py RULES items 5-8: year/date
columns, regionID/regionName/lgd_code pairing, and enum-vs-string typing).
Every rule here is a mechanical function of the column's name and its
profiled statistics (dtype, cardinality, distinct values) - no row-level
judgment calls beyond what the existing manifest schema conventions already
fully specify. See the deterministic-metadata-generator plan for context.
"""

from __future__ import annotations

import re
from typing import Any

from dataio.api.services.csv_profiler import ColumnProfile, CsvProfile
from dataio.api.services.reference_data import load_canonical_enum_registry

_REGION_ID_VALUE_RE = re.compile(r"^[a-z]+_[A-Za-z0-9]+$")
_YEAR_VALUE_RE = re.compile(r"^\d{4}$")
# Splits on underscore/hyphen/space AND on camelCase boundaries (same
# approach as csv_profiler._TOKEN_BOUNDARY_RE) - a column name that's
# already camelCase (e.g. "sourceDocument", a real column name in the live
# livestock census CSVs) must round-trip through this unchanged, not get
# flattened to one token and re-lowercased.
_WORD_SPLIT_RE = re.compile(r"(?<!^)(?=[A-Z])|[_\-\s]+")


def _enum_ref_name(column_name: str) -> str:
    """Deterministic enumDefinitions block name for a column - the column's
    own bare name (last dotted segment) in camelCase plus "Enum", e.g.
    "species" -> "speciesEnum", "table.source_document" -> "sourceDocumentEnum",
    "sourceDocument" -> "sourceDocumentEnum" (unchanged, not "sourcedocumentEnum").
    """
    bare = column_name.rsplit(".", 1)[-1]
    parts = [p for p in _WORD_SPLIT_RE.split(bare) if p]
    if not parts:
        return "valueEnum"
    camel = parts[0].lower() + "".join(p.capitalize() for p in parts[1:])
    return f"{camel}Enum"


_SOURCE_COLUMN_DESCRIPTIONS = {
    "sourceDocument": (
        "URL or filename of the source document from which this row's data was extracted."
    ),
    "sourceTableID": (
        "Identifier of the specific table within the source document this row was extracted from."
    ),
    "sourcePage": "Printed page number within the source document where this row's data appears.",
    "sourcePDFPage": "Absolute PDF page number of the source page within the source document.",
    "sourceSheetRef": (
        "Reference to the specific sheet/tab within the source spreadsheet this row came from."
    ),
    "sourceGranularity": (
        "The level of aggregation/detail at which this row's source data was originally reported."
    ),
}


def infer_fixed_column_description(column_name: str) -> str | None:
    """Canned description for a column whose name follows a well-known
    structural convention that means the same thing in every dataset
    (region identifiers, provenance/source columns) - a curator should
    never be prompted to describe these. Returns None for anything else
    (a domain-specific column only a curator can meaningfully describe).

    Deliberately name-only (no CSV values needed, unlike infer_column_type's
    stricter regionID detection) - this is about whether to prompt the
    curator at intake time, before any profiling has happened, not about
    final typing. The two can disagree in rare edge cases (e.g. a
    non-region "foo.ID" primary-key column skipped here) without breaking
    anything: a column skipped here just falls back to the generic stub if
    it turns out to have no curator-supplied description at generation time.
    """
    if column_name == "state.ID":
        return (
            "LGD-based region identifier in the format state_<lgd_code> for states or "
            "ut_<lgd_code> for union territories (e.g., state_28, ut_1)."
        )
    if column_name == "state.lgd_code":
        return (
            "Standard numeric Local Government Directory (LGD) code for the state "
            "or union territory."
        )
    if column_name == "state.name":
        return "State or union territory name in title case as per LGD."
    if column_name == "district.ID":
        return "LGD district code."
    if column_name == "district.lgd_code":
        return "Numeric LGD district code extracted from district.ID."
    if column_name == "district.name":
        return "District name as per LGD."

    if "." in column_name:
        prefix, bare_name = column_name.rsplit(".", 1)
    else:
        prefix, bare_name = None, column_name

    if column_name.endswith(".ID") and prefix:
        return f"Prefixed LGD identifier for the {prefix}."
    if column_name.endswith(".name") and prefix:
        return f"{prefix.capitalize()} name standardised to LGD classification."
    if column_name.endswith(".lgd_code") and prefix:
        return f"Numeric LGD code extracted from {prefix}.ID."
    if column_name.lower().startswith("source"):
        return _SOURCE_COLUMN_DESCRIPTIONS.get(bare_name, f"Provenance metadata: {bare_name}.")
    return None


def infer_column_type(column: ColumnProfile) -> dict[str, Any]:
    """Returns a v2-schema field spec (type, nullable, and format/enumRef
    as applicable) for one column, using only its name and profiled
    statistics - the deterministic replacement for the LLM's per-column
    typing guess.
    """
    nullable = column.null_fraction > 0
    bare_name = column.name.rsplit(".", 1)[-1]

    # A "year" column (bare name, or the final dotted segment, e.g.
    # "survey.year") - or any column whose every distinct value is a bare
    # 4-digit number - is a calendar year, never int/string.
    looks_like_year = bare_name.lower() == "year" or (
        column.all_distinct_values is not None
        and len(column.all_distinct_values) > 0
        and all(_YEAR_VALUE_RE.match(v) for v in column.all_distinct_values)
    )
    if looks_like_year:
        field: dict[str, Any] = {"type": "date", "format": "%Y", "nullable": nullable}
        if column.all_distinct_values:
            field["allowedValues"] = sorted(column.all_distinct_values)
            field["temporal_axis"] = bare_name.lower()
        return field

    # A `.ID` column is only a regionID if its values already look like
    # `<lowercase-word>_<code>` (e.g. "state_KA") - a plain numeric or
    # freeform `.ID` column that doesn't follow that shape is not.
    if (
        column.name.endswith(".ID")
        and column.all_distinct_values is not None
        and len(column.all_distinct_values) > 0
        and all(_REGION_ID_VALUE_RE.match(v) for v in column.all_distinct_values)
    ):
        return {"type": "regionID", "nullable": nullable}
    if column.name.endswith(".name"):
        return {"type": "regionName", "nullable": nullable}
    if column.name.endswith(".lgd_code"):
        return {"type": "int", "nullable": nullable}

    # Prefer enum over string for any non-numeric column with a small,
    # closed set of values - the profiler only populates all_distinct_values
    # under its cardinality threshold, so "populated" already means "small
    # enough". A numeric-dtype column (e.g. a page-number column with ~150
    # distinct values) is never enum just because its cardinality happens to
    # be low - a closed set of numbers is still a number, not a category -
    # so it falls through to the int/float branches below regardless.
    is_numeric_dtype = "int" in column.dtype or "float" in column.dtype
    if (
        not is_numeric_dtype
        and column.all_distinct_values is not None
        and len(column.all_distinct_values) > 0
    ):
        return {"type": "enum", "enumRef": _enum_ref_name(column.name), "nullable": nullable}

    if "int" in column.dtype:
        return {"type": "int", "nullable": nullable}
    if "float" in column.dtype:
        return {"type": "float", "nullable": nullable}
    return {"type": "string", "nullable": nullable}


def build_enum_definition(column: ColumnProfile) -> dict[str, Any]:
    """A placeholder enumDefinitions entry using each distinct value as its
    own description - a deterministic starting point for the curator to
    refine (via the intake form / draft review), not a final answer.
    """
    return {
        "description": f"Values observed in the '{column.name}' column.",
        "values": {value: {"description": value} for value in (column.all_distinct_values or [])},
    }


def infer_data_dictionary(profile: CsvProfile) -> tuple[dict[str, dict], dict[str, dict]]:
    """Returns (data_dictionary, enum_definitions) for one table's profile.
    isJoinKey/joinKeyType are not set here - see apply_join_keys, applied
    once the composite key for the whole dataset is known.
    """
    data_dictionary: dict[str, dict] = {}
    enum_definitions: dict[str, dict] = {}
    for column in profile.columns:
        field = infer_column_type(column)
        field["description"] = f"'{column.name}' column."
        if field["type"] == "enum":
            enum_definitions[field["enumRef"]] = build_enum_definition(column)
        data_dictionary[column.name] = field
    return data_dictionary, enum_definitions


def find_region_lgd_pairs(
    profile: CsvProfile, data_dictionary: dict[str, dict]
) -> list[tuple[str, str]]:
    """Pairs each regionID-typed column (per the already-inferred
    data_dictionary, not a fresh guess) with its sibling `<prefix>.lgd_code`
    column when present - the existing rule that a regionID used as a join
    key must always be paired with its LGD code column, never included
    alone.
    """
    column_names = {c.name for c in profile.columns}
    pairs: list[tuple[str, str]] = []
    for column in profile.columns:
        if data_dictionary.get(column.name, {}).get("type") != "regionID":
            continue
        if not column.name.endswith(".ID"):
            continue
        lgd_name = f"{column.name[: -len('.ID')]}.lgd_code"
        if lgd_name in column_names:
            pairs.append((column.name, lgd_name))
    return pairs


def infer_join_key_candidates(profile: CsvProfile, data_dictionary: dict[str, dict]) -> list[str]:
    """Deterministic *suggestion*, not a final decision - meant to be
    surfaced to the curator as pre-checked candidates in the intake form
    (this is the one genuinely judgment-based part of manifest drafting, so
    it's a human confirming a deterministic suggestion rather than an LLM
    guessing freely). Candidates: every regionID+lgd_code pair (both
    columns, LGD code first to match existing convention), every date-typed
    column, and every enum-typed "dimension" column (e.g. species/breed/sex
    in a livestock census - the grain the row is reported at) - excluding
    `source*` provenance columns throughout (e.g. a "sourcepdf.year" field
    recording the source PDF's own publish year, which is constant per
    document and not a dimension of the data itself; a real column in the
    live BAHS milk/meat production CSVs, found by running this against
    every dataset in staging-data/).
    """
    candidates: list[str] = []
    for region_column, lgd_column in find_region_lgd_pairs(profile, data_dictionary):
        candidates.extend([lgd_column, region_column])
    for column in profile.columns:
        field_type = data_dictionary.get(column.name, {}).get("type")
        is_provenance = column.name.lower().startswith("source")
        if field_type in {"date", "enum"} and not is_provenance and column.name not in candidates:
            candidates.append(column.name)
    return candidates


def lookup_canonical_enum_definitions(column_names: list[str]) -> dict[str, dict]:
    """Matches column (bare) names against canonical_enum_registry.yaml -
    the deterministic replacement for the LLM's "reuse the INL-98 registry
    verbatim, never invent a new one for this concept" instruction. Returns
    a canonicalEnumDefinitions-shaped dict (block name -> definition),
    including only the blocks whose match keyword actually appears in one
    of the given column names.
    """
    bare_names_lower = {name.rsplit(".", 1)[-1].lower() for name in column_names}
    result: dict[str, dict] = {}
    for registry in load_canonical_enum_registry():
        keywords = [k.lower() for k in registry.get("matchColumnNameContains", [])]
        if any(keyword in bare_name for keyword in keywords for bare_name in bare_names_lower):
            result[registry["block"]] = registry["definition"]
    return result


def _normalize_canonical_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def match_canonical_values(
    distinct_values: list[str], canonical_definition: dict[str, Any]
) -> dict[str, dict[str, str]]:
    """Matches a column's observed enum values against one
    canonicalEnumDefinitions block's values (as returned by
    lookup_canonical_enum_definitions) by normalized-key equality - e.g. the
    dataset-local value "crossbred/exotic" matches the canonical key
    "crossbred_exotic". Returns {value: {"canonical": key, "canonicalRollup":
    rollup}} only for values that actually match; a value with no
    normalized match is omitted entirely rather than guessed - a curator
    can add a real cross-dataset link by hand (via the draft-review YAML
    editor) for edge cases a string match can't resolve on its own.
    """
    canonical_values = canonical_definition.get("values", {})
    normalized_lookup = {_normalize_canonical_key(key): key for key in canonical_values}
    result: dict[str, dict[str, str]] = {}
    for value in distinct_values:
        canonical_key = normalized_lookup.get(_normalize_canonical_key(value))
        if canonical_key is None:
            continue
        rollup = canonical_values[canonical_key].get("rollup", canonical_key)
        result[value] = {"canonical": canonical_key, "canonicalRollup": rollup}
    return result


def apply_join_keys(
    data_dictionary: dict[str, dict], join_key_columns: list[str]
) -> dict[str, dict]:
    """Marks each column in join_key_columns with isJoinKey/joinKeyType on
    its data_dictionary entry (mutates and returns the same dict) - matches
    the existing manifest convention: joinKeyType "temporal" for a
    date-typed column, "compositeComponent" otherwise.
    """
    for column_name in join_key_columns:
        field = data_dictionary.get(column_name)
        if field is None:
            continue
        field["isJoinKey"] = True
        is_temporal = field.get("type") in {"date", "dateTime"}
        field["joinKeyType"] = "temporal" if is_temporal else "compositeComponent"
    return data_dictionary
