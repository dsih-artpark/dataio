"""Deterministic, rule-based CSV profiling for the LLM drafter - dtypes,
nulls, cardinality, sample values, and an explicit check for the
source-citation columns the drafter is not allowed to invent. No LLM
involved. See metadata-architecture memo, Stage 03.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, Field

# Source-citation concepts the drafter must find in the data rather than
# invent - which document, which table within it, which page. Column
# naming for these varies across datasets (a PDF-table-extraction might use
# "sourceTableID"/"sourcePage"; an Excel-derived dataset may have no
# per-row table/page concept at all, or name its citation column something
# else entirely) - so detection is by keyword, against whatever column
# names actually exist, not a fixed exact-name list. Keys are the fixed
# metadata.yaml field names these concepts map to (see
# metadata_field_reference.md); "source" itself isn't used as a keyword
# since it's a common prefix across all three real conventions
# (sourceDocument/sourceTableID/sourcePage) and wouldn't discriminate
# between them.
SOURCE_CONCEPT_KEYWORDS: dict[str, frozenset[str]] = {
    "sourceDocument": frozenset({"document", "citation", "reference", "doc", "url", "pdf", "link", "publication", "report"}),
    "sourceTableID": frozenset({"table"}),
    "sourcePage": frozenset({"page"}),
}

_TOKEN_BOUNDARY_RE = re.compile(r"(?<!^)(?=[A-Z])|[_\.\-\s]+")


def _tokenize_column_name(column_name: str) -> set[str]:
    return {tok.lower() for tok in _TOKEN_BOUNDARY_RE.split(column_name) if tok}


def detect_missing_source_concepts(column_names) -> list[str]:
    """Which of SOURCE_CONCEPT_KEYWORDS has no matching column, by keyword
    rather than exact name - so "SourceTable", "table_no", and
    "sourceTableID" are all recognized as satisfying the same concept.
    """
    all_tokens: set[str] = set()
    for column_name in column_names:
        all_tokens |= _tokenize_column_name(str(column_name))
    return [
        concept
        for concept, keywords in SOURCE_CONCEPT_KEYWORDS.items()
        if not (all_tokens & keywords)
    ]

# Columns with at most this many distinct values get their COMPLETE value
# list in the profile, not just a sample - these are exactly the columns
# that become `enum` fields, and an enum's allowedValues has to cover every
# value actually in the data or validation fails on whichever row has the
# one value that didn't make a 10-item sample (e.g. a rare "breeding & work"
# among many "breeding only" rows).
ENUM_CANDIDATE_MAX_CARDINALITY = 200


class ColumnProfile(BaseModel):
    name: str
    dtype: str
    null_count: int
    null_fraction: float
    distinct_count: int
    sample_values: list[str] = Field(default_factory=list)
    # Populated only when distinct_count <= ENUM_CANDIDATE_MAX_CARDINALITY -
    # the full, exhaustive set of values this column takes, in that case.
    all_distinct_values: list[str] | None = None
    min_value: str | None = None
    max_value: str | None = None


class CsvProfile(BaseModel):
    path: str
    row_count: int
    columns: list[ColumnProfile]
    missing_source_columns: list[str] = Field(default_factory=list)
    sample_rows_csv: str = ""


def profile_csv(path: str | Path, sample_rows: int = 20) -> CsvProfile:
    df = pd.read_csv(path)

    missing_source_columns = detect_missing_source_concepts(df.columns)

    columns: list[ColumnProfile] = []
    for column_name in df.columns:
        series = df[column_name]
        null_count = int(series.isna().sum())
        row_count = len(series)
        distinct_count = int(series.nunique(dropna=True))
        distinct_values = series.dropna().unique()
        sample_values = [str(v) for v in distinct_values[:10]]
        all_distinct_values = (
            sorted(str(v) for v in distinct_values)
            if distinct_count <= ENUM_CANDIDATE_MAX_CARDINALITY
            else None
        )

        min_value = None
        max_value = None
        numeric_series = pd.to_numeric(series, errors="coerce")
        if numeric_series.notna().any():
            min_value = str(numeric_series.min())
            max_value = str(numeric_series.max())

        columns.append(
            ColumnProfile(
                name=str(column_name),
                dtype=str(series.dtype),
                null_count=null_count,
                null_fraction=(null_count / row_count) if row_count else 0.0,
                distinct_count=distinct_count,
                sample_values=sample_values,
                all_distinct_values=all_distinct_values,
                min_value=min_value,
                max_value=max_value,
            )
        )

    sample_rows_csv = df.head(sample_rows).to_csv(index=False)

    return CsvProfile(
        path=str(path),
        row_count=len(df),
        columns=columns,
        missing_source_columns=missing_source_columns,
        sample_rows_csv=sample_rows_csv,
    )


def read_full_csv_text(path: str | Path) -> str:
    """The raw CSV text, embedded whole in the prompt for every table -
    every table's actual content is always sent to the LLM, never
    summarized away. Datasets whose combined full-CSV-text would exceed one
    LLM call's context budget are split across multiple calls instead (see
    draft_service._batch_tables), rather than any single table's content
    being dropped down to a profile-only summary.
    """
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
