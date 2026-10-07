"""Deterministic gap-comment generation: cross-references a table's actual
region x year coverage against the curated region_history.yaml reference
table (known Indian state/UT reorganization events), and generates the
same kind of `comments` entry the LLM drafter used to produce from general
knowledge (e.g. "Telangana absent from 1997-2012 data... part of Andhra
Pradesh until 2014") - but from a curated, curator-verified fact table
cross-checked against the real data, not a hallucination risk.
"""

from __future__ import annotations

import pandas as pd

from dataio.api.services.reference_data import load_region_history


def _region_gaps_from_frame(df: pd.DataFrame, region_column: str, date_column: str) -> list[str]:
    """Core comparison logic, operating on an already-loaded DataFrame -
    shared by detect_region_gaps (reads its own single-pair slice of the
    CSV, for standalone callers) and detect_region_gaps_for_table (reads
    the whole file once and reuses it across every region/date column
    pair, rather than re-reading the same CSV from disk once per pair).

    Matching is case-insensitive - real region-name columns are often
    stored in a fixed case convention (e.g. all-caps, as in the actual
    ARTPARK livestock census CSVs: "TELANGANA") that won't literally equal
    region_history.yaml's human-readable casing ("Telangana").
    """
    normalized_region = df[region_column].astype(str).str.casefold()
    # Excludes genuinely-missing values from regions_present without
    # dropping rows from normalized_region itself (which must stay the
    # same length/index as df for the boolean mask below to align
    # correctly) - otherwise a blank region value would astype(str) into
    # the literal text "nan" and get treated as a real (bogus) region name,
    # since a plain .dropna() afterward only removes actual nulls, not the
    # string "nan" that .astype(str) already turned them into.
    regions_present = set(normalized_region[df[region_column].notna()])
    comments: list[str] = []

    for event in load_region_history():
        region = event["region"]
        region_casefold = region.casefold()
        if region_casefold not in regions_present:
            continue
        effective_year = int(event["effective_date"][:4])
        years_for_region = pd.to_numeric(
            df.loc[normalized_region == region_casefold, date_column], errors="coerce"
        ).dropna()
        earliest_year = int(years_for_region.min()) if not years_for_region.empty else None

        if earliest_year is not None and earliest_year < effective_year:
            comments.append(
                f"[region history] '{region}' appears in this data as early as {earliest_year}, "
                f"before its documented effective date of {event['effective_date']} "
                f"({event['note']}) - worth checking this is expected for this dataset."
            )
        else:
            comments.append(f"[region history] {event['note']}")

    return comments


def detect_region_gaps(csv_path: str, region_column: str, date_column: str) -> list[str]:
    """Reads csv_path directly - region x year presence isn't captured by
    the lightweight per-column CsvProfile, only a real read gives the
    joint distribution. For each region_history event whose `region`
    actually appears in this table's data, adds an explanatory comment. If
    the region appears *before* its documented effective date, that's
    either a data-entry anomaly or evidence this dataset predates the
    documented event differently than expected - flagged in the comment
    text rather than silently accepted, since a curated fact contradicted
    by the actual data is exactly the kind of thing a human should look at.
    """
    df = pd.read_csv(csv_path, usecols=[region_column, date_column])
    return _region_gaps_from_frame(df, region_column, date_column)


def detect_region_gaps_for_table(csv_path: str, data_dictionary: dict[str, dict]) -> list[str]:
    """Finds every regionName-typed column paired with a date-typed column
    in this table's inferred data_dictionary (see field_inference) and
    runs the region-gap comparison for each pair - the orchestration a
    caller needs without having to know column names in advance. Returns
    combined, de-duplicated comments in stable order.

    Reads csv_path once (covering every region/date column involved) and
    reuses that single DataFrame across all pairs, rather than re-reading
    the same file from disk once per pair - a table with e.g. 2 region
    columns and 2 date columns would otherwise mean 4 separate full reads.
    """
    date_types = {"date", "dateTime"}
    region_columns = [name for name, f in data_dictionary.items() if f.get("type") == "regionName"]
    date_columns = [name for name, f in data_dictionary.items() if f.get("type") in date_types]
    if not region_columns or not date_columns:
        return []

    needed_columns = sorted(set(region_columns) | set(date_columns))
    df = pd.read_csv(csv_path, usecols=needed_columns)

    comments: list[str] = []
    seen: set[str] = set()
    for region_column in region_columns:
        for date_column in date_columns:
            for comment in _region_gaps_from_frame(df, region_column, date_column):
                if comment not in seen:
                    seen.add(comment)
                    comments.append(comment)
    return comments
