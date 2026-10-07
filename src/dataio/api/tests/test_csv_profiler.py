from __future__ import annotations

from pathlib import Path

from dataio.api.services.csv_profiler import (
    detect_missing_source_concepts,
    profile_csv,
    read_full_csv_text,
)

CSV_WITH_SOURCE_COLUMNS = """state,year,value,sourceTableID,sourcePage,sourceDocument
Karnataka,2019,120,Table 4.2,p. 37,https://example.gov/report.pdf
Kerala,2019,,Table 4.2,p. 37,https://example.gov/report.pdf
Karnataka,2020,130,Table 4.2,p. 37,https://example.gov/report.pdf
"""

CSV_WITHOUT_SOURCE_COLUMNS = """state,year,value
Karnataka,2019,120
Kerala,2019,95
"""


def test_profile_csv_computes_dtype_null_and_cardinality(tmp_path: Path):
    csv_path = tmp_path / "with_source.csv"
    csv_path.write_text(CSV_WITH_SOURCE_COLUMNS, encoding="utf-8")

    profile = profile_csv(csv_path)

    assert profile.row_count == 3
    value_col = next(c for c in profile.columns if c.name == "value")
    assert value_col.null_count == 1
    assert value_col.null_fraction == 1 / 3
    assert value_col.min_value == "120.0"
    assert value_col.max_value == "130.0"

    state_col = next(c for c in profile.columns if c.name == "state")
    assert state_col.distinct_count == 2


def test_profile_csv_reports_no_missing_source_columns_when_present(tmp_path: Path):
    csv_path = tmp_path / "with_source.csv"
    csv_path.write_text(CSV_WITH_SOURCE_COLUMNS, encoding="utf-8")

    profile = profile_csv(csv_path)

    assert profile.missing_source_columns == []


def test_profile_csv_flags_missing_source_columns(tmp_path: Path):
    csv_path = tmp_path / "without_source.csv"
    csv_path.write_text(CSV_WITHOUT_SOURCE_COLUMNS, encoding="utf-8")

    profile = profile_csv(csv_path)

    assert set(profile.missing_source_columns) == {"sourceDocument", "sourceTableID", "sourcePage"}


def test_detect_missing_source_concepts_recognizes_alternate_naming():
    # Different naming per dataset (not the exact literal "sourceTableID"/
    # "sourcePage"/"sourceDocument") must still be recognized.
    columns = ["state", "year", "value", "Source_Table_No", "PageNumber", "citation_url"]

    assert detect_missing_source_concepts(columns) == []


def test_detect_missing_source_concepts_reports_only_genuinely_absent_ones():
    # This dataset (e.g. Excel-derived, single workbook) has a document
    # reference but no per-row table/page concept - only those two should
    # be reported missing, not sourceDocument.
    columns = ["state", "year", "value", "source_reference"]

    assert detect_missing_source_concepts(columns) == ["sourceTableID", "sourcePage"]


def test_detect_missing_source_concepts_does_not_let_source_prefix_satisfy_other_concepts():
    # A column literally named "sourceTableID" contains the substring
    # "source" - that must not be enough to also satisfy the sourceDocument
    # concept, which needs its own distinguishing keyword (document/
    # citation/reference/etc.), not just the shared "source" prefix.
    columns = ["state", "sourceTableID"]

    assert set(detect_missing_source_concepts(columns)) == {"sourceDocument", "sourcePage"}


def test_profile_csv_bounds_sample_rows(tmp_path: Path):
    csv_path = tmp_path / "with_source.csv"
    csv_path.write_text(CSV_WITH_SOURCE_COLUMNS, encoding="utf-8")

    profile = profile_csv(csv_path, sample_rows=2)

    # header + 2 data rows
    assert profile.sample_rows_csv.strip().count("\n") == 2


def test_read_full_csv_text_roundtrips(tmp_path: Path):
    csv_path = tmp_path / "without_source.csv"
    csv_path.write_text(CSV_WITHOUT_SOURCE_COLUMNS, encoding="utf-8")

    assert read_full_csv_text(csv_path) == CSV_WITHOUT_SOURCE_COLUMNS


def test_profile_csv_includes_complete_distinct_values_for_low_cardinality_columns(tmp_path: Path):
    # A rare value ("breeding & work") that a top-10 sample could easily miss
    # in a bigger real dataset - all_distinct_values must still surface it.
    csv_text = "utility\n" + "\n".join(["breeding only"] * 50 + ["work only"] * 50 + ["breeding & work"])
    csv_path = tmp_path / "utility.csv"
    csv_path.write_text(csv_text, encoding="utf-8")

    profile = profile_csv(csv_path)
    utility_col = next(c for c in profile.columns if c.name == "utility")

    assert utility_col.all_distinct_values == ["breeding & work", "breeding only", "work only"]


def test_profile_csv_omits_all_distinct_values_above_cardinality_threshold(tmp_path: Path, monkeypatch):
    import dataio.api.services.csv_profiler as profiler_module

    monkeypatch.setattr(profiler_module, "ENUM_CANDIDATE_MAX_CARDINALITY", 2)
    csv_path = tmp_path / "with_source.csv"
    csv_path.write_text(CSV_WITH_SOURCE_COLUMNS, encoding="utf-8")

    profile = profile_csv(csv_path)
    year_col = next(c for c in profile.columns if c.name == "year")

    # 2 distinct years (2019, 2020) is within the (monkeypatched) threshold of 2
    assert year_col.all_distinct_values == ["2019", "2020"]

    state_col = next(c for c in profile.columns if c.name == "state")
    # 2 distinct states is also within threshold - shrink further to prove the None branch
    monkeypatch.setattr(profiler_module, "ENUM_CANDIDATE_MAX_CARDINALITY", 1)
    profile = profile_csv(csv_path)
    state_col = next(c for c in profile.columns if c.name == "state")
    assert state_col.all_distinct_values is None
