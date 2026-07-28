from __future__ import annotations

from pathlib import Path

from dataio.api.services.region_gap_detector import detect_region_gaps, detect_region_gaps_for_table


def _write_csv(tmp_path: Path, rows: list[tuple[str, int]]) -> str:
    csv_path = tmp_path / "data.csv"
    lines = ["state.name,year"] + [f"{state},{year}" for state, year in rows]
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(csv_path)


def test_detect_region_gaps_notes_known_event_when_data_matches_expected_pattern(tmp_path: Path):
    # Telangana only appears from 2019 onward - consistent with its real
    # 2014-06-02 effective date, so no anomaly warning should fire.
    csv_path = _write_csv(tmp_path, [
        ("Andhra Pradesh", 1997), ("Andhra Pradesh", 2012), ("Telangana", 2019),
    ])

    comments = detect_region_gaps(csv_path, "state.name", "year")

    telangana_comments = [c for c in comments if "Telangana" in c]
    assert len(telangana_comments) == 1
    assert telangana_comments[0].startswith("[region history]")
    assert "as early as" not in telangana_comments[0]  # no anomaly - matches documented history


def test_detect_region_gaps_flags_anomaly_when_region_appears_before_effective_date(tmp_path: Path):
    # Telangana appearing in 2010 data contradicts its real 2014 formation -
    # worth surfacing to a curator rather than silently trusting the data.
    csv_path = _write_csv(tmp_path, [("Telangana", 2010)])

    comments = detect_region_gaps(csv_path, "state.name", "year")

    telangana_comments = [c for c in comments if "Telangana" in c]
    assert len(telangana_comments) == 1
    assert "as early as 2010" in telangana_comments[0]
    assert "2014-06-02" in telangana_comments[0]


def test_detect_region_gaps_matches_regardless_of_case(tmp_path: Path):
    # Regression: the real ARTPARK livestock census CSVs store state names
    # in ALL CAPS ("TELANGANA"), which must still match region_history.yaml's
    # human-readable casing ("Telangana").
    csv_path = _write_csv(tmp_path, [("ANDHRA PRADESH", 1997), ("TELANGANA", 2019)])

    comments = detect_region_gaps(csv_path, "state.name", "year")

    assert any("Telangana" in c and "as early as" not in c for c in comments)


def test_detect_region_gaps_skips_events_whose_region_is_absent_from_data(tmp_path: Path):
    csv_path = _write_csv(tmp_path, [("Karnataka", 2019), ("Kerala", 2019)])

    comments = detect_region_gaps(csv_path, "state.name", "year")

    assert comments == []


def test_detect_region_gaps_for_table_requires_both_region_name_and_date_columns():
    only_region = {"state.name": {"type": "regionName"}, "count": {"type": "int"}}
    only_date = {"year": {"type": "date", "format": "%Y"}, "count": {"type": "int"}}

    assert detect_region_gaps_for_table("unused.csv", only_region) == []
    assert detect_region_gaps_for_table("unused.csv", only_date) == []


def test_detect_region_gaps_for_table_finds_the_region_and_date_columns_automatically(tmp_path: Path):
    csv_path = _write_csv(tmp_path, [("Andhra Pradesh", 1997), ("Telangana", 2019)])
    data_dictionary = {
        "state.name": {"type": "regionName"},
        "year": {"type": "date", "format": "%Y"},
        "count": {"type": "int"},
    }

    comments = detect_region_gaps_for_table(csv_path, data_dictionary)

    assert any("Telangana" in c for c in comments)
