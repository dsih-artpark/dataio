from __future__ import annotations

from pathlib import Path

from dataio.api.services.digitization_log import DigitizationLog, load_digitization_log

FIXTURE_YAML = """
schemaVersion: "1.0"
preparedBy: "engineer@artpark.in"
preparedAt: "2026-07-21"
sourceDocuments:
  - sourceTableID: "Table 4.2"
    sourcePage: "p. 37"
    sourceDocument: "https://example.gov/report.pdf"
observations:
  - id: "obs-1"
    description: "Telangana absent before 2014 - didn't exist as a separate state yet."
    resolution: "expected"
  - id: "obs-2"
    description: "A handful of 2021 rows have negative values, cause unclear."
    resolution: "needs_investigation"
normalizationSteps:
  - id: "norm-1"
    description: "Renamed 'Dist_Name' to 'district_name'."
    field: "district_name"
    changeType: "rename"
notes: "free text"
"""


def test_load_digitization_log_returns_none_for_missing_path(tmp_path: Path):
    assert load_digitization_log(tmp_path / "does_not_exist.yaml") is None


def test_load_digitization_log_returns_none_for_none_path():
    assert load_digitization_log(None) is None


def test_load_digitization_log_parses_fixture(tmp_path: Path):
    log_path = tmp_path / "digitization_log.yaml"
    log_path.write_text(FIXTURE_YAML, encoding="utf-8")

    log = load_digitization_log(log_path)

    assert isinstance(log, DigitizationLog)
    assert log.sourceDocuments[0].sourceTableID == "Table 4.2"
    assert len(log.observations) == 2
    assert log.normalizationSteps[0].field == "district_name"


def test_already_explained_summary_includes_expected_but_not_needs_investigation(tmp_path: Path):
    log_path = tmp_path / "digitization_log.yaml"
    log_path.write_text(FIXTURE_YAML, encoding="utf-8")
    log = load_digitization_log(log_path)

    summary = log.already_explained_summary()
    assert "obs-1" in summary
    assert "Telangana" in summary
    assert "obs-2" not in summary
    assert "norm-1" in summary


def test_needs_investigation_summary_includes_only_unresolved_observations(tmp_path: Path):
    log_path = tmp_path / "digitization_log.yaml"
    log_path.write_text(FIXTURE_YAML, encoding="utf-8")
    log = load_digitization_log(log_path)

    summary = log.needs_investigation_summary()
    assert "obs-2" in summary
    assert "obs-1" not in summary
