from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from dataio.api.services.dataset_changes import DatasetChange, DatasetChangeInput


def _entry(**overrides) -> dict:
    entry = {
        "dataset_id": "EP0006DS0097",
        "date": "2026-11-20",
        "type": "data",
        "breaking": True,
        "summary": "Renamed the district column.",
        "reason": "Match the v2 naming rules.",
        "changes": [{"kind": "column_renamed", "table": "district_level",
                     "old": "dist", "new": "district"}],
    }
    entry.update(overrides)
    return entry


def _with_change(**fields) -> dict:
    return _entry(changes=[{**_entry()["changes"][0], **fields}])


def test_a_valid_entry_validates():
    entry = DatasetChange.model_validate(_entry())

    assert entry.dataset_id == "EP0006DS0097"
    assert entry.date == date(2026, 11, 20)
    assert entry.changes[0].old == "dist"
    assert entry.changes[0].column is None


def test_values_are_kept_exactly_as_written():
    change = DatasetChange.model_validate(_with_change(old="0123", new=" <NA> ")).changes[0]

    assert (change.old, change.new) == ("0123", " <NA> ")


@pytest.mark.parametrize("entry, field", [
    (_entry(summery="x"), "summery"),  # unknown field (also blocks archive paths / Linear IDs)
    (_with_change(kind="column_moved"), "changes.0.kind"),
    (_entry(dataset_id="EP06DS97"), "dataset_id"),
    (_entry(date="20 Nov 2026"), "date"),
    (_entry(type="schema"), "type"),
    (_entry(summary="   "), "summary"),
    (_entry(reason=""), "reason"),
    (_with_change(old=10), "changes.0.old"),  # a number would lose digits (0.10 -> 0.1)
])
def test_a_bad_entry_is_rejected_naming_the_field(entry, field):
    with pytest.raises(ValidationError) as err:
        DatasetChange.model_validate(entry)

    assert field in {".".join(map(str, e["loc"])) for e in err.value.errors()}


def test_an_entry_may_have_no_change_rows():
    assert DatasetChange.model_validate(_entry(changes=[])).changes == []


def test_input_without_a_date_has_none():
    body = {k: v for k, v in _entry().items() if k not in ("dataset_id", "date")}

    assert DatasetChangeInput.model_validate(body).date is None


def test_input_rejects_a_dataset_id_field():
    with pytest.raises(ValidationError):
        DatasetChangeInput.model_validate({k: v for k, v in _entry().items() if k != "date"})
