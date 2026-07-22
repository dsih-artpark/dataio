from __future__ import annotations

import pytest

from dataio.api.database.enums import TemporalResolution
from dataio.api.database.temporal_resolution_mapping import (
    is_lossy_temporal_resolution,
    resolve_temporal_resolution,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("YEAR", TemporalResolution.YEAR.value),
        ("year", TemporalResolution.YEAR.value),
        ("quinquennial", TemporalResolution.YEAR.value),
        ("decadal", TemporalResolution.YEAR.value),
        ("monthly", TemporalResolution.MONTH.value),
        ("Weekly", TemporalResolution.WEEK.value),
        ("daily", TemporalResolution.DATE.value),
        ("hourly", TemporalResolution.HOUR.value),
        ("static", TemporalResolution.NONE.value),
    ],
)
def test_resolve_temporal_resolution_maps_known_values(raw, expected):
    assert resolve_temporal_resolution(raw) == expected


def test_resolve_temporal_resolution_raises_on_unknown_value():
    with pytest.raises(ValueError) as exc_info:
        resolve_temporal_resolution("fortnightly")
    # error message should list the real valid enum values so the caller
    # can fix the source metadata.yaml or extend the mapping table
    assert "YEAR" in str(exc_info.value)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("quinquennial", True),
        ("decadal", True),
        ("year", False),
        ("monthly", False),
    ],
)
def test_is_lossy_temporal_resolution(raw, expected):
    assert is_lossy_temporal_resolution(raw) == expected
