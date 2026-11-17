"""The public dataset changelog: one entry per update to a published dataset's
data or metadata (DIO-53).

These models are the checks for entries recorded through the admin panel and
served to the dataset page's "Version history". Entries are public, so unknown
fields are rejected: no archive paths, QC scorecard links or Linear IDs.
"""

from __future__ import annotations

import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

ChangeKind = Literal[
    "table_added",
    "table_removed",
    "table_renamed",
    "column_added",
    "column_removed",
    "column_renamed",
    "unit_changed",
    "values_recoded",
    "rows_changed",
    "metadata_updated",
]

# Prose fields: surrounding spaces don't count, so blank text is rejected.
ProseText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class Change(BaseModel):
    # strict: old/new must be text, so a number can't silently lose digits
    # (0.10 -> "0.1"); names and values are kept exactly as written.
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: ChangeKind
    # Not blank: an empty row would show publicly as "— —". Kept as written otherwise.
    table: str = Field(pattern=r"\S")
    column: str | None = None
    old: str | None = None
    new: str | None = None
    note: str | None = None


class DatasetChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str = Field(pattern=r"^[A-Z]{2}\d{4}DS\d{4}$")
    date: datetime.date
    type: Literal["data", "metadata"]
    breaking: bool
    summary: ProseText
    reason: ProseText
    changes: list[Change] = Field(default_factory=list)  # optional: summary + reason are enough


class DatasetChangeInput(BaseModel):
    """Body for creating or editing an entry; the dataset comes from the URL.
    No date means today."""

    model_config = ConfigDict(extra="forbid")

    date: datetime.date | None = None
    type: Literal["data", "metadata"]
    breaking: bool
    summary: ProseText
    reason: ProseText
    changes: list[Change] = Field(default_factory=list)
