"""The public dataset changelog: one entry per update to a published dataset's
data or metadata (DIO-53).

These models are the checks for entries recorded through the admin panel and
served to the dataset page's "Version history". Entries are public, so unknown
fields are rejected, so archive paths and internal references can't slip in.
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

# Entries are public and shown in full, so text and row counts have limits.
SUMMARY_MAX = 300
REASON_MAX = 2000
CELL_MAX = 500
ROWS_MAX = 200

# Prose fields: surrounding spaces don't count, so blank text is rejected.
Summary = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=SUMMARY_MAX)]
Reason = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=REASON_MAX)]


class Change(BaseModel):
    # strict: old/new must be text, so a number can't silently lose digits
    # (0.10 -> "0.1"); names and values are kept exactly as written.
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: ChangeKind
    # Not blank: an empty row would show publicly as "— —". Kept as written otherwise.
    table: str = Field(pattern=r"\S", max_length=CELL_MAX)
    column: str | None = Field(default=None, max_length=CELL_MAX)
    old: str | None = Field(default=None, max_length=CELL_MAX)
    new: str | None = Field(default=None, max_length=CELL_MAX)
    note: str | None = Field(default=None, max_length=CELL_MAX)


class DatasetChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str = Field(pattern=r"^[A-Z]{2}\d{4}DS\d{4}$")
    date: datetime.date
    type: Literal["data", "metadata"]
    breaking: bool
    summary: Summary
    reason: Reason
    changes: list[Change] = Field(default_factory=list, max_length=ROWS_MAX)  # optional: summary + reason are enough


class DatasetChangeInput(BaseModel):
    """Body for creating or editing an entry; the dataset comes from the URL.
    No date means today."""

    model_config = ConfigDict(extra="forbid")

    date: datetime.date | None = None
    type: Literal["data", "metadata"]
    breaking: bool
    summary: Summary
    reason: Reason
    changes: list[Change] = Field(default_factory=list, max_length=ROWS_MAX)
