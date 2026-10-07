"""Digitization Log: a structured record a data engineer fills in alongside
a raw dataset, capturing (a) source-document facts and (b) what they
observed/normalized while digitizing it. The LLM drafter (draft_service.py)
reads this so it doesn't re-flag something already explained on purpose -
see metadata-architecture memo, Stage 02.

Stored as <dataset_folder>/digitization_log.yaml, sibling to metadata.yaml
and info.yml. A missing file is a valid, expected state: the drafter just
flags everything instead of suppressing anything as already-explained.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class SourceDocument(BaseModel):
    # Matches the real per-row metadata.yaml columns exactly (see
    # metadata_field_reference.md): sourceDocument is the source PDF's
    # URL/filename, sourceTableID the specific table within it, sourcePage
    # the printed page number. There is no separate "statement" field in
    # the real schema.
    sourceDocument: str | None = None
    sourceTableID: str | None = None
    sourcePage: str | None = None


class Observation(BaseModel):
    id: str
    description: str
    affectedFields: list[str] = Field(default_factory=list)
    # expected: not a data problem, don't re-flag it.
    # needs_investigation: engineer noticed it but hasn't resolved it -
    #   the drafter SHOULD still surface this one.
    # wont_fix: known issue, accepted as-is.
    resolution: str = "expected"


class NormalizationStep(BaseModel):
    id: str
    description: str
    field: str | None = None
    changeType: str | None = None  # rename | dtype_cast | unit_conversion | value_recode | dedup | other


class DigitizationLog(BaseModel):
    schemaVersion: str = "1.0"
    preparedBy: str | None = None
    preparedAt: str | None = None
    sourceDocuments: list[SourceDocument] = Field(default_factory=list)
    observations: list[Observation] = Field(default_factory=list)
    normalizationSteps: list[NormalizationStep] = Field(default_factory=list)
    notes: str | None = None

    def already_explained_summary(self) -> str:
        """Renders the parts of this log the drafter must not re-derive or
        re-flag, for direct inclusion in the LLM prompt.
        """
        lines: list[str] = []
        for obs in self.observations:
            if obs.resolution == "expected":
                lines.append(f"- [{obs.id}] {obs.description}")
        for step in self.normalizationSteps:
            field_note = f" (field: {step.field})" if step.field else ""
            lines.append(f"- [{step.id}] {step.description}{field_note}")
        return "\n".join(lines)

    def needs_investigation_summary(self) -> str:
        """Observations the engineer flagged but did not resolve - the
        drafter should still surface these, not suppress them.
        """
        lines = [
            f"- [{obs.id}] {obs.description}"
            for obs in self.observations
            if obs.resolution == "needs_investigation"
        ]
        return "\n".join(lines)


def load_digitization_log(path: str | Path | None) -> DigitizationLog | None:
    """Returns None if `path` is None or the file doesn't exist - a missing
    digitization log is expected and valid, not an error.
    """
    if not path:
        return None
    file_path = Path(path)
    if not file_path.exists():
        return None
    with open(file_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return DigitizationLog.model_validate(raw)
