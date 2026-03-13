from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class Finding(BaseModel):
    severity: str
    code: str
    message: str
    path: str | None = None
    line: int | None = None
    column: int | None = None
    table: str | None = None
    row: int | None = None
    field: str | None = None
    rule_id: str | None = None
    hint: str | None = None


class ValidationSummary(BaseModel):
    errors: int = 0
    warnings: int = 0
    infos: int = 0
    rows_checked: int = 0
    tables_checked: int = 0


class ValidationResult(BaseModel):
    status: str = "pass"
    dataset_kind: str
    metadata_spec_version: str | None = None
    summary: ValidationSummary = Field(default_factory=ValidationSummary)
    findings: list[Finding] = Field(default_factory=list)
    inferred: dict[str, Any] = Field(default_factory=dict)

    def add_finding(self, finding: Finding) -> None:
        self.findings.append(finding)
        if finding.severity == "error":
            self.summary.errors += 1
            self.status = "fail"
        elif finding.severity == "warning":
            self.summary.warnings += 1
            if self.status == "pass":
                self.status = "warn"
        else:
            self.summary.infos += 1
