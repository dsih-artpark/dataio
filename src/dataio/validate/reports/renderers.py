from __future__ import annotations

from dataio.validate.reports.models import ValidationResult


def render_text_summary(result: ValidationResult) -> str:
    lines = [
        f"status: {result.status}",
        f"dataset kind: {result.dataset_kind}",
        f"metadata spec version: {result.metadata_spec_version or 'unknown'}",
        f"errors: {result.summary.errors}",
        f"warnings: {result.summary.warnings}",
        f"rows checked: {result.summary.rows_checked}",
        f"tables checked: {result.summary.tables_checked}",
    ]
    if result.findings:
        lines.append("")
        lines.append("findings:")
        for finding in result.findings:
            location: list[str] = []
            if finding.table:
                location.append(f"table={finding.table}")
            if finding.row is not None:
                location.append(f"row={finding.row}")
            if finding.field:
                location.append(f"field={finding.field}")
            suffix = f" ({', '.join(location)})" if location else ""
            lines.append(f"- [{finding.severity}] {finding.code}: {finding.message}{suffix}")
    return "\n".join(lines)


def render_markdown_summary(result: ValidationResult) -> str:
    lines = [
        "# Validation Result",
        "",
        f"- Status: `{result.status}`",
        f"- Dataset kind: `{result.dataset_kind}`",
        f"- Metadata spec version: `{result.metadata_spec_version or 'unknown'}`",
        f"- Errors: `{result.summary.errors}`",
        f"- Warnings: `{result.summary.warnings}`",
        f"- Rows checked: `{result.summary.rows_checked}`",
        f"- Tables checked: `{result.summary.tables_checked}`",
    ]
    if result.findings:
        lines.extend(["", "## Findings"])
        for finding in result.findings:
            lines.append(f"- **{finding.severity.upper()}** `{finding.code}`: {finding.message}")
    return "\n".join(lines)
