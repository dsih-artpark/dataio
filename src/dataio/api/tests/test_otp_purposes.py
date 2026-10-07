"""Every OTP purpose the app creates must pass otp_tokens_purpose_check.

The constraint lives only in SQL migrations, so a purpose missing from it
passes unit tests and then fails on a real database (as dataset deletion did
until migration 025). This reads the newest migration that defines the
constraint and checks each create_otp(..., purpose=...) call in the app
against it, so adding a purpose or rewriting the constraint can't drop one
silently."""

from __future__ import annotations

import re
from pathlib import Path

from dataio.db.migration_runner import discover_forward_migrations

SRC = Path(__file__).resolve().parents[2]  # src/dataio
MIGRATIONS = SRC / "db" / "migrations"
CREATE_OTP_PURPOSE = re.compile(r'create_otp\([^)]*?purpose=(f?)"([^"]*)"', re.DOTALL)


def _latest_purpose_constraint_sql() -> str:
    migrations, _ = discover_forward_migrations(MIGRATIONS)
    defining = [
        m for m in migrations
        if "ADD CONSTRAINT otp_tokens_purpose_check" in m.path.read_text(encoding="utf-8")
    ]
    latest = max(defining, key=lambda m: m.number)
    return latest.path.read_text(encoding="utf-8")


def _like_to_regex(pattern: str, escape: str | None) -> re.Pattern:
    out, i = [], 0
    while i < len(pattern):
        char = pattern[i]
        if escape and char == escape and i + 1 < len(pattern):
            out.append(re.escape(pattern[i + 1]))
            i += 2
            continue
        out.append(".*" if char == "%" else "." if char == "_" else re.escape(char))
        i += 1
    return re.compile("".join(out), re.DOTALL)


def _allowed():
    sql = _latest_purpose_constraint_sql()
    fixed = set(re.findall(r"'([^']*)'", re.search(r"purpose IN \(([^)]*)\)", sql).group(1)))
    patterns = [
        _like_to_regex(pattern, escape or None)
        for pattern, escape in re.findall(r"purpose LIKE '([^']*)'(?: ESCAPE '(.)')?", sql)
    ]
    return fixed, patterns


def _is_allowed(purpose: str) -> bool:
    fixed, patterns = _allowed()
    return purpose in fixed or any(p.fullmatch(purpose) for p in patterns)


def _purposes_used_in_code() -> set[str]:
    purposes = set()
    for path in (SRC / "api").rglob("*.py"):
        if "tests" in path.parts:
            continue
        for is_fstring, value in CREATE_OTP_PURPOSE.findall(path.read_text(encoding="utf-8")):
            # an f-string placeholder (e.g. a dataset id) gets a realistic value
            purposes.add(re.sub(r"\{[^}]*\}", "CS0007DS0113", value) if is_fstring else value)
    return purposes


def test_every_purpose_the_app_creates_is_allowed_by_the_database():
    used = _purposes_used_in_code()

    # guards the scan itself: these are the known create_otp callers
    assert {"login", "registration", "account_deletion", "dataset_deletion:CS0007DS0113"} <= used
    assert [p for p in sorted(used) if not _is_allowed(p)] == []


def test_the_constraint_still_rejects_malformed_and_unknown_purposes():
    assert not _is_allowed("dataset_deletion:")  # a dataset id is required
    assert not _is_allowed("datasetXdeletion:CS0007DS0113")  # "_" is literal
    assert not _is_allowed("password_reset")
