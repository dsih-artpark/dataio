"""
Unit tests for the SQL migration runner.
"""

import os
from pathlib import Path

os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from dataio.db.migration_runner import (
    build_parser,
    discover_forward_migrations,
    normalize_migration_name,
    parse_migration_file,
)


def test_parse_migration_file_ignores_rollbacks(tmp_path: Path):
    rollback = tmp_path / "014_auth_signoff_and_sso_rollback.sql"
    rollback.write_text(
        "DELETE FROM db_migration_history WHERE migration_number = 14;",
        encoding="utf-8",
    )

    assert parse_migration_file(rollback) is None


def test_discover_forward_migrations_skips_untracked_sql(tmp_path: Path):
    tracked = tmp_path / "013_auth_hardening.sql"
    tracked.write_text(
        "BEGIN;\nSELECT add_migration(13, '013_auth_hardening');\nCOMMIT;\n",
        encoding="utf-8",
    )
    untracked = tmp_path / "legacy_registration_and_verification.sql"
    untracked.write_text("-- legacy manual migration without add_migration\n", encoding="utf-8")
    rollback = tmp_path / "013_auth_hardening_rollback.sql"
    rollback.write_text(
        "DELETE FROM db_migration_history WHERE migration_number = 13;",
        encoding="utf-8",
    )

    migrations, skipped = discover_forward_migrations(tmp_path)

    assert [migration.number for migration in migrations] == [13]
    assert migrations[0].name == "013_auth_hardening"
    assert sorted(path.name for path in skipped) == [
        "013_auth_hardening_rollback.sql",
        "legacy_registration_and_verification.sql",
    ]


def test_discover_forward_migrations_rejects_duplicate_numbers(tmp_path: Path):
    first = tmp_path / "013_auth_hardening.sql"
    first.write_text(
        "SELECT add_migration(13, '013_auth_hardening');\n",
        encoding="utf-8",
    )
    second = tmp_path / "013_other.sql"
    second.write_text(
        "SELECT add_migration(13, '013_other');\n",
        encoding="utf-8",
    )

    try:
        discover_forward_migrations(tmp_path)
    except ValueError as exc:
        assert "Duplicate migration number 13" in str(exc)
    else:
        raise AssertionError("Expected duplicate migration numbers to raise ValueError")


def test_parser_supports_min_number():
    parser = build_parser()
    args = parser.parse_args(["--min-number", "13"])
    assert args.min_number == 13


def test_normalize_migration_name_handles_sql_suffix():
    assert normalize_migration_name("002_helper_functions_to_add_datasets.sql") == (
        "002_helper_functions_to_add_datasets"
    )
    assert normalize_migration_name("002_helper_functions_to_add_datasets") == (
        "002_helper_functions_to_add_datasets"
    )


def test_current_legacy_auth_and_chat_files_are_now_tracked():
    migrations_dir = Path(__file__).resolve().parents[2] / "db" / "migrations"
    migrations, _skipped = discover_forward_migrations(migrations_dir)
    numbers = {migration.number for migration in migrations}

    assert {9, 10, 11, 12}.issubset(numbers)
