"""
Utilities for applying forward SQL migrations in order.

This runner intentionally ignores rollback files and only executes migrations
that register themselves via ``add_migration(...)``.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv

MIGRATION_RE = re.compile(
    r"SELECT\s+add_migration\(\s*(?P<number>\d+)\s*,\s*'(?P<name>[^']+)'\s*\)",
    re.IGNORECASE,
)
FILENAME_RE = re.compile(r"^(?P<number>\d{3})_.*\.sql$")


@dataclass(frozen=True)
class MigrationFile:
    number: int
    name: str
    path: Path


def normalize_migration_name(name: str) -> str:
    normalized = name.strip()
    if normalized.endswith(".sql"):
        normalized = normalized[:-4]
    return normalized


def migration_dir() -> Path:
    configured = os.getenv("MIGRATIONS_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parent / "migrations"


def parse_migration_file(path: Path) -> MigrationFile | None:
    if path.name.endswith("_rollback.sql"):
        return None

    filename_match = FILENAME_RE.match(path.name)
    if not filename_match:
        return None

    contents = path.read_text(encoding="utf-8")
    migration_match = MIGRATION_RE.search(contents)
    if not migration_match:
        return None

    file_number = int(filename_match.group("number"))
    migration_number = int(migration_match.group("number"))
    migration_name = migration_match.group("name")

    if migration_number != file_number:
        raise ValueError(
            f"Migration number mismatch in {path.name}: filename says {file_number}, "
            f"SQL registers {migration_number}"
        )

    return MigrationFile(
        number=migration_number,
        name=normalize_migration_name(migration_name),
        path=path,
    )


def discover_forward_migrations(migrations_path: Path) -> tuple[list[MigrationFile], list[Path]]:
    if not migrations_path.exists():
        raise FileNotFoundError(f"Migration directory does not exist: {migrations_path}")

    migrations: list[MigrationFile] = []
    skipped: list[Path] = []
    seen_numbers: dict[int, Path] = {}

    for path in sorted(migrations_path.glob("*.sql")):
        migration = parse_migration_file(path)
        if migration is None:
            skipped.append(path)
            continue

        if migration.number in seen_numbers:
            raise ValueError(
                f"Duplicate migration number {migration.number} in "
                f"{seen_numbers[migration.number].name} and {path.name}"
            )

        seen_numbers[migration.number] = path
        migrations.append(migration)

    return migrations, skipped


def connect_kwargs() -> dict[str, object]:
    load_dotenv(override=True)
    return {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "dbname": os.getenv("DB_NAME"),
    }


def get_applied_migrations() -> dict[int, str]:
    kwargs = connect_kwargs()
    with psycopg2.connect(**kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.db_migration_history')")
            history_table = cur.fetchone()[0]
            if history_table is None:
                return {}

            cur.execute(
                """
                SELECT migration_number, migration_name
                FROM db_migration_history
                ORDER BY migration_number
                """
            )
            return {int(number): name for number, name in cur.fetchall()}


def run_migration(path: Path) -> None:
    psql = shutil.which("psql")
    if not psql:
        raise RuntimeError("psql is required to run SQL migrations but was not found on PATH")

    kwargs = connect_kwargs()
    env = os.environ.copy()
    if kwargs.get("password"):
        env["PGPASSWORD"] = str(kwargs["password"])

    cmd = [
        psql,
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        str(kwargs["host"]),
        "-p",
        str(kwargs["port"]),
        "-U",
        str(kwargs["user"]),
        "-d",
        str(kwargs["dbname"]),
        "-f",
        str(path),
    ]
    subprocess.run(cmd, check=True, env=env)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="all-migrations",
        description=(
            "Apply only new forward SQL migrations recorded in db_migration_history. "
            "Rollback files are ignored."
        ),
    )
    parser.add_argument(
        "--migrations-dir",
        default=None,
        help="Override the migrations directory. Defaults to MIGRATIONS_DIR or src/dataio/db/migrations.",
    )
    parser.add_argument(
        "--min-number",
        type=int,
        default=None,
        help=(
            "Only apply migrations with a number greater than or equal to this value. "
            "Defaults to MIGRATION_MIN_NUMBER when set."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    migrations_path = (
        Path(args.migrations_dir).expanduser().resolve()
        if args.migrations_dir
        else migration_dir()
    )
    migrations, skipped = discover_forward_migrations(migrations_path)
    try:
        applied = get_applied_migrations()
    except OperationalError as exc:
        print(
            "Could not connect to Postgres to inspect db_migration_history. "
            "Check DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME and database reachability.",
            file=sys.stderr,
        )
        print(f"Connection error: {exc}", file=sys.stderr)
        return 2

    for number, name in applied.items():
        discovered = next((migration for migration in migrations if migration.number == number), None)
        if discovered and discovered.name != normalize_migration_name(name):
            raise ValueError(
                f"Applied migration {number} is recorded as {name!r}, "
                f"but file {discovered.path.name} registers {discovered.name!r}"
            )

    min_number = args.min_number
    if min_number is None and os.getenv("MIGRATION_MIN_NUMBER"):
        min_number = int(os.getenv("MIGRATION_MIN_NUMBER", "0"))

    pending = [migration for migration in migrations if migration.number not in applied]
    if min_number is not None:
        pending = [migration for migration in pending if migration.number >= min_number]

    print(f"Migration directory: {migrations_path}")
    if min_number is not None:
        print(f"Minimum migration number: {min_number}")
    if skipped:
        print("Skipped SQL files without forward-migration markers:")
        for path in skipped:
            print(f"  - {path.name}")

    if not pending:
        print("No new forward migrations to apply.")
        return 0

    print("Applying forward migrations:")
    for migration in pending:
        print(f"  - {migration.number:03d} {migration.name} ({migration.path.name})")
        run_migration(migration.path)

    print(f"Applied {len(pending)} migration(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
