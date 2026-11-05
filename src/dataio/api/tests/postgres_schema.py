"""Throwaway PostgreSQL schema for tests that need real SQL.

Tests using this are skipped unless DATAIO_TEST_DATABASE_URL points at a
database they may wipe, e.g.

    DATAIO_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/catalogue_test
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

TEST_DATABASE_URL = os.getenv("DATAIO_TEST_DATABASE_URL")
DB_DIR = Path(__file__).resolve().parents[2] / "db"
# Migrations that schema.sql (regenerated from 001-024) doesn't include yet
# and that tests depend on.
MIGRATIONS_AFTER_SCHEMA = [
    "029_dataset_licence.sql",
    "030_access_requests.sql",
    "031_public_download_consent.sql",
]

requires_postgres = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="DATAIO_TEST_DATABASE_URL not set (needs a disposable PostgreSQL database)",
)


def fresh_schema_engine() -> Engine:
    """Wipe the test database and load schema.sql plus later migrations."""
    engine = create_engine(TEST_DATABASE_URL)
    raw = engine.raw_connection()
    try:
        cursor = raw.cursor()
        cursor.execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
        cursor.execute((DB_DIR / "schema.sql").read_text())
        cursor.execute("SET search_path TO public")
        for name in MIGRATIONS_AFTER_SCHEMA:
            cursor.execute((DB_DIR / "migrations" / name).read_text())
        raw.commit()
    finally:
        raw.close()
    engine.dispose()  # drop the connection whose search_path schema.sql emptied
    return engine
