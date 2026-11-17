"""Dataset changelog entries in the database (DIO-53): the table, the admin
service and the public reads. Uses a real PostgreSQL database (see
postgres_schema.py).
"""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from sqlalchemy.exc import IntegrityError  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from dataio.api.database.enums import AccessLevel, SpatialResolution, TemporalResolution  # noqa: E402
from dataio.api.database.models import (  # noqa: E402
    Collection,
    DataOwner,
    Dataset,
    DatasetChangeRecord,
    User,
)
from dataio.api.tests.postgres_schema import fresh_schema_engine, requires_postgres  # noqa: E402


@pytest.fixture(scope="module")
def session_factory():
    if not os.getenv("DATAIO_TEST_DATABASE_URL"):
        pytest.skip("DATAIO_TEST_DATABASE_URL not set")
    engine = fresh_schema_engine()
    factory = sessionmaker(bind=engine)
    session = factory()
    collection = Collection(
        collection_id="EP0001", collection_name="Dengue", category_id="EP", category_name="Epidemiology"
    )
    owner = DataOwner(name="NVBDCP")
    session.add_all([collection, owner])
    session.flush()
    for ds_id, level in (("EP0001DS0001", AccessLevel.DOWNLOAD), ("EP0001DS0003", AccessLevel.NONE)):
        session.add(
            Dataset(
                ds_id=ds_id,
                title=f"Dataset {ds_id}",
                collection_id=collection.id,
                data_owner_id=owner.id,
                spatial_resolution=SpatialResolution.DISTRICT,
                temporal_resolution=TemporalResolution.MONTH,
                access_level=level,
            )
        )
        # One row per INSERT: a batched insert casts to SQLAlchemy's default
        # enum type names, which differ from schema.sql's.
        session.flush()
    session.add_all(
        [
            User(email="admin@example.org", is_admin=True, email_verified=True),
            User(email="admin2@example.org", is_admin=True, email_verified=True),
            User(email="known@example.org", email_verified=True),
        ]
    )
    session.commit()
    session.close()
    yield factory
    engine.dispose()


@pytest.fixture()
def db(session_factory):
    session = session_factory()
    session.query(DatasetChangeRecord).delete()
    session.commit()
    session.close()
    return session_factory


def _record(**overrides) -> DatasetChangeRecord:
    fields = dict(
        dataset_id="EP0001DS0001",
        change_date="2026-11-20",
        change_type="data",
        breaking=False,
        summary="Renamed the district column.",
        reason="Match the v2 naming rules.",
    )
    fields.update(overrides)
    return DatasetChangeRecord(**fields)


# ------------------------------------------------------------------ table


@requires_postgres
def test_the_table_accepts_an_entry_and_defaults_to_draft(db):
    session = db()
    try:
        row = _record()
        session.add(row)
        session.commit()
        session.refresh(row)
        assert row.status == "DRAFT"
        assert row.changes == []
        assert row.created_at is not None
    finally:
        session.close()


@requires_postgres
@pytest.mark.parametrize("overrides", [{"status": "LIVE"}, {"change_type": "schema"}, {"summary": "  "}])
def test_the_table_rejects_bad_status_type_or_blank_text(db, overrides):
    session = db()
    try:
        session.add(_record(**overrides))
        with pytest.raises(IntegrityError):
            session.commit()
    finally:
        session.rollback()
        session.close()
