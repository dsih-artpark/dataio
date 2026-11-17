"""Dataset changelog entries in the database (DIO-53): the table, the admin
service and the public reads. Uses a real PostgreSQL database (see
postgres_schema.py).
"""

from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone

import pytest
from fastapi import HTTPException

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
from dataio.api.services import dataset_change_service as change_module  # noqa: E402
from dataio.api.services.dataset_change_service import DatasetChangeService  # noqa: E402
from dataio.api.services.dataset_changes import DatasetChangeInput  # noqa: E402
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
def db(session_factory, monkeypatch):
    monkeypatch.setattr(change_module, "DBSession", session_factory)
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


# ------------------------------------------------------------ admin service


def _user(factory, email: str) -> User:
    session = factory()
    try:
        return session.query(User).filter(User.email == email).one()
    finally:
        session.close()


def _body(**overrides) -> DatasetChangeInput:
    fields = {
        "type": "data",
        "breaking": True,
        "summary": "Renamed the district column.",
        "reason": "Match the v2 naming rules.",
        "changes": [{"kind": "column_renamed", "table": "district_level", "old": "dist", "new": "district"}],
    }
    fields.update(overrides)
    return DatasetChangeInput.model_validate(fields)


def _status(call) -> int:
    with pytest.raises(HTTPException) as error:
        call()
    return error.value.status_code


@requires_postgres
def test_an_admin_creates_a_draft_dated_today_by_default(db):
    entry = DatasetChangeService().create_draft(_user(db, "admin@example.org"), "EP0001DS0001", _body())

    assert entry["status"] == "DRAFT"
    assert entry["date"] == datetime.now(timezone.utc).date().isoformat()
    assert entry["created_by"] == "admin@example.org"
    assert entry["dataset_title"] == "Dataset EP0001DS0001"
    assert entry["changes"][0]["old"] == "dist"


@requires_postgres
def test_creating_needs_an_admin_and_a_known_dataset(db):
    service = DatasetChangeService()

    assert _status(lambda: service.create_draft(_user(db, "known@example.org"), "EP0001DS0001", _body())) == 403
    assert _status(lambda: service.create_draft(_user(db, "admin@example.org"), "EP0001DS0999", _body())) == 404


@requires_postgres
def test_publishing_records_who_and_when_and_cannot_repeat(db):
    service, admin = DatasetChangeService(), _user(db, "admin@example.org")
    draft = service.create_draft(admin, "EP0001DS0001", _body())

    published = service.publish(admin, draft["id"])

    assert published["status"] == "PUBLISHED"
    assert published["published_by"] == "admin@example.org"
    assert published["published_at"] is not None
    assert _status(lambda: service.publish(admin, draft["id"])) == 409


@requires_postgres
def test_two_admins_publishing_at_once_publish_once(db):
    service = DatasetChangeService()
    draft = service.create_draft(_user(db, "admin@example.org"), "EP0001DS0001", _body())
    admins = [_user(db, "admin@example.org"), _user(db, "admin2@example.org")]

    def publish(admin):
        try:
            return service.publish(admin, draft["id"])["published_by"]
        except HTTPException as error:
            return error.status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(publish, admins))

    assert results.count(409) == 1
    assert {"admin@example.org", "admin2@example.org"} & set(results)


@requires_postgres
def test_editing_a_published_entry_records_the_editor(db):
    service = DatasetChangeService()
    draft = service.create_draft(_user(db, "admin@example.org"), "EP0001DS0001", _body())
    service.publish(_user(db, "admin@example.org"), draft["id"])

    edited = service.update(_user(db, "admin2@example.org"), draft["id"], _body(summary="Corrected summary."))

    assert edited["summary"] == "Corrected summary."
    assert edited["status"] == "PUBLISHED"
    assert edited["updated_by"] == "admin2@example.org"
    assert edited["updated_at"] is not None


@requires_postgres
def test_only_drafts_can_be_deleted(db):
    service, admin = DatasetChangeService(), _user(db, "admin@example.org")
    published = service.create_draft(admin, "EP0001DS0001", _body())
    service.publish(admin, published["id"])
    draft = service.create_draft(admin, "EP0001DS0001", _body())

    assert _status(lambda: service.delete_draft(admin, published["id"])) == 409
    service.delete_draft(admin, draft["id"])
    assert [e["id"] for e in service.list_admin(admin)["entries"]] == [published["id"]]


@requires_postgres
def test_admin_list_filters_and_counts_drafts(db):
    service, admin = DatasetChangeService(), _user(db, "admin@example.org")
    older = service.create_draft(admin, "EP0001DS0001", _body(date="2026-10-01"))
    newer = service.create_draft(admin, "EP0001DS0003", _body(date="2026-11-01"))
    service.publish(admin, older["id"])

    listed = service.list_admin(admin)
    assert [e["id"] for e in listed["entries"]] == [newer["id"], older["id"]]
    assert (listed["total"], listed["drafts"]) == (2, 1)
    assert [e["id"] for e in service.list_admin(admin, status="PUBLISHED")["entries"]] == [older["id"]]
    assert [e["id"] for e in service.list_admin(admin, dataset_id="EP0001DS0003")["entries"]] == [newer["id"]]
    assert _status(lambda: service.list_admin(admin, status="bogus")) == 422
    assert _status(lambda: service.list_admin(admin, offset=-1)) == 400
    assert len(service.list_admin(admin, limit=10000)["entries"]) == 2
    assert _status(lambda: service.list_admin(_user(db, "known@example.org"))) == 403


@requires_postgres
def test_a_malformed_or_unknown_id_is_a_404(db):
    service, admin = DatasetChangeService(), _user(db, "admin@example.org")
    for change_id in ("not-a-uuid", str(uuid.uuid4())):
        assert _status(lambda: service.update(admin, change_id, _body())) == 404
        assert _status(lambda: service.publish(admin, change_id)) == 404
        assert _status(lambda: service.delete_draft(admin, change_id)) == 404


# ------------------------------------------------------------- public reads


def _published(db, dataset_id: str = "EP0001DS0001", **overrides) -> dict:
    service, admin = DatasetChangeService(), _user(db, "admin@example.org")
    draft = service.create_draft(admin, dataset_id, _body(**overrides), archive_prefix="archive/filestore/x")
    return service.publish(admin, draft["id"])


def _set_access(db, dataset_id: str, level: AccessLevel) -> None:
    session = db()
    try:
        session.query(Dataset).filter(Dataset.ds_id == dataset_id).update({"access_level": level})
        session.commit()
    finally:
        session.close()


@requires_postgres
def test_public_lists_only_published_entries_newest_first(db):
    older = _published(db, date="2026-10-01")
    newer = _published(db, date="2026-11-01")
    DatasetChangeService().create_draft(_user(db, "admin@example.org"), "EP0001DS0001", _body())

    listed = DatasetChangeService().list_public()

    assert [e["id"] for e in listed["entries"]] == [newer["id"], older["id"]]
    assert listed["total"] == 2
    assert listed["entries"][0]["dataset_title"] == "Dataset EP0001DS0001"


@requires_postgres
def test_public_output_has_no_internal_fields(db):
    _published(db)

    entry = DatasetChangeService().list_public_for_dataset("EP0001DS0001")["entries"][0]

    assert set(entry) == {
        "id", "dataset_id", "dataset_title", "restricted", "date", "type", "breaking",
        "summary", "reason", "changes",
    }
    assert entry["restricted"] is False
    assert entry["changes"][0]["old"] == "dist"


@requires_postgres
def test_restricted_datasets_hide_old_new_and_note(db):
    _published(db, "EP0001DS0003", changes=[
        {"kind": "values_recoded", "table": "cases", "column": "status", "old": "A", "new": "B", "note": "n"},
    ])

    entry = DatasetChangeService().list_public_for_dataset("EP0001DS0003")["entries"][0]

    assert entry["restricted"] is True
    assert entry["changes"] == [{"kind": "values_recoded", "table": "cases", "column": "status",
                                 "old": None, "new": None, "note": None}]
    assert entry["summary"] == "Renamed the district column."


@requires_postgres
def test_masking_follows_the_current_access_level(db):
    _published(db)
    _set_access(db, "EP0001DS0001", AccessLevel.NONE)
    try:
        entry = DatasetChangeService().list_public()["entries"][0]
        assert entry["restricted"] is True and entry["changes"][0]["old"] is None
    finally:
        _set_access(db, "EP0001DS0001", AccessLevel.DOWNLOAD)


@requires_postgres
def test_entries_of_a_deleted_dataset_leave_the_public_lists(db):
    session = db()
    try:
        session.add(_record(dataset_id="EP0001DS0999", status="PUBLISHED"))
        session.commit()
    finally:
        session.close()
    service = DatasetChangeService()

    assert service.list_public()["entries"] == []
    assert _status(lambda: service.list_public_for_dataset("EP0001DS0999")) == 404
    assert [e["dataset_id"] for e in service.list_admin(_user(db, "admin@example.org"))["entries"]] == [
        "EP0001DS0999"]


@requires_postgres
def test_public_paging_limits(db):
    for _ in range(3):
        _published(db)
    service = DatasetChangeService()

    assert _status(lambda: service.list_public(offset=-1)) == 400
    assert len(service.list_public(limit=10000)["entries"]) == 3
    assert len(service.list_public(limit=2)["entries"]) == 2
    assert len(service.list_public(limit=0)["entries"]) == 1


@requires_postgres
def test_routes_wire_admin_and_public_calls(db):
    import importlib

    from fastapi.testclient import TestClient

    from dataio.api.auth.jwt import get_current_web_user

    app = importlib.import_module("dataio.api").app
    client = TestClient(app)
    body = {"type": "data", "breaking": False, "summary": "S.", "reason": "R.", "changes": []}
    try:
        app.dependency_overrides[get_current_web_user] = lambda: _user(db, "known@example.org")
        assert client.post("/api/v1/web/admin/datasets/EP0001DS0001/changes", json=body).status_code == 403

        app.dependency_overrides[get_current_web_user] = lambda: _user(db, "admin@example.org")
        draft = client.post("/api/v1/web/admin/datasets/EP0001DS0001/changes", json=body)
        assert draft.status_code == 200 and draft.json()["status"] == "DRAFT"
        change_id = draft.json()["id"]
        assert client.get("/api/v1/web/public/datasets/EP0001DS0001/changes").json()["entries"] == []
        edited = client.patch(f"/api/v1/web/admin/dataset-changes/{change_id}", json={**body, "summary": "T."})
        assert edited.json()["summary"] == "T."
        assert client.get("/api/v1/web/admin/dataset-changes?status=DRAFT").json()["total"] == 1
        assert client.delete(f"/api/v1/web/admin/dataset-changes/{change_id}").status_code == 204

        again = client.post("/api/v1/web/admin/datasets/EP0001DS0001/changes", json=body).json()["id"]
        assert client.post(f"/api/v1/web/admin/dataset-changes/{again}/publish").status_code == 200
        assert len(client.get("/api/v1/web/public/dataset-changes").json()["entries"]) == 1
        assert len(client.get("/api/v1/web/public/datasets/EP0001DS0001/changes").json()["entries"]) == 1
    finally:
        app.dependency_overrides.clear()
