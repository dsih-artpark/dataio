"""Signed-out downloads, dataset previews and access requests (public site).

The synthetic-preview rules are plain functions and always run. The rest
uses a real PostgreSQL database (see postgres_schema.py); S3, email and the
invite flow are replaced by fakes.
"""

from __future__ import annotations

import importlib
import os
import uuid

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from sqlalchemy.orm import sessionmaker

from dataio.api.auth import security
from dataio.api.database.enums import AccessLevel, ResourceType, SpatialResolution, TemporalResolution
from dataio.api.database.models import (
    AccessRequest,
    Collection,
    DataOwner,
    Dataset,
    DatasetDownload,
    User,
    UserPermission,
)
from dataio.api.services import email_service as email_module
from dataio.api.services import filestore_service as filestore_module
from dataio.api.services import public_access_service as access_module
from dataio.api.services import web_admin_service as admin_module
from dataio.api.services.public_access_service import (
    HIDDEN_TEXT,
    PublicAccessService,
    count_words,
    synthetic_rows,
)
from dataio.api.tests.postgres_schema import fresh_schema_engine, requires_postgres

# ------------------------------------------------------- synthetic preview


def test_synthetic_preview_keeps_places_codes_and_dates_and_randomises_numbers():
    columns = ["district", "lgd_district_code", "year", "month", "dengue_cases", "rate", "notes"]
    rows = [
        ["Bagalkote", "524", "2023", "7", "18", "1.25", "outbreak in ward 4"],
        ["Ballari", "528", "2023", "7", "6", "0.50", ""],
    ]
    out = synthetic_rows("EP0001DS0001", columns, rows)

    assert [row[:4] for row in out] == [row[:4] for row in rows]
    assert all(row[4].isdigit() for row in out)
    assert all(len(row[5].split(".")[1]) == 2 for row in out)
    assert out[0][6] == HIDDEN_TEXT
    assert out[1][6] == ""  # empty stays empty


def test_synthetic_preview_is_stable_per_dataset():
    columns, rows = ["district", "cases"], [["Bidar", "4"], ["Kolar", "9"]]
    assert synthetic_rows("A", columns, rows) == synthetic_rows("A", columns, rows)


def test_count_words():
    assert count_words("  one two\nthree  ") == 3
    assert count_words("") == 0


# ------------------------------------------------------------- database


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
    for ds_id, level in (("EP0001DS0001", AccessLevel.DOWNLOAD), ("EP0001DS0002", AccessLevel.VIEW), ("EP0001DS0003", AccessLevel.NONE)):
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
            User(email="known@example.org", email_verified=True),
        ]
    )
    session.commit()
    session.close()
    yield factory
    engine.dispose()


class FakeFilestore:
    def list_files_in_s3(self, dataset_id, version_type):
        return [{"table_name": "main", "download_link": f"https://s3.example/{dataset_id}/main.csv", "metadata": {}}]

    def read_table_head(self, dataset_id, version_type, max_rows=5):
        return {
            "table": "main",
            "columns": ["district", "year", "cases"],
            "rows": [["Bidar", "2023", "4"], ["Kolar", "2023", "9"]],
        }


class FakeEmail:
    sent: list = []

    def send_email(self, to_email, subject, html_body, text_body=None):
        FakeEmail.sent.append({"to": to_email, "subject": subject, "text": text_body, "html": html_body})
        return True


@pytest.fixture()
def db(session_factory, monkeypatch):
    monkeypatch.setattr(access_module, "DBSession", session_factory)
    monkeypatch.setattr(filestore_module, "FilestoreService", FakeFilestore)
    monkeypatch.setattr(email_module, "EmailService", FakeEmail)
    monkeypatch.setattr(security, "enforce_ip_rate_limit", lambda *args, **kwargs: None)
    FakeEmail.sent = []
    session = session_factory()
    session.query(AccessRequest).delete()
    session.query(DatasetDownload).delete()
    session.query(UserPermission).delete()
    session.query(User).filter(User.email.notin_(["admin@example.org", "known@example.org"])).delete()
    session.commit()
    session.close()
    return session_factory


def _admin(factory) -> User:
    session = factory()
    try:
        return session.query(User).filter(User.email == "admin@example.org").one()
    finally:
        session.close()


def _request(**overrides) -> dict:
    data = dict(
        dataset_id="EP0001DS0002",
        full_name="Asha Rao",
        email="Asha@Example.org",
        organisation="District NGO",
        organisation_type="NGO / non-profit",
        organisation_type_other=None,
        purpose="Planning dengue control in two districts.",
        consent=True,
    )
    data.update(overrides)
    return data


# ------------------------------------------------------------ downloads


@requires_postgres
def test_open_dataset_download_is_logged_with_the_visitor(db):
    result = PublicAccessService().get_open_download_urls(
        "EP0001DS0001", "203.0.113.7", "pytest", email=" Asha@Example.org ", accept_terms=True
    )
    assert [t["table_name"] for t in result["tables"]] == ["main"]
    visitor_id = uuid.UUID(result["visitor_id"])

    session = db()
    log = session.query(DatasetDownload).one()
    session.close()
    assert log.user_email is None
    assert log.access_channel == "PUBLIC"
    assert log.ip_address == "203.0.113.7"
    assert log.visitor_email == "asha@example.org"
    assert log.visitor_id == visitor_id
    assert log.terms_accepted_at is not None


@requires_postgres
def test_returning_visitor_downloads_with_only_the_cookie_id(db):
    service = PublicAccessService()
    first = service.get_open_download_urls("EP0001DS0001", email="asha@example.org", accept_terms=True)
    again = service.get_open_download_urls("EP0001DS0001", visitor_id=first["visitor_id"])
    assert again["visitor_id"] == first["visitor_id"]
    assert again["tables"]

    session = db()
    logs = session.query(DatasetDownload).order_by(DatasetDownload.downloaded_at).all()
    session.close()
    assert [log.visitor_email for log in logs] == ["asha@example.org", "asha@example.org"]
    # The second download keeps the time the visitor originally accepted the terms.
    assert logs[1].terms_accepted_at == logs[0].terms_accepted_at


@requires_postgres
def test_given_visitor_id_is_kept_for_a_new_email(db):
    visitor_id = str(uuid.uuid4())
    result = PublicAccessService().get_open_download_urls(
        "EP0001DS0001", email="asha@example.org", visitor_id=visitor_id, accept_terms=True
    )
    assert result["visitor_id"] == visitor_id


@requires_postgres
@pytest.mark.parametrize(
    "kwargs",
    [
        {},  # nothing: first visit
        {"visitor_id": "not-a-uuid"},
        {"visitor_id": str(uuid.uuid4())},  # unknown cookie, e.g. records deleted
    ],
)
def test_download_without_email_or_known_visitor_needs_the_pop_up(db, kwargs):
    with pytest.raises(HTTPException) as error:
        PublicAccessService().get_open_download_urls("EP0001DS0001", **kwargs)
    assert error.value.status_code == 428
    session = db()
    assert session.query(DatasetDownload).count() == 0
    session.close()


@requires_postgres
def test_email_without_accepting_the_terms_is_refused(db):
    with pytest.raises(HTTPException) as error:
        PublicAccessService().get_open_download_urls("EP0001DS0001", email="asha@example.org")
    assert error.value.status_code == 400


@requires_postgres
@pytest.mark.parametrize("ds_id", ["EP0001DS0002", "EP0001DS0003"])
def test_view_only_and_restricted_never_get_download_links(db, ds_id):
    with pytest.raises(HTTPException) as error:
        PublicAccessService().get_open_download_urls(ds_id, email="asha@example.org", accept_terms=True)
    assert error.value.status_code == 403


@requires_postgres
def test_download_route_answers_403_for_view_only(db):
    client = TestClient(importlib.import_module("dataio.api").app)
    response = client.post(
        "/api/v1/web/public/datasets/EP0001DS0002/download-urls",
        json={"email": "asha@example.org", "accept_terms": True},
    )
    assert response.status_code == 403
    assert "download_url" not in response.text


@requires_postgres
def test_download_route_takes_the_email_in_the_body(db):
    client = TestClient(importlib.import_module("dataio.api").app)
    url = "/api/v1/web/public/datasets/EP0001DS0001/download-urls"
    assert client.post(url, json={}).status_code == 428
    assert client.post(url, json={"email": "not-an-email", "accept_terms": True}).status_code == 422
    response = client.post(url, json={"email": "asha@example.org", "accept_terms": True})
    assert response.status_code == 200
    assert response.json()["tables"][0]["download_url"].endswith("/main.csv")
    # The old GET form is gone, so emails never end up in URLs.
    assert client.get(url).status_code == 405


@requires_postgres
def test_unknown_dataset_is_404(db):
    with pytest.raises(HTTPException) as error:
        PublicAccessService().get_open_download_urls("NOPE")
    assert error.value.status_code == 404


# -------------------------------------------------------------- preview


@requires_postgres
def test_preview_modes_follow_access_level(db):
    service = PublicAccessService()
    real = service.get_preview("EP0001DS0001")
    assert real["mode"] == "real"
    assert real["rows"][0] == ["Bidar", "2023", "4"]

    synthetic = service.get_preview("EP0001DS0002")
    assert synthetic["mode"] == "synthetic"
    assert [row[:2] for row in synthetic["rows"]] == [["Bidar", "2023"], ["Kolar", "2023"]]

    restricted = service.get_preview("EP0001DS0003")
    assert restricted == {"mode": "none", "table": None, "columns": [], "rows": []}


# ------------------------------------------------------ access requests


@requires_postgres
def test_access_request_is_saved_and_emails_team_and_requester(db):
    result = PublicAccessService().submit_access_request(**_request())
    assert result["submitted"] is True

    session = db()
    row = session.query(AccessRequest).one()
    session.close()
    assert row.email == "asha@example.org"
    assert row.status == "PENDING"
    assert {mail["to"] for mail in FakeEmail.sent} == {access_module.ACCESS_REQUEST_NOTIFY_EMAIL, "asha@example.org"}


@requires_postgres
def test_requester_text_is_escaped_in_the_team_email(db):
    PublicAccessService().submit_access_request(**_request(purpose="<script>alert(1)</script>"))
    team = next(m for m in FakeEmail.sent if m["to"] == access_module.ACCESS_REQUEST_NOTIFY_EMAIL)
    assert "<script>" not in team["html"]


@requires_postgres
@pytest.mark.parametrize(
    "overrides, status",
    [
        ({"organisation_type": "Spaceship"}, 422),
        ({"organisation_type": "Other", "organisation_type_other": " "}, 422),
        ({"consent": False}, 422),
        ({"purpose": "word " * 251}, 422),
        ({"dataset_id": "EP0001DS0001"}, 400),
        ({"dataset_id": "NOPE"}, 404),
    ],
)
def test_access_request_validation(db, overrides, status):
    with pytest.raises(HTTPException) as error:
        PublicAccessService().submit_access_request(**_request(**overrides))
    assert error.value.status_code == status


@requires_postgres
def test_approving_a_known_user_grants_download(db):
    service = PublicAccessService()
    request_id = service.submit_access_request(**_request(email="known@example.org"))["id"]

    result = service.review_access_request(_admin(db), request_id, approve=True)
    assert result["status"] == "APPROVED"
    assert result["invited"] is False

    session = db()
    perm = session.query(UserPermission).filter(UserPermission.user_email == "known@example.org").one()
    session.close()
    assert perm.resource_type == ResourceType.DATASET
    assert perm.resource_id == "EP0001DS0002"
    assert perm.permission == AccessLevel.DOWNLOAD


@requires_postgres
def test_approving_someone_without_an_account_invites_them_first(db, monkeypatch):
    invited = []

    def fake_invite(self, admin_user, email, display_name=None, **kwargs):
        session = db()
        session.add(User(email=email, display_name=display_name))
        session.commit()
        session.close()
        invited.append(email)
        return {"invited": True, "email": email}

    monkeypatch.setattr(admin_module.WebAdminService, "__init__", lambda self: None)
    monkeypatch.setattr(admin_module.WebAdminService, "invite_user", fake_invite)
    service = PublicAccessService()
    request_id = service.submit_access_request(**_request(dataset_id="EP0001DS0003"))["id"]

    result = service.review_access_request(_admin(db), request_id, approve=True)
    assert invited == ["asha@example.org"]
    assert result["invited"] is True


@requires_postgres
def test_rejecting_sends_the_note_and_cannot_be_reviewed_twice(db):
    service = PublicAccessService()
    request_id = service.submit_access_request(**_request())["id"]
    FakeEmail.sent = []

    result = service.review_access_request(_admin(db), request_id, approve=False, note="Licence doesn't allow it.")
    assert result["status"] == "REJECTED"
    assert "Licence doesn't allow it." in FakeEmail.sent[0]["text"]

    with pytest.raises(HTTPException) as error:
        service.review_access_request(_admin(db), request_id, approve=True)
    assert error.value.status_code == 409


@requires_postgres
def test_only_admins_review(db):
    service = PublicAccessService()
    request_id = service.submit_access_request(**_request())["id"]
    with pytest.raises(HTTPException) as error:
        service.review_access_request(User(email="known@example.org", is_admin=False), request_id, approve=True)
    assert error.value.status_code == 403


@requires_postgres
def test_list_access_requests_filters_by_status(db):
    service = PublicAccessService()
    service.submit_access_request(**_request())
    listed = service.list_access_requests(status="PENDING")
    assert listed["total"] == 1
    assert listed["pending"] == 1
    assert listed["requests"][0]["dataset_title"] == "Dataset EP0001DS0002"
    assert service.list_access_requests(status="APPROVED")["total"] == 0
    with pytest.raises(HTTPException):
        service.list_access_requests(status="bogus")
    # an unknown id is a 404, not a crash
    with pytest.raises(HTTPException) as error:
        service.review_access_request(_admin(db), str(uuid.uuid4()), approve=True)
    assert error.value.status_code == 404
