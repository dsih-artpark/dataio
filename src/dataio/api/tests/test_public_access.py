"""Signed-out downloads, dataset previews and access requests (public site).

The synthetic-preview rules are plain functions and always run. The rest
uses a real PostgreSQL database (see postgres_schema.py); S3, email and the
invite flow are replaced by fakes.
"""

from __future__ import annotations

import importlib
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pytest
import yaml
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
    SAMPLE_TEXT,
    PublicAccessService,
    count_words,
    synthetic_rows,
)
from dataio.api.tests.postgres_schema import fresh_schema_engine, requires_postgres

# ------------------------------------------------------- synthetic preview

# A v2 data dictionary like a line list's: IDs, places, dates, codes and counts.
DICTIONARY = {
    "case_id": {"type": "string"},
    "district.ID": {"type": "regionID"},
    "village.name": {"type": "regionName"},
    "onset_date": {"type": "date", "format": "%Y-%m-%d"},
    "sex": {"type": "enum", "allowedValues": ["M", "F"]},
    "serotype": {"type": "enum", "enumRef": "serotypes"},
    "age": {"type": "int", "min": 0, "max": 99},
    "rate": {"type": "float", "range": [0, 1]},
    "notes": {"description": "free text, no type"},
}
ENUMS = {"serotypes": {"values": {"DENV-1": {}, "DENV-2": {}}}}


def test_synthetic_preview_follows_the_data_dictionary():
    rows = synthetic_rows(
        "EP0001DS0002", DICTIONARY, ENUMS, date(2023, 6, 1), date(2023, 6, 30), count=20
    )
    assert len(rows) == 20 and all(len(row) == len(DICTIONARY) for row in rows)
    for case_id, region_id, village, onset, sex, serotype, age, rate, notes in rows:
        assert case_id == SAMPLE_TEXT and notes == SAMPLE_TEXT
        assert region_id.startswith("district_")
        assert village == "Sample village"
        assert date(2023, 6, 1) <= date.fromisoformat(onset) <= date(2023, 6, 30)
        assert sex in ("M", "F") and serotype in ("DENV-1", "DENV-2")
        assert 0 <= int(age) <= 99 and 0 <= float(rate) <= 1


@pytest.mark.parametrize(
    "field",
    [
        {"type": "int", "min": "n/a", "max": "n/a"},
        {"type": "float", "range": ["low", "high"]},
        {"type": "float", "min": "inf", "max": "nan"},
        {"type": "int", "range": [None, None]},
        {"type": "int", "min": [1], "max": {}},
    ],
)
def test_synthetic_preview_ignores_numbers_it_cannot_read(field):
    rows = synthetic_rows("A", {"value": field}, {}, count=3)
    assert len(rows) == 3
    assert all(float(row[0]) == float(row[0]) for row in rows)


def test_synthetic_preview_is_stable_per_dataset():
    assert synthetic_rows("A", DICTIONARY, ENUMS) == synthetic_rows("A", DICTIONARY, ENUMS)
    assert synthetic_rows("A", DICTIONARY, ENUMS) != synthetic_rows("B", DICTIONARY, ENUMS)


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
    manifest = {"datasetTables": {"cases": {"dataDictionary": DICTIONARY}}, "enumDefinitions": ENUMS}
    for ds_id, level, extra in (
        ("EP0001DS0001", AccessLevel.DOWNLOAD, {}),
        # JSONB doesn't keep key order, so column order comes from the YAML, as in real imports.
        (
            "EP0001DS0002",
            AccessLevel.VIEW,
            {"manifest_json": manifest, "manifest_yaml": yaml.safe_dump(manifest, sort_keys=False)},
        ),
        ("EP0001DS0003", AccessLevel.NONE, {}),
        # View only with only an untyped v1 data dictionary
        (
            "EP0001DS0004",
            AccessLevel.VIEW,
            {"data_dictionary_json": json.dumps({"tables": {"main": {"data_dictionary": {"district": {}}}}})},
        ),
    ):
        session.add(
            Dataset(
                ds_id=ds_id,
                title=f"Dataset {ds_id}",
                collection_id=collection.id,
                data_owner_id=owner.id,
                spatial_resolution=SpatialResolution.DISTRICT,
                temporal_resolution=TemporalResolution.MONTH,
                access_level=level,
                **extra,
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
def test_no_files_is_said_before_asking_for_consent(db, monkeypatch):
    monkeypatch.setattr(FakeFilestore, "list_files_in_s3", lambda self, dataset_id, version_type: [])
    # Without an email: "no files" (404), not the pop-up (428), so nobody
    # gives consent for a download that can't happen.
    with pytest.raises(HTTPException) as error:
        PublicAccessService().get_open_download_urls("EP0001DS0001")
    assert error.value.status_code == 404


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


@pytest.fixture()
def downloads_on(monkeypatch):
    monkeypatch.setattr(importlib.import_module("dataio.api.routers.web"), "PUBLIC_DOWNLOADS_ENABLED", True)


def test_download_route_asks_to_sign_in_while_signed_out_downloads_are_off(monkeypatch):
    web_module = importlib.import_module("dataio.api.routers.web")
    monkeypatch.setattr(web_module, "PUBLIC_DOWNLOADS_ENABLED", False)

    def must_not_run(*args, **kwargs):
        raise AssertionError("nothing may be read or logged while downloads are off")

    monkeypatch.setattr(web_module, "get_client_info", must_not_run)
    client = TestClient(importlib.import_module("dataio.api").app)

    response = client.post(
        "/api/v1/web/public/datasets/EP0001DS0001/download-urls",
        json={"email": "asha@example.org", "accept_terms": True},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == web_module.SIGN_IN_TO_DOWNLOAD


@requires_postgres
def test_download_route_answers_403_for_view_only(db, downloads_on):
    client = TestClient(importlib.import_module("dataio.api").app)
    response = client.post(
        "/api/v1/web/public/datasets/EP0001DS0002/download-urls",
        json={"email": "asha@example.org", "accept_terms": True},
    )
    assert response.status_code == 403
    assert "download_url" not in response.text


@requires_postgres
def test_download_route_takes_the_email_in_the_body(db, downloads_on):
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

    restricted = service.get_preview("EP0001DS0003")
    assert restricted == {"mode": "none", "table": None, "columns": [], "rows": []}


@requires_postgres
def test_view_only_preview_never_reads_the_data_file(db, monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("a View only preview must not read the data file")

    monkeypatch.setattr(FakeFilestore, "read_table_head", fail)
    service = PublicAccessService()

    synthetic = service.get_preview("EP0001DS0002")
    assert synthetic["mode"] == "synthetic"
    assert synthetic["table"] == "cases"
    assert synthetic["columns"] == list(DICTIONARY)
    assert synthetic["rows"] == synthetic_rows("EP0001DS0002", DICTIONARY, ENUMS)

    # Without a typed data dictionary there is nothing to build it from.
    assert service.get_preview("EP0001DS0004")["mode"] == "unavailable"


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
def test_receipt_carries_no_text_from_the_form(db):
    # Anyone can type someone else's address, so the sender's words must not reach it.
    PublicAccessService().submit_access_request(**_request(full_name="Click evil.example now"))
    receipt = next(m for m in FakeEmail.sent if m["to"] == "asha@example.org")
    assert "evil" not in receipt["text"] and "evil" not in receipt["html"]
    assert receipt["text"].startswith("Hi,")


@requires_postgres
def test_filled_in_honeypot_is_dropped_quietly(db):
    result = PublicAccessService().submit_access_request(**_request(), honeypot="http://spam.example")
    assert result["submitted"] is True
    session = db()
    assert session.query(AccessRequest).count() == 0
    session.close()
    assert FakeEmail.sent == []


@requires_postgres
def test_repeat_of_a_pending_request_sends_nothing_more(db):
    service = PublicAccessService()
    first = service.submit_access_request(**_request())
    sent_after_first = len(FakeEmail.sent)
    again = service.submit_access_request(**_request(email="ASHA@example.org"))
    assert again["submitted"] is True and again["id"] != first["id"]
    assert len(FakeEmail.sent) == sent_after_first
    session = db()
    assert session.query(AccessRequest).count() == 1
    session.close()

    # Once it's been answered, a new request is welcome.
    service.review_access_request(_admin(db), first["id"], approve=False)
    service.submit_access_request(**_request())
    session = db()
    assert session.query(AccessRequest).count() == 2
    session.close()


@requires_postgres
def test_approval_finds_an_account_whatever_the_case_of_its_email(db, monkeypatch):
    session = db()
    session.add(User(email="Mixed.Case@Example.org", email_verified=True))
    session.commit()
    session.close()
    monkeypatch.setattr(admin_module.WebAdminService, "__init__", lambda self: None)
    monkeypatch.setattr(
        admin_module.WebAdminService,
        "invite_user",
        lambda *args, **kwargs: pytest.fail("an existing account must not be invited again"),
    )
    service = PublicAccessService()
    request_id = service.submit_access_request(**_request(email="mixed.case@example.org"))["id"]

    assert service.review_access_request(_admin(db), request_id, approve=True)["invited"] is False
    session = db()
    perm = session.query(UserPermission).one()
    session.close()
    # Granted to the account's own spelling, which its permission checks compare against.
    assert perm.user_email == "Mixed.Case@Example.org"


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
def test_two_admins_reviewing_at_once_decide_it_once(db, monkeypatch):
    service = PublicAccessService()
    request_id = service.submit_access_request(**_request(email="known@example.org"))["id"]
    FakeEmail.sent = []
    outcomes = []

    def review(approve):
        try:
            outcomes.append(service.review_access_request(_admin(db), request_id, approve=approve)["status"])
        except HTTPException as exc:
            outcomes.append(exc.status_code)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(review, [True, False]))
    assert outcomes.count(409) == 1
    assert {o for o in outcomes if o != 409} <= {"APPROVED", "REJECTED"}
    assert len(FakeEmail.sent) == 1  # one decision email, not two


@requires_postgres
def test_rejecting_does_not_need_the_admin_service(db, monkeypatch):
    def unavailable(self):
        raise RuntimeError("S3 settings missing")

    monkeypatch.setattr(admin_module.WebAdminService, "__init__", unavailable)
    service = PublicAccessService()
    request_id = service.submit_access_request(**_request())["id"]
    assert service.review_access_request(_admin(db), request_id, approve=False)["status"] == "REJECTED"
    # Approving someone who already has an account doesn't need it either.
    request_id = service.submit_access_request(**_request(email="known@example.org"))["id"]
    assert service.review_access_request(_admin(db), request_id, approve=True)["status"] == "APPROVED"


@requires_postgres
def test_negative_offset_is_a_400(db):
    with pytest.raises(HTTPException) as error:
        PublicAccessService().list_access_requests(offset=-1)
    assert error.value.status_code == 400


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
