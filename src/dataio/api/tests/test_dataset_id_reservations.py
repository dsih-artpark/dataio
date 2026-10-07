"""Reserving dataset IDs and suggesting the next ID: format and collection
checks, and conflicts reported as 409 rather than a server error."""

from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from dataio.api.database.functions import ReservedIdConflict
from dataio.api.routers.web import ReserveDatasetIdRequest
from dataio.api.services.admin_dataset_service import AdminDatasetService
from dataio.api.services.web_admin_service import WebAdminService

ADMIN_USER = SimpleNamespace(email="admin@example.com", is_admin=True, is_group=False)
DB = "dataio.api.services.web_admin_service.database"
KNOWN_COLLECTION = SimpleNamespace(collection_id="CS0007", category_id="CS")


@pytest.fixture
def service(monkeypatch):
    monkeypatch.setattr(
        f"{DB}.get_collection_by_identifier",
        lambda collection_id: KNOWN_COLLECTION if collection_id == "CS0007" else None,
    )
    monkeypatch.setattr(WebAdminService, "_require_admin", lambda self, user: None)
    return WebAdminService()


@pytest.mark.parametrize("ds_id", ["CS0007DS113", "cs0007ds0113", "CS0007DS0113 ", "CS7DS0113", "CSRDS16"])
def test_reserve_request_rejects_malformed_dataset_ids(ds_id):
    with pytest.raises(ValidationError):
        ReserveDatasetIdRequest(ds_id=ds_id)


def test_reserve_request_accepts_a_well_formed_dataset_id():
    assert ReserveDatasetIdRequest(ds_id="CS0007DS0113").ds_id == "CS0007DS0113"


def test_reserve_dataset_id_rejects_an_id_from_another_collection(service, monkeypatch):
    monkeypatch.setattr(f"{DB}.create_reserved_dataset_id", _fail_if_reserved)

    with pytest.raises(HTTPException) as exc_info:
        service.reserve_dataset_id(ADMIN_USER, "CS0007DS0113", collection_id="CS0026")

    assert exc_info.value.status_code == 400


def test_reserve_dataset_id_rejects_an_unknown_collection(service, monkeypatch):
    monkeypatch.setattr(f"{DB}.create_reserved_dataset_id", _fail_if_reserved)

    with pytest.raises(HTTPException) as exc_info:
        service.reserve_dataset_id(ADMIN_USER, "XX0001DS0113")

    assert exc_info.value.status_code == 404


def test_reserve_dataset_id_reports_a_taken_number_as_409(service, monkeypatch):
    def taken(*args):
        raise ReservedIdConflict("Dataset number 0113 is already used by CS0026DS0113.")

    monkeypatch.setattr(f"{DB}.create_reserved_dataset_id", taken)

    with pytest.raises(HTTPException) as exc_info:
        service.reserve_dataset_id(ADMIN_USER, "CS0007DS0113")

    assert exc_info.value.status_code == 409
    assert "CS0026DS0113" in exc_info.value.detail


def test_reserve_dataset_id_reserves_a_valid_id(service, monkeypatch):
    reserved = []

    def reserve(ds_id, collection_id, note, reserved_by):
        reserved.append((ds_id, collection_id, reserved_by))
        return SimpleNamespace(ds_id=ds_id, collection_id=collection_id, note=note, reserved_by=reserved_by, created_at=None)

    monkeypatch.setattr(f"{DB}.create_reserved_dataset_id", reserve)

    result = service.reserve_dataset_id(ADMIN_USER, "CS0007DS0113", collection_id="CS0007")

    assert result["ds_id"] == "CS0007DS0113"
    assert reserved == [("CS0007DS0113", "CS0007", ADMIN_USER.email)]


def _fail_if_reserved(*args):
    raise AssertionError("must not reserve")


ADMIN_DB = "dataio.api.services.admin_dataset_service.database"


def test_suggest_next_dataset_id_returns_404_for_an_unknown_collection(monkeypatch):
    monkeypatch.setattr(f"{ADMIN_DB}.get_collection_by_identifier", lambda collection_id: None)

    with pytest.raises(HTTPException) as exc_info:
        AdminDatasetService().suggest_next_dataset_id("XX0001")

    assert exc_info.value.status_code == 404


def test_suggest_next_dataset_id_reports_an_exhausted_counter_as_409(monkeypatch):
    monkeypatch.setattr(f"{ADMIN_DB}.get_collection_by_identifier", lambda collection_id: KNOWN_COLLECTION)

    def exhausted(collection_id):
        raise ValueError("The dataset number counter is past 9999; dataset IDs only have four digits.")

    monkeypatch.setattr(f"{ADMIN_DB}.suggest_next_dataset_id", exhausted)

    with pytest.raises(HTTPException) as exc_info:
        AdminDatasetService().suggest_next_dataset_id("CS0007")

    assert exc_info.value.status_code == 409
    assert "9999" in exc_info.value.detail


def test_suggest_next_raw_dataset_id_returns_404_for_an_unknown_collection(monkeypatch):
    monkeypatch.setattr(f"{ADMIN_DB}.get_collection_by_identifier", lambda collection_id: None)

    with pytest.raises(HTTPException) as exc_info:
        AdminDatasetService().suggest_next_raw_dataset_id("XX0001")

    assert exc_info.value.status_code == 404


def test_suggest_raw_dataset_id_for_category_returns_404_for_an_unknown_category(monkeypatch):
    monkeypatch.setattr(f"{ADMIN_DB}.category_exists", lambda category_id: False)

    with pytest.raises(HTTPException) as exc_info:
        AdminDatasetService().suggest_next_raw_dataset_id_for_category("XX")

    assert exc_info.value.status_code == 404
