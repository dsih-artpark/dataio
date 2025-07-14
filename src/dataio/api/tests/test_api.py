import dotenv
import os
import pytest

from dataio.api.api import app
from fastapi.testclient import TestClient

dotenv.load_dotenv()
TEST_ADMIN_KEY = os.getenv("TEST_ADMIN_KEY")
TEST_ANALYST_KEY = os.getenv("TEST_ANALYST_KEY")
TEST_PUBLIC_KEY = os.getenv("TEST_PUBLIC_KEY")
TEST_EXT_COLLABORATOR_KEY = os.getenv("TEST_EXT_COLLABORATOR_KEY")

client = TestClient(app)


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to Dataset Management System API"}


def test_get_datasets_for_admin():
    response = client.get("/api/v1/datasets", headers={"X-API-Key": TEST_ADMIN_KEY})
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    pytest.admin_datasets_object = response.json()


def test_get_datasets_for_public():
    response = client.get("/api/v1/datasets", headers={"X-API-Key": TEST_PUBLIC_KEY})
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    pytest.public_datasets_object = response.json()


def test_get_datasets_for_analyst():
    response = client.get("/api/v1/datasets", headers={"X-API-Key": TEST_ANALYST_KEY})
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    pytest.analyst_datasets_object = response.json()


def test_get_datasets_for_ext_collaborator():
    response = client.get(
        "/api/v1/datasets", headers={"X-API-Key": TEST_EXT_COLLABORATOR_KEY}
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    pytest.ext_collaborator_datasets_object = response.json()


def test_all_datasets_are_downloadable_for_admin():
    assert all(
        [
            dataset["access_level"] == "DOWNLOAD"
            for dataset in pytest.admin_datasets_object
        ]
    )


def test_public_datasets_permissions():
    public_dl_dataset_id = ["TS0001DS0001"]
    public_view_ds_ids = ["TS0001DS0002", "TS0001DS0003"]
    public_none_ds_ids = ["TS0001DS0004", "TS0001DS0005"]

    assert all(
        [
            dataset["access_level"] == "DOWNLOAD"
            for dataset in pytest.public_datasets_object
            if dataset["ds_id"] in public_dl_dataset_id
        ]
    )

    assert all(
        [
            dataset["access_level"] == "VIEW"
            for dataset in pytest.public_datasets_object
            if dataset["ds_id"] in public_view_ds_ids
        ]
    )

    assert (
        len(
            [
                dataset
                for dataset in pytest.public_datasets_object
                if dataset["ds_id"] in public_none_ds_ids
            ]
        )
        == 0
    )


def test_analyst_datasets_permissions():
    analyst_dl_datasets = [
        "TS0001DS0001",
        "TS0001DS0002",
        "TS0001DS0003",
        "TS0001DS0004",
    ]
    analyst_none_datasets = ["TS0001DS0005"]

    assert all(
        [
            dataset["access_level"] == "DOWNLOAD"
            for dataset in pytest.analyst_datasets_object
            if dataset["ds_id"] in analyst_dl_datasets
        ]
    )

    assert (
        len(
            [
                dataset
                for dataset in pytest.analyst_datasets_object
                if dataset["ds_id"] in analyst_none_datasets
            ]
        )
        == 0
    )


def test_ext_collaborator_datasets_permissions():
    ext_collaborator_dl_datasets = [
        "TS0001DS0001",
        "TS0001DS0003",
        "TS0001DS0004",
    ]
    ext_collaborator_view_datasets = ["TS0001DS0002"]
    ext_collaborator_none_datasets = [
        "TS0001DS0005",
    ]

    assert all(
        [
            dataset["access_level"] == "DOWNLOAD"
            for dataset in pytest.ext_collaborator_datasets_object
            if dataset["ds_id"] in ext_collaborator_dl_datasets
        ]
    )

    assert all(
        [
            dataset["access_level"] == "VIEW"
            for dataset in pytest.ext_collaborator_datasets_object
            if dataset["ds_id"] in ext_collaborator_view_datasets
        ]
    )

    assert (
        len(
            [
                dataset
                for dataset in pytest.ext_collaborator_datasets_object
                if dataset["ds_id"] in ext_collaborator_none_datasets
            ]
        )
        == 0
    )


def test_ts0001ds0005_not_returned_for_all_users_except_admin():
    all_non_admin_datasets = (
        pytest.public_datasets_object
        + pytest.analyst_datasets_object
        + pytest.ext_collaborator_datasets_object
    )
    assert (
        len(
            [
                dataset
                for dataset in all_non_admin_datasets
                if dataset["ds_id"] == "TS0001DS0005"
            ]
        )
        == 0
    )

    assert all(
        [
            dataset["access_level"] == "DOWNLOAD"
            for dataset in pytest.admin_datasets_object
            if dataset["ds_id"] == "TS0001DS0005"
        ]
    )
