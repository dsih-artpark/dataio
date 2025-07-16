import dotenv
import os
import pytest

from dataio.api import app
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


# Admin access tests
def test_admin_can_get_collections():
    """Test that admin can get collections and receives non-empty list"""
    response = client.get(
        "/api/v1/admin/collections", headers={"X-API-Key": TEST_ADMIN_KEY}
    )
    assert response.status_code == 200
    collections = response.json()
    assert isinstance(collections, list)
    assert len(collections) > 0, "Admin should get non-empty collections list"


def test_admin_can_get_users():
    """Test that admin can get users and receives non-empty list"""
    response = client.get("/api/v1/admin/users", headers={"X-API-Key": TEST_ADMIN_KEY})
    assert response.status_code == 200
    users = response.json()
    assert isinstance(users, list)
    assert len(users) > 0, "Admin should get non-empty users list"


def test_admin_can_get_data_owners():
    """Test that admin can get data owners and receives non-empty list"""
    response = client.get(
        "/api/v1/admin/data-owners", headers={"X-API-Key": TEST_ADMIN_KEY}
    )
    assert response.status_code == 200
    data_owners = response.json()
    assert isinstance(data_owners, list)
    assert len(data_owners) > 0, "Admin should get non-empty data owners list"


# User dataset table access tests
def test_public_user_can_get_dataset_table_with_download_permission():
    """Test that public user can get dataset table for dataset they have DOWNLOAD access to"""
    # Public user has DOWNLOAD access to TS0001DS0001
    response = client.get(
        "/api/v1/datasets/TS0001DS0001/STANDARDISED/tables",
        headers={"X-API-Key": TEST_PUBLIC_KEY},
    )
    assert response.status_code == 200, (
        "Public user should be able to access dataset table with DOWNLOAD permission"
    )


def test_public_user_cannot_get_dataset_table_with_view_permission():
    """Test that public user cannot get dataset table for dataset they have VIEW access to"""
    # Public user has VIEW access to TS0001DS0002
    response = client.get(
        "/api/v1/datasets/TS0001DS0002/STANDARDISED/tables",
        headers={"X-API-Key": TEST_PUBLIC_KEY},
    )
    assert response.status_code == 403, (
        "Public user should not be able to access dataset table with VIEW permission"
    )


def test_public_user_cannot_get_dataset_table_with_none_permission():
    """Test that public user cannot get dataset table for dataset they have NONE access to"""
    # Public user has NONE access to TS0001DS0004
    response = client.get(
        "/api/v1/datasets/TS0001DS0004/STANDARDISED/tables",
        headers={"X-API-Key": TEST_PUBLIC_KEY},
    )
    assert response.status_code == 403, (
        "Public user should not be able to access dataset table with NONE permission"
    )


def test_analyst_user_can_get_dataset_table_with_download_permission():
    """Test that analyst user can get dataset table for dataset they have DOWNLOAD access to"""
    # Analyst user has DOWNLOAD access to TS0001DS0001
    response = client.get(
        "/api/v1/datasets/TS0001DS0001/STANDARDISED/tables",
        headers={"X-API-Key": TEST_ANALYST_KEY},
    )
    assert response.status_code == 200, (
        "Analyst user should be able to access dataset table with DOWNLOAD permission"
    )


def test_analyst_user_cannot_get_dataset_table_with_none_permission():
    """Test that analyst user cannot get dataset table for dataset they have NONE access to"""
    # Analyst user has NONE access to TS0001DS0005
    response = client.get(
        "/api/v1/datasets/TS0001DS0005/STANDARDISED/tables",
        headers={"X-API-Key": TEST_ANALYST_KEY},
    )
    assert response.status_code == 403, (
        "Analyst user should not be able to access dataset table with NONE permission"
    )


def test_ext_collaborator_can_get_dataset_table_with_download_permission():
    """Test that ext collaborator can get dataset table for dataset they have DOWNLOAD access to"""
    # Ext collaborator has DOWNLOAD access to TS0001DS0001
    response = client.get(
        "/api/v1/datasets/TS0001DS0001/STANDARDISED/tables",
        headers={"X-API-Key": TEST_EXT_COLLABORATOR_KEY},
    )
    assert response.status_code == 200, (
        "Ext collaborator should be able to access dataset table with DOWNLOAD permission"
    )


def test_ext_collaborator_cannot_get_dataset_table_with_view_permission():
    """Test that ext collaborator cannot get dataset table for dataset they have VIEW access to"""
    # Ext collaborator has VIEW access to TS0001DS0002
    response = client.get(
        "/api/v1/datasets/TS0001DS0002/STANDARDISED/tables",
        headers={"X-API-Key": TEST_EXT_COLLABORATOR_KEY},
    )
    assert response.status_code == 403, (
        "Ext collaborator should not be able to access dataset table with VIEW permission"
    )


def test_ext_collaborator_cannot_get_dataset_table_with_none_permission():
    """Test that ext collaborator cannot get dataset table for dataset they have NONE access to"""
    # Ext collaborator has NONE access to TS0001DS0005
    response = client.get(
        "/api/v1/datasets/TS0001DS0005/STANDARDISED/tables",
        headers={"X-API-Key": TEST_EXT_COLLABORATOR_KEY},
    )
    assert response.status_code == 403, (
        "Ext collaborator should not be able to access dataset table with NONE permission"
    )
