"""
Comprehensive integration tests for permission scenarios.
Run after inserting test data with: python src/dataio/api/tests/insert_test_data.py --clean --all
"""

from dataio.api.tests.test_utils import (
    api_call,
    load_test_api_keys,
    TEST_USERS,
    TEST_DATASETS,
    client,
)


class TestUserDatasetAccess:
    """Test complete permission matrix from README."""

    def test_admin_access_all_datasets(self):
        """Test admin has DOWNLOAD access to all datasets."""
        api_keys = load_test_api_keys()
        result = api_call("/api/v1/datasets", api_keys[TEST_USERS["admin"]])
        assert result["status_code"] == 200

        datasets = result["data"]
        dataset_dict = {ds["ds_id"]: ds["access_level"] for ds in datasets}

        # Admin should have DOWNLOAD access to all 7 datasets
        expected_datasets = [
            "ds0001",
            "ds0002",
            "ds0003",
            "ds0004",
            "ds0005",
            "ds0006",
            "ds0007",
        ]
        assert len(datasets) == 7
        for ds_key in expected_datasets:
            ds_id = TEST_DATASETS[ds_key]
            assert ds_id in dataset_dict, f"Dataset {ds_id} not found in admin response"
            assert dataset_dict[ds_id] == "DOWNLOAD", (
                f"Dataset {ds_id} should have DOWNLOAD access"
            )

    def test_analyst_access_pattern(self):
        """Test analyst's complete access pattern."""
        api_keys = load_test_api_keys()
        result = api_call("/api/v1/datasets", api_keys[TEST_USERS["analyst"]])
        assert result["status_code"] == 200

        datasets = result["data"]
        dataset_dict = {ds["ds_id"]: ds["access_level"] for ds in datasets}
        dataset_ids = set(dataset_dict.keys())

        # Analyst should have DOWNLOAD access to DS0001 (default), DS0002-0004 (individual), DS0006-0007 (resource group)
        # Should NOT have access to DS0005 (admin-only)
        assert TEST_DATASETS["ds0001"] in dataset_ids
        assert dataset_dict[TEST_DATASETS["ds0001"]] == "DOWNLOAD"

        assert TEST_DATASETS["ds0002"] in dataset_ids
        assert dataset_dict[TEST_DATASETS["ds0002"]] == "DOWNLOAD"

        assert TEST_DATASETS["ds0003"] in dataset_ids
        assert dataset_dict[TEST_DATASETS["ds0003"]] == "DOWNLOAD"

        assert TEST_DATASETS["ds0004"] in dataset_ids
        assert dataset_dict[TEST_DATASETS["ds0004"]] == "DOWNLOAD"

        assert TEST_DATASETS["ds0005"] not in dataset_ids  # Admin-only

        assert TEST_DATASETS["ds0006"] in dataset_ids  # Resource group
        assert dataset_dict[TEST_DATASETS["ds0006"]] == "DOWNLOAD"

        assert TEST_DATASETS["ds0007"] in dataset_ids  # Resource group
        assert dataset_dict[TEST_DATASETS["ds0007"]] == "DOWNLOAD"

    def test_external_collaborator_access_pattern(self):
        """Test external collaborator's access pattern."""
        api_keys = load_test_api_keys()
        result = api_call("/api/v1/datasets", api_keys[TEST_USERS["external"]])
        assert result["status_code"] == 200

        datasets = result["data"]
        dataset_dict = {ds["ds_id"]: ds["access_level"] for ds in datasets}
        dataset_ids = set(dataset_dict.keys())

        # External should have DOWNLOAD access to DS0001 (default), DS0003-0004 (individual)
        # Should have VIEW access to DS0002 (default)
        # Should NOT have access to DS0005-0007
        assert TEST_DATASETS["ds0001"] in dataset_ids
        assert dataset_dict[TEST_DATASETS["ds0001"]] == "DOWNLOAD"

        assert TEST_DATASETS["ds0002"] in dataset_ids
        assert dataset_dict[TEST_DATASETS["ds0002"]] == "VIEW"

        assert TEST_DATASETS["ds0003"] in dataset_ids
        assert dataset_dict[TEST_DATASETS["ds0003"]] == "DOWNLOAD"

        assert TEST_DATASETS["ds0004"] in dataset_ids
        assert dataset_dict[TEST_DATASETS["ds0004"]] == "DOWNLOAD"

        assert TEST_DATASETS["ds0005"] not in dataset_ids
        assert TEST_DATASETS["ds0006"] not in dataset_ids
        assert TEST_DATASETS["ds0007"] not in dataset_ids

    def test_public_user_access_pattern(self):
        """Test public user's access pattern."""
        api_keys = load_test_api_keys()
        result = api_call("/api/v1/datasets", api_keys[TEST_USERS["public"]])
        assert result["status_code"] == 200

        datasets = result["data"]
        dataset_dict = {ds["ds_id"]: ds["access_level"] for ds in datasets}
        dataset_ids = set(dataset_dict.keys())

        # Public should have DOWNLOAD access to DS0001 (default)
        # Should have VIEW access to DS0002-0003 (default)
        # Should NOT have access to DS0004-0007
        assert TEST_DATASETS["ds0001"] in dataset_ids
        assert dataset_dict[TEST_DATASETS["ds0001"]] == "DOWNLOAD"

        assert TEST_DATASETS["ds0002"] in dataset_ids
        assert dataset_dict[TEST_DATASETS["ds0002"]] == "VIEW"

        assert TEST_DATASETS["ds0003"] in dataset_ids
        assert dataset_dict[TEST_DATASETS["ds0003"]] == "VIEW"

        assert TEST_DATASETS["ds0004"] not in dataset_ids
        assert TEST_DATASETS["ds0005"] not in dataset_ids
        assert TEST_DATASETS["ds0006"] not in dataset_ids
        assert TEST_DATASETS["ds0007"] not in dataset_ids


class TestAuthenticationEdgeCases:
    """Test auth failures and admin endpoints."""

    def test_authentication_failures(self):
        """Test authentication failure scenarios."""
        # Invalid API key should return 401
        result = api_call("/api/v1/datasets", "invalid_key")
        assert result["status_code"] == 401

        # Missing API key should return 401
        response = client.get("/api/v1/datasets")
        assert response.status_code == 401

    def test_admin_endpoints_require_admin(self):
        """Test admin endpoints require admin privileges."""
        api_keys = load_test_api_keys()

        # Admin should have access
        result = api_call("/api/v1/admin/users", api_keys[TEST_USERS["admin"]])
        assert result["status_code"] == 200

        # Non-admin should not have access
        result = api_call("/api/v1/admin/users", api_keys[TEST_USERS["analyst"]])
        assert result["status_code"] == 403
