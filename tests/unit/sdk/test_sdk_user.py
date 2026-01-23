"""
Unit tests for the DataIO SDK user module.

Tests SDK client initialization, API calls, and data handling.
"""

import os
import pytest
import warnings
from unittest.mock import patch, MagicMock
from pathlib import Path


class TestDataIOAPIInitialization:
    """Tests for DataIOAPI client initialization."""

    def test_init_with_explicit_values(self, mock_requests_session):
        """Test initialization with explicit base_url and api_key."""
        from dataio.sdk.user import DataIOAPI

        api = DataIOAPI(
            base_url="https://test-api.example.com/api/v1",
            api_key="dio_test_key_12345",
        )

        assert api.base_url == "https://test-api.example.com/api/v1"
        assert "X-API-Key" in api.session.headers

    def test_init_without_base_url_raises(self, mock_requests_session):
        """Test that missing base_url raises ValueError."""
        from dataio.sdk.user import DataIOAPI

        # Clear environment variables
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                DataIOAPI(api_key="test_key")
            assert "DATAIO_API_BASE_URL" in str(exc_info.value)

    def test_init_without_api_key_raises(self, mock_requests_session):
        """Test that missing api_key raises ValueError."""
        from dataio.sdk.user import DataIOAPI

        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                DataIOAPI(base_url="https://test.example.com")
            assert "DATAIO_API_KEY" in str(exc_info.value)

    def test_init_from_environment(self, mock_requests_session, sdk_env_vars):
        """Test initialization from environment variables."""
        from dataio.sdk.user import DataIOAPI

        api = DataIOAPI()

        assert api.base_url == "https://test-api.example.com/api/v1"
        assert api.data_dir == "/tmp/dataio_test"

    def test_legacy_api_key_warning(self, mock_requests_session):
        """Test that legacy API keys trigger deprecation warning."""
        from dataio.sdk.user import DataIOAPI, LegacyAPIKeyWarning

        # Reset the class-level warning flag
        DataIOAPI._legacy_key_warning_shown = False

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            api = DataIOAPI(
                base_url="https://test.example.com",
                api_key="legacy_key_without_prefix",  # No dio_ prefix
            )
            # Check that a warning was issued
            assert len(w) >= 1
            assert any(issubclass(warning.category, LegacyAPIKeyWarning) for warning in w)

    def test_modern_api_key_no_warning(self, mock_requests_session):
        """Test that modern API keys don't trigger warning."""
        from dataio.sdk.user import DataIOAPI

        # Reset the class-level warning flag
        DataIOAPI._legacy_key_warning_shown = False

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            api = DataIOAPI(
                base_url="https://test.example.com",
                api_key="dio_modern_key_12345",  # Has dio_ prefix
            )
            # Filter for our specific warning
            from dataio.sdk.user import LegacyAPIKeyWarning
            legacy_warnings = [warning for warning in w if issubclass(warning.category, LegacyAPIKeyWarning)]
            assert len(legacy_warnings) == 0

    def test_default_data_dir(self, mock_requests_session):
        """Test default data directory."""
        from dataio.sdk.user import DataIOAPI

        with patch.dict(os.environ, {"DATAIO_DATA_DIR": ""}, clear=False):
            api = DataIOAPI(
                base_url="https://test.example.com",
                api_key="dio_test_key",
            )
            assert api.data_dir == "data"


class TestDataIOAPIRequests:
    """Tests for DataIOAPI request methods."""

    def test_request_method(self, mock_requests_session):
        """Test the _request method."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True  # Suppress warning
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        mock_requests_session.request.return_value.json.return_value = {"data": "test"}

        result = api._request("GET", "/datasets")

        mock_requests_session.request.assert_called_once_with(
            "GET", "https://test.example.com/api/v1/datasets"
        )
        assert result == {"data": "test"}

    def test_request_with_kwargs(self, mock_requests_session):
        """Test _request with additional kwargs."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        api._request("POST", "/weather/download", json={"variables": ["t2m"]})

        mock_requests_session.request.assert_called_once()
        call_kwargs = mock_requests_session.request.call_args
        assert call_kwargs[1]["json"] == {"variables": ["t2m"]}


class TestListDatasets:
    """Tests for list_datasets method."""

    def test_list_datasets_default(self, mock_requests_session):
        """Test listing datasets with default parameters."""
        from dataio.sdk.user import DataIOAPI, DatasetList

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        mock_response = [
            {"ds_id": "DS0001", "title": "Dataset 1"},
            {"ds_id": "DS0002", "title": "Dataset 2"},
        ]
        mock_requests_session.request.return_value.json.return_value = mock_response

        result = api.list_datasets()

        assert isinstance(result, DatasetList)
        assert len(result) == 2
        mock_requests_session.request.assert_called_with(
            "GET", "https://test.example.com/api/v1/datasets"
        )

    def test_list_datasets_with_limit(self, mock_requests_session):
        """Test listing datasets with custom limit."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        mock_requests_session.request.return_value.json.return_value = []

        api.list_datasets(limit=50)

        mock_requests_session.request.assert_called_with(
            "GET", "https://test.example.com/api/v1/datasets?limit=50"
        )


class TestGetDatasetDetails:
    """Tests for get_dataset_details method."""

    def test_get_dataset_details_by_string_id(self, mock_requests_session):
        """Test getting dataset details by string ID."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        mock_datasets = [
            {"ds_id": "GS0001DS0001", "title": "Dataset 1"},
            {"ds_id": "GS0001DS0002", "title": "Dataset 2"},
        ]
        mock_requests_session.request.return_value.json.return_value = mock_datasets

        result = api.get_dataset_details("0001")

        assert result["ds_id"] == "GS0001DS0001"

    def test_get_dataset_details_by_int_id(self, mock_requests_session):
        """Test getting dataset details by integer ID."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        mock_datasets = [
            {"ds_id": "GS0001DS0001", "title": "Dataset 1"},
        ]
        mock_requests_session.request.return_value.json.return_value = mock_datasets

        result = api.get_dataset_details(1)

        assert result["ds_id"] == "GS0001DS0001"

    def test_get_dataset_details_not_found(self, mock_requests_session):
        """Test that non-existent dataset raises ValueError."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        mock_requests_session.request.return_value.json.return_value = []

        with pytest.raises(ValueError) as exc_info:
            api.get_dataset_details("9999")
        assert "not found" in str(exc_info.value).lower()


class TestListDatasetTables:
    """Tests for list_dataset_tables method."""

    def test_list_dataset_tables_default_bucket(self, mock_requests_session):
        """Test listing tables with default bucket type."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        mock_tables = [{"table_name": "main", "download_link": "http://..."}]
        mock_requests_session.request.return_value.json.return_value = mock_tables

        result = api.list_dataset_tables("DS0001")

        assert len(result) == 1
        mock_requests_session.request.assert_called_with(
            "GET", "https://test.example.com/api/v1/datasets/DS0001/STANDARDISED/tables"
        )

    def test_list_dataset_tables_preprocessed(self, mock_requests_session):
        """Test listing tables with PREPROCESSED bucket."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        mock_requests_session.request.return_value.json.return_value = []

        api.list_dataset_tables("DS0001", bucket_type="preprocessed")

        mock_requests_session.request.assert_called_with(
            "GET", "https://test.example.com/api/v1/datasets/DS0001/PREPROCESSED/tables"
        )


class TestDatasetList:
    """Tests for DatasetList class."""

    def test_dataset_list_str_representation(self):
        """Test DatasetList string representation."""
        from dataio.sdk.user import DatasetList

        datasets = DatasetList([
            {"ds_id": "DS0001", "title": "Test Dataset"},
        ])

        str_repr = str(datasets)
        assert "DS0001" in str_repr
        assert "Test Dataset" in str_repr


class TestRegionMethods:
    """Tests for region-related methods."""

    def test_get_children_regions(self, mock_requests_session):
        """Test getting children regions."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        mock_regions = [{"region_id": "REG001", "region_name": "Child Region"}]
        mock_requests_session.request.return_value.json.return_value = mock_regions

        result = api.get_children_regions("REG000")

        assert len(result) == 1
        mock_requests_session.request.assert_called_with(
            "GET", "https://test.example.com/api/v1/regions/REG000/children"
        )

    def test_get_shapefile_list(self, mock_requests_session):
        """Test getting shapefile list."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        mock_shapefiles = [{"region_id": "REG001", "name": "Region 1"}]
        mock_requests_session.request.return_value.json.return_value = mock_shapefiles

        result = api.get_shapefile_list()

        assert len(result) == 1


class TestWeatherMethods:
    """Tests for weather-related methods."""

    def test_list_weather_datasets(self, mock_requests_session):
        """Test listing weather datasets."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        mock_datasets = [{"dataset_name": "era5_sfc", "variables": ["t2m"]}]
        mock_requests_session.request.return_value.json.return_value = mock_datasets

        result = api.list_weather_datasets()

        assert len(result) == 1
        mock_requests_session.request.assert_called_with(
            "GET", "https://test.example.com/api/v1/weather/datasets"
        )


class TestLoadGeojson:
    """Tests for _load_geojson helper method."""

    def test_load_geojson_from_dict(self, mock_requests_session):
        """Test loading GeoJSON from dictionary."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        geojson = {"type": "Feature", "geometry": {"type": "Point"}}
        result = api._load_geojson(geojson)

        assert result == geojson

    def test_load_geojson_from_file(self, mock_requests_session, tmp_path):
        """Test loading GeoJSON from file."""
        from dataio.sdk.user import DataIOAPI
        import json

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        geojson = {"type": "Feature", "geometry": {"type": "Point"}}
        geojson_file = tmp_path / "test.geojson"
        geojson_file.write_text(json.dumps(geojson))

        result = api._load_geojson(str(geojson_file))

        assert result == geojson

    def test_load_geojson_invalid_type(self, mock_requests_session):
        """Test that invalid type raises ValueError."""
        from dataio.sdk.user import DataIOAPI

        DataIOAPI._legacy_key_warning_shown = True
        api = DataIOAPI(
            base_url="https://test.example.com/api/v1",
            api_key="dio_test_key",
        )

        with pytest.raises(ValueError):
            api._load_geojson(12345)  # Invalid type
