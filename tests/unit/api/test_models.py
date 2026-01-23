"""
Unit tests for API Pydantic models.

Tests model validation, serialization, and field constraints.
"""

import pytest
from pydantic import ValidationError


class TestRawDatasetCreate:
    """Tests for RawDatasetCreate model."""

    def test_valid_raw_dataset(self):
        """Test creating a valid raw dataset."""
        from dataio.api.models import RawDatasetCreate

        data = {
            "rds_id": "RDS001",
            "title": "Raw Dataset Title",
            "source": "External Source",
        }
        model = RawDatasetCreate(**data)
        assert model.rds_id == "RDS001"
        assert model.title == "Raw Dataset Title"
        assert model.source == "External Source"

    def test_empty_rds_id_fails(self):
        """Test that empty rds_id raises validation error."""
        from dataio.api.models import RawDatasetCreate

        with pytest.raises(ValidationError) as exc_info:
            RawDatasetCreate(rds_id="", title="Title", source="Source")
        assert "min_length" in str(exc_info.value).lower() or "at least 1" in str(exc_info.value).lower()

    def test_missing_required_fields(self):
        """Test that missing required fields raise validation error."""
        from dataio.api.models import RawDatasetCreate

        with pytest.raises(ValidationError):
            RawDatasetCreate(rds_id="RDS001")


class TestDatasetCreate:
    """Tests for DatasetCreate model."""

    def test_valid_dataset_minimal(self):
        """Test creating a dataset with minimal required fields."""
        from dataio.api.models import DatasetCreate

        data = {
            "ds_id": "GS0001DS0001",
            "title": "Test Dataset",
            "collection_id": "GS0001",
            "data_owner_name": "Test Owner",
            "raw_dataset_ids": ["RDS001"],
        }
        model = DatasetCreate(**data)
        assert model.ds_id == "GS0001DS0001"
        assert model.title == "Test Dataset"
        assert model.description is None
        assert model.tags is None

    def test_valid_dataset_full(self):
        """Test creating a dataset with all optional fields."""
        from dataio.api.models import DatasetCreate
        from dataio.api.database.enums import AccessLevel

        data = {
            "ds_id": "GS0001DS0001",
            "title": "Test Dataset",
            "collection_id": "GS0001",
            "data_owner_name": "Test Owner",
            "description": "A detailed description",
            "spatial_coverage_region_id": "REG001",
            "spatial_resolution": "1km",
            "temporal_coverage_start_date": "2020-01-01",
            "temporal_coverage_end_date": "2023-12-31",
            "temporal_resolution": "daily",
            "access_level": AccessLevel.VIEW,
            "additional_metadata": {"key": "value"},
            "tags": ["tag1", "tag2"],
            "raw_dataset_ids": ["RDS001", "RDS002"],
        }
        model = DatasetCreate(**data)
        assert model.description == "A detailed description"
        assert model.tags == ["tag1", "tag2"]
        assert model.access_level == AccessLevel.VIEW

    def test_ds_id_max_length(self):
        """Test that ds_id respects max length constraint."""
        from dataio.api.models import DatasetCreate

        with pytest.raises(ValidationError) as exc_info:
            DatasetCreate(
                ds_id="A" * 51,  # Exceeds max_length=50
                title="Test",
                collection_id="GS0001",
                data_owner_name="Owner",
                raw_dataset_ids=["RDS001"],
            )
        assert "max_length" in str(exc_info.value).lower() or "50" in str(exc_info.value)

    def test_empty_raw_dataset_ids_fails(self):
        """Test that empty raw_dataset_ids list fails validation."""
        from dataio.api.models import DatasetCreate

        with pytest.raises(ValidationError):
            DatasetCreate(
                ds_id="GS0001DS0001",
                title="Test",
                collection_id="GS0001",
                data_owner_name="Owner",
                raw_dataset_ids=[],  # min_length=1
            )


class TestUser:
    """Tests for User model."""

    def test_valid_user(self):
        """Test creating a valid user."""
        from dataio.api.models import User

        user = User(email="user@example.com", is_group=False)
        assert user.email == "user@example.com"
        assert user.is_group is False

    def test_valid_group(self):
        """Test creating a valid group."""
        from dataio.api.models import User

        group = User(email="group@example.com", is_group=True)
        assert group.is_group is True


class TestUserCreate:
    """Tests for UserCreate model."""

    def test_valid_user_create(self):
        """Test creating a valid user create request."""
        from dataio.api.models import UserCreate

        user = UserCreate(email="newuser@example.com", is_group=False)
        assert user.email == "newuser@example.com"


class TestUserGroupCreate:
    """Tests for UserGroupCreate model."""

    def test_valid_user_group_create(self):
        """Test creating a valid user group membership."""
        from dataio.api.models import UserGroupCreate

        membership = UserGroupCreate(
            group_email="group@example.com",
            user_email="user@example.com"
        )
        assert membership.group_email == "group@example.com"
        assert membership.user_email == "user@example.com"


class TestUserPermissionCreate:
    """Tests for UserPermissionCreate model."""

    def test_valid_permission_create(self):
        """Test creating a valid permission."""
        from dataio.api.models import UserPermissionCreate
        from dataio.api.database.enums import AccessLevel, ResourceType

        permission = UserPermissionCreate(
            user_email="user@example.com",
            resource_type=ResourceType.DATASET,
            resource_id="GS0001DS0001",
            permission=AccessLevel.DOWNLOAD,
        )
        assert permission.user_email == "user@example.com"
        assert permission.permission == AccessLevel.DOWNLOAD


class TestDataOwnerModels:
    """Tests for DataOwner models."""

    def test_data_owner_create(self):
        """Test creating a data owner."""
        from dataio.api.models import DataOwnerCreate

        owner = DataOwnerCreate(
            name="ARTPARK",
            contact_person="John Doe",
            contact_person_email="john@artpark.in",
        )
        assert owner.name == "ARTPARK"
        assert owner.contact_person == "John Doe"

    def test_data_owner_create_minimal(self):
        """Test creating a data owner with only required fields."""
        from dataio.api.models import DataOwnerCreate

        owner = DataOwnerCreate(name="ARTPARK")
        assert owner.name == "ARTPARK"
        assert owner.contact_person is None

    def test_data_owner_update(self):
        """Test data owner update model."""
        from dataio.api.models import DataOwnerUpdate

        update = DataOwnerUpdate(name="Updated Name")
        assert update.name == "Updated Name"
        assert update.contact_person is None


class TestCollectionModels:
    """Tests for Collection models."""

    def test_collection_create(self):
        """Test creating a collection."""
        from dataio.api.models import CollectionCreate

        collection = CollectionCreate(
            collection_id="GS0001",
            collection_name="Test Collection",
            category_id="CAT001",
            category_name="Test Category",
        )
        assert collection.collection_id == "GS0001"
        assert collection.collection_name == "Test Collection"

    def test_collection_update(self):
        """Test collection update model."""
        from dataio.api.models import CollectionUpdate

        update = CollectionUpdate(collection_name="Updated Name")
        assert update.collection_name == "Updated Name"
        assert update.category_id is None


class TestTableMetadata:
    """Tests for TableMetadata model."""

    def test_table_metadata(self):
        """Test creating table metadata."""
        from dataio.api.models import TableMetadata, DataDictionaryItem

        metadata = TableMetadata(
            table_name="main_data",
            description="Main data table",
            source="External API",
            data_dictionary={
                "column1": DataDictionaryItem(
                    description="First column",
                    comments="Important field",
                    access=True,
                )
            },
        )
        assert metadata.table_name == "main_data"
        assert "column1" in metadata.data_dictionary

    def test_data_dictionary_item_defaults(self):
        """Test data dictionary item default values."""
        from dataio.api.models import DataDictionaryItem

        item = DataDictionaryItem()
        assert item.description is None
        assert item.comments is None
        assert item.access is True  # Default


class TestRegionResponse:
    """Tests for RegionResponse model."""

    def test_region_response(self):
        """Test creating a region response."""
        from dataio.api.models import RegionResponse

        region = RegionResponse(
            region_id="REG001",
            region_name="Test Region",
            parent_region_id="REG000",
        )
        assert region.region_id == "REG001"
        assert region.parent_region_id == "REG000"

    def test_region_response_no_parent(self):
        """Test region response without parent."""
        from dataio.api.models import RegionResponse

        region = RegionResponse(
            region_id="REG001",
            region_name="Root Region",
        )
        assert region.parent_region_id is None


class TestWeatherModels:
    """Tests for Weather data models."""

    def test_weather_variable_metadata(self):
        """Test weather variable metadata."""
        from dataio.api.models import WeatherVariableMetadata

        var = WeatherVariableMetadata(
            name="t2m",
            long_name="2 metre temperature",
            units="K",
            spatial_resolution="0.25 degrees",
            temporal_resolution="hourly",
        )
        assert var.name == "t2m"
        assert var.units == "K"

    def test_weather_dataset_metadata(self):
        """Test weather dataset metadata."""
        from dataio.api.models import WeatherDatasetMetadata, WeatherVariableMetadata

        dataset = WeatherDatasetMetadata(
            dataset_name="era5_sfc",
            variables=[
                WeatherVariableMetadata(name="t2m", long_name="Temperature"),
                WeatherVariableMetadata(name="tp", long_name="Precipitation"),
            ],
            temporal_coverage_start="2020-01-01",
            temporal_coverage_end="2023-12-31",
            spatial_bounds={
                "min_lat": 6.0,
                "max_lat": 37.0,
                "min_lon": 68.0,
                "max_lon": 98.0,
            },
        )
        assert dataset.dataset_name == "era5_sfc"
        assert len(dataset.variables) == 2

    def test_weather_data_request(self):
        """Test weather data request model."""
        from dataio.api.models import WeatherDataRequest

        request = WeatherDataRequest(
            variables=["t2m", "d2m"],
            start_date="2023-01-01",
            end_date="2023-01-31",
            geojson={
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
            },
        )
        assert request.variables == ["t2m", "d2m"]
        assert request.start_date == "2023-01-01"

    def test_weather_data_request_empty_variables_fails(self):
        """Test that empty variables list fails validation."""
        from dataio.api.models import WeatherDataRequest

        with pytest.raises(ValidationError):
            WeatherDataRequest(
                variables=[],  # min_length=1
                start_date="2023-01-01",
                end_date="2023-01-31",
                geojson={"type": "Feature"},
            )
