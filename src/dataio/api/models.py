from typing import Any, List, Optional, Dict
from pydantic import BaseModel, Field
from dataio.api.database.enums import (
    AccessLevel,
    VersionType,
    UpdationFrequency,
    ResourceType,
)


class RawDatasetCreate(BaseModel):
    rds_id: str = Field(..., description="Raw dataset identifier", min_length=1)
    title: str = Field(..., description="Raw dataset title", min_length=1)
    source: str = Field(..., description="Raw dataset source", min_length=1)


class RawDatasetUpdate(BaseModel):
    title: Optional[str] = Field(None, description="Raw dataset title")
    source: Optional[str] = Field(None, description="Raw dataset source")


class DatasetCreate(BaseModel):
    ds_id: str = Field(
        ..., description="Dataset identifier", min_length=1, max_length=50
    )
    title: str = Field(..., description="Dataset title", min_length=1)
    collection_id: str = Field(..., description="Collection ID this dataset belongs to")
    data_owner_name: str = Field(..., description="Name of the data owner")
    description: Optional[str] = Field(None, description="Dataset description")
    spatial_coverage_region_id: Optional[str] = Field(
        None, description="Spatial coverage region ID"
    )
    spatial_resolution: Optional[str] = Field(
        None, description="Spatial resolution information"
    )
    temporal_coverage_start_date: Optional[str] = Field(
        None, description="Temporal coverage start date"
    )
    temporal_coverage_end_date: Optional[str] = Field(
        None, description="Temporal coverage end date"
    )
    temporal_resolution: Optional[str] = Field(
        None, description="Temporal resolution information"
    )
    access_level: AccessLevel = Field(
        default=AccessLevel.NONE, description="Public access level for the dataset"
    )
    additional_metadata: Optional[dict] = Field(None, description="Additional metadata")
    tags: Optional[List[str]] = Field(None, description="Tags for the dataset")
    raw_dataset_ids: List[str] = Field(
        min_length=1,
        description="Raw dataset IDs for the dataset",
    )


class DatasetUpdate(BaseModel):
    ds_id: Optional[str] = Field(None, description="Dataset identifier")
    title: Optional[str] = Field(None, description="Dataset title")
    collection_id: Optional[str] = Field(None, description="Collection ID this dataset belongs to")
    data_owner_name: Optional[str] = Field(None, description="Name of the data owner")
    description: Optional[str] = Field(None, description="Dataset description")
    spatial_coverage_region_id: Optional[str] = Field(
        None, description="Spatial coverage region ID"
    )
    spatial_resolution: Optional[str] = Field(
        None, description="Spatial resolution information"
    )
    temporal_coverage_start_date: Optional[str] = Field(
        None, description="Temporal coverage start date"
    )
    temporal_coverage_end_date: Optional[str] = Field(
        None, description="Temporal coverage end date"
    )
    temporal_resolution: Optional[str] = Field(
        None, description="Temporal resolution information"
    )
    access_level: Optional[AccessLevel] = Field(
        default=None, description="Public access level for the dataset"
    )
    additional_metadata: Optional[dict] = Field(None, description="Additional metadata")
    tags: Optional[List[str]] = Field(None, description="Tags for the dataset")
    raw_dataset_ids: Optional[List[str]] = Field(
        default=None,
        description="Raw dataset IDs for the dataset",
    )


class DatasetDocumentationUpdate(BaseModel):
    readme_md: Optional[str] = Field(
        None, description="Dataset README markdown content"
    )
    data_dictionary_json: Optional[Any] = Field(
        None, description="Dataset data dictionary JSON payload"
    )


class ManifestDraftReject(BaseModel):
    reason: Optional[str] = Field(None, description="Why this draft was rejected")


class ManifestDraftFlagField(BaseModel):
    field_path: str = Field(..., description="Dotted path or column name being flagged")
    note: str = Field(..., description="Why this field needs curator attention")


class ManifestDraftEdit(BaseModel):
    draft_yaml: str = Field(..., description="Full replacement manifest YAML, curator-edited")


class ManifestDraftInfoYamlRequest(BaseModel):
    access_level: AccessLevel = Field(
        ..., description="Access level for the generated info.yml - not derivable from the draft"
    )


class ClassifyColumnsRequest(BaseModel):
    table_name: str = Field(..., description="Name of the table (CSV filename stem)")
    column_names: List[str] = Field(
        ..., description="Column names read from the CSV header - schema only"
    )


class User(BaseModel):
    email: str
    is_group: bool


class UserCreate(BaseModel):
    email: str
    is_group: bool


class UserGroupCreate(BaseModel):
    group_email: str
    user_email: str


class ResourceGroupCreate(BaseModel):
    resource_group_id: str
    group_name: str


class UserPermissionCreate(BaseModel):
    user_email: str
    resource_type: ResourceType
    resource_id: str
    permission: AccessLevel


class ResourceGroupMemberCreate(BaseModel):
    resource_group_id: str
    resource_id: str
    resource_type: ResourceType
    resource_json: Optional[dict] = None


class UserReturn(BaseModel):
    email: str
    is_group: bool
    key: Optional[str] = None
    message: Optional[str] = None


class DataOwnerCreate(BaseModel):
    name: str
    contact_person: Optional[str] = None
    contact_person_email: Optional[str] = None


class DataOwnerUpdate(BaseModel):
    name: Optional[str] = None
    contact_person: Optional[str] = None
    contact_person_email: Optional[str] = None


class CollectionCreate(BaseModel):
    collection_id: str
    collection_name: str
    category_id: str
    category_name: str


class CollectionUpdate(BaseModel):
    collection_name: Optional[str] = None
    category_id: Optional[str] = None
    category_name: Optional[str] = None


class DataDictionaryItem(BaseModel):
    description: Optional[str] = None
    comments: Optional[str] = None
    access: bool = True


class TableMetadata(BaseModel):
    table_name: str
    description: Optional[str] = None
    source: Optional[str] = None
    data_dictionary: Optional[Dict[str, DataDictionaryItem]] = None


class RegionResponse(BaseModel):
    region_id: str
    region_name: str
    parent_region_id: Optional[str] = None


# Weather Data Models


class WeatherVariableMetadata(BaseModel):
    """Metadata for a single weather variable."""

    name: str
    long_name: Optional[str] = None
    units: Optional[str] = None
    spatial_resolution: Optional[str] = None  # e.g., "0.25 degrees", "25 km"
    temporal_resolution: Optional[str] = None  # e.g., "hourly", "daily"


class WeatherDatasetMetadata(BaseModel):
    """Metadata for a weather dataset in the Zarr store."""

    dataset_name: str
    variables: List[WeatherVariableMetadata]
    temporal_coverage_start: str
    temporal_coverage_end: str
    spatial_bounds: Dict[str, float]  # {min_lat, max_lat, min_lon, max_lon}


class WeatherDataRequest(BaseModel):
    """Request model for weather data download."""

    variables: List[str] = Field(
        ..., description="List of variables to extract", min_length=1
    )
    start_date: str = Field(
        ...,
        description="Start date in ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)",
    )
    end_date: str = Field(
        ..., description="End date in ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"
    )
    geojson: Dict = Field(
        ..., description="GeoJSON object defining spatial coverage (Feature or FeatureCollection)"
    )
