from typing import List, Optional, Dict
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
