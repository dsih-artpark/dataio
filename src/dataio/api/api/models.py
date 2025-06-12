from typing import List, Optional
from pydantic import BaseModel, Field
from dataio.api.database.enums import AccessLevel, VersionType, UpdationFrequency

class DatasetCreate(BaseModel):
    ds_id: str = Field(..., description="Dataset identifier", min_length=1, max_length=50)
    title: str = Field(..., description="Dataset title", min_length=1)
    collection_id: int = Field(..., description="ID of the collection this dataset belongs to")
    data_owner_id: int = Field(..., description="ID of the data owner")
    description: Optional[str] = Field(None, description="Dataset description")
    spatial_coverage_region_id: Optional[str] = Field(None, description="Spatial coverage region ID")
    spatial_resolution: Optional[str] = Field(None, description="Spatial resolution information")
    temporal_coverage_start_date: Optional[str] = Field(None, description="Temporal coverage start date")
    temporal_coverage_end_date: Optional[str] = Field(None, description="Temporal coverage end date")
    temporal_resolution: Optional[str] = Field(None, description="Temporal resolution information")
    access_level: AccessLevel = Field(
        default=AccessLevel.NONE,
        description="Public access level for the dataset"
    )
    additional_metadata: Optional[dict] = Field(None, description="Additional metadata")

class DatasetVersionCreate(BaseModel):
    ds_id: str = Field(..., description="12 digit DS_ID", min_length=12, max_length=12)
    version_id: str = Field(..., description="Version identifier", min_length=1, max_length=20)
    version_title: str = Field(..., description="Version title", min_length=1)
    type: VersionType = Field(..., description="Version type")
    last_modified_date: str = Field(..., description="Last modified date")
    updation_frequency: UpdationFrequency = Field(..., description="Updation frequency")
    access_level: AccessLevel = Field(..., description="Access level")

class User(BaseModel):
    email: str
    is_group: bool