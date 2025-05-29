from typing import List, Optional
from pydantic import BaseModel, Field
from dataset_manager.database.models import AccessLevel

class DatasetCreate(BaseModel):
    raw_dataset_ids: List[int] = Field(..., description="List of raw dataset IDs")
    ds_id: str = Field(..., description="Dataset identifier", min_length=1, max_length=50)
    title: str = Field(..., description="Dataset title", min_length=1)
    collection_id: int = Field(..., description="ID of the collection this dataset belongs to")
    data_owner_id: int = Field(..., description="ID of the data owner")
    concept_id: int = Field(..., description="ID of the concept this dataset represents")
    description: Optional[str] = Field(None, description="Dataset description")
    tag_ids: Optional[List[int]] = Field(default=[], description="List of tag IDs")
    spatial_coverage: Optional[str] = Field(None, description="Spatial coverage information")
    spatial_resolution: Optional[str] = Field(None, description="Spatial resolution information")
    temporal_coverage: Optional[str] = Field(None, description="Temporal coverage information")
    temporal_resolution: Optional[str] = Field(None, description="Temporal resolution information")
    public_access_level: AccessLevel = Field(
        default=AccessLevel.NONE,
        description="Public access level for the dataset"
    )
    notes: Optional[str] = Field(None, description="Additional notes")
    supplementary_documents: Optional[str] = Field(None, description="Supplementary document information")
