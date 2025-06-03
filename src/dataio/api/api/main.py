from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel, Field
from dataio.api import database
from dataio.api.database.models import AccessLevel
from dataio.api.api.models import DatasetCreate, User
import logging
from dataio.api.routers import secure
from dataio.api.api.auth import get_user

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Dataset Management System API")

app.include_router(secure.router, prefix="/api/v1",
                   dependencies=[Depends(get_user)])

@app.get("/")
async def root():
    return {"message": "Welcome to Dataset Management System API"}

@app.get("/api/v1/datasets")
async def get_datasets(
    limit: int = Query(100, ge=1, le=100, description="Number of records to return"),
    user: User = Depends(get_user)
):
    """
    Retrieve a list of datasets with pagination.
    
    Parameters:
    - limit: Maximum number of records to return (1-100)
    
    Returns:
    - List of datasets
    """
    try:
        print('hi')
        datasets = database.get_datasets(limit=limit)
        if not datasets:
            return []
        return datasets
    except Exception as e:
        logger.error(f"Error retrieving datasets: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# @app.get("/datasets/{dataset_id}")
# async def get_dataset(dataset_id: int):
#     """
#     Retrieve a specific dataset by its ID.
    
#     Parameters:
#     - dataset_id: The unique identifier of the dataset
    
#     Returns:
#     - Dataset details
    
#     Raises:
#     - HTTPException: If dataset is not found
#     """
#     try:
#         dataset = database.get_dataset_by_id(dataset_id)
#         if dataset is None:
#             raise HTTPException(status_code=404, detail="Dataset not found")
#         return dataset
#     except Exception as e:
#         logger.error(f"Error retrieving dataset {dataset_id}: {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.post("/datasets/", status_code=201)
# async def create_dataset(dataset: DatasetCreate):
#     """
#     Create a new dataset.
    
#     Parameters:
#     - dataset: Dataset creation data.
#       public_access_level: possible enum values are:
#         - NONE
#         - VIEW
#         - DOWNLOAD
    
#     Returns:
#     - Created dataset details
    
#     Raises:
#     - HTTPException: If dataset creation fails
#     """
#     try:
#         created_dataset = database.create_dataset(
#             raw_dataset_ids=dataset.raw_dataset_ids,
#             ds_id=dataset.ds_id,
#             title=dataset.title,
#             collection_id=dataset.collection_id,
#             data_owner_id=dataset.data_owner_id,
#             concept_id=dataset.concept_id,
#             description=dataset.description,
#             tag_ids=dataset.tag_ids,
#             spatial_coverage=dataset.spatial_coverage,
#             spatial_resolution=dataset.spatial_resolution,
#             temporal_coverage=dataset.temporal_coverage,
#             temporal_resolution=dataset.temporal_resolution,
#             public_access_level=dataset.public_access_level,
#             notes=dataset.notes,
#             supplementary_documents=dataset.supplementary_documents
#         )
#         return created_dataset
#     except Exception as e:
#         logger.error(f"Error creating dataset: {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))
