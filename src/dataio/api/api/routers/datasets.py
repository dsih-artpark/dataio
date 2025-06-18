from fastapi import HTTPException, Query, Depends, UploadFile, APIRouter
import logging
from dataio.api import database
from dataio.api.api.models import DatasetCreate, User, DatasetVersionCreate, DatasetUpdate
from dataio.api.api.auth import get_user
from dataio.api.api.filestore import DatasetVersionS3

logger = logging.getLogger(__name__)

dataset_router = APIRouter(
    prefix = "/api/v1/datasets",
    tags = ["datasets"]
)

##
## DATASETS TABLE ENDPOINTS
##

@dataset_router.get("/")
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
        user_permissions =  database.determine_user_permissions(user)
        datasets = database.get_datasets(limit=limit, user_permissions=user_permissions)
        if not datasets:
            return []
        print(datasets)
        return datasets
    except Exception as e:
        logger.error(f"Error retrieving datasets: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve datasets. Contact support.")
    
@dataset_router.post("/")
async def create_dataset(dataset: DatasetCreate, user: User = Depends(get_user)):
    """
    Create a new dataset.
    """
    if not database.check_if_admin(user):
        raise HTTPException(status_code=403, detail="You are not authorized to create a dataset")
    try:
        created_dataset = database.create_dataset(dataset)
        return created_dataset
    except Exception as e:
        logger.error(f"Error creating dataset: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create dataset. Contact support.")

@dataset_router.put("/{dataset_id}")
async def update_dataset(dataset_id: str, dataset: DatasetUpdate, user: User = Depends(get_user)):
    """
    Update a dataset.
    """
    if not database.check_if_admin(user):
        raise HTTPException(status_code=403, detail="You are not authorized to update a dataset")
    try:
        updated_dataset = database.update_dataset(dataset_id, dataset)
        return updated_dataset
    except Exception as e:
        logger.error(f"Error updating dataset: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to update dataset. Contact support.")

@dataset_router.post("/{dataset_id}/versions")
async def create_dataset_version(dataset_id: str, version: DatasetVersionCreate, user: User = Depends(get_user)):
    """
    Create a new dataset version.
    """
    if not database.check_if_admin(user):
        raise HTTPException(status_code=403, detail="You are not authorized to create a dataset version")
        
    try:
        created_version = database.create_dataset_version(dataset_id, version)
        return created_version
    except Exception as e:
        logger.error(f"Error creating dataset version: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create dataset version. Contact support.")
    
##
## FILESTORE MODIFICATION ENDPOINTS
##
    
@dataset_router.post("/{dataset_id}/versions/{version_id}/files")
async def create_dataset_version_file(dataset_id: str, version_id: str, file: UploadFile, user: User = Depends(get_user)):
    if not database.check_if_admin(user):
            raise HTTPException(status_code=403, detail="You are not authorized to create a dataset version file")
    try:
        dataset_version_s3 = DatasetVersionS3(dataset_id, version_id)
        dataset_version_s3.upload_file(file)
        return {"message": "File uploaded successfully"}
    except Exception as e:
        logger.error(f"Failed to upload file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to upload file. Contact support.")
    
@dataset_router.get("/{dataset_id}/versions/{version_id}/files")
async def get_dataset_version_files(dataset_id: str, version_id: str, user: User = Depends(get_user)):
    if not database.check_if_admin(user):
        raise HTTPException(status_code=403, detail="You are not authorized to get a dataset version files")
    try:
        dataset_version_s3 = DatasetVersionS3(dataset_id, version_id)
        return dataset_version_s3.list_files_in_s3()
    except Exception as e:
        logger.error(f"Failed to get dataset version files: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get dataset version files. Contact support.")

@dataset_router.delete("/{dataset_id}/versions/{version_id}/files/{file_name}")
async def delete_dataset_version_file(dataset_id: str, version_id: str, file_name: str, user: User = Depends(get_user)):
    if not database.check_if_admin(user):
        raise HTTPException(status_code=403, detail="You are not authorized to delete a dataset version file")
    try:
        dataset_version_s3 = DatasetVersionS3(dataset_id, version_id)
        dataset_version_s3.delete_file(file_name)
        return {"message": "File deleted successfully"}
    except Exception as e:
        logger.error(f"Failed to delete dataset version file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete dataset version file. Contact support.")
    