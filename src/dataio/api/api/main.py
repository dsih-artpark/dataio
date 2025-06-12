from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, Depends, UploadFile
from pydantic import BaseModel, Field
import logging
from dataio.api import database
from dataio.api.database.models import AccessLevel
from dataio.api.api.models import DatasetCreate, User, DatasetVersionCreate
from dataio.api.routers import secure
from dataio.api.api.auth import get_user


from dataio.api.api.filestore import DatasetVersionS3


# Set up logging
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, filename="api.log", filemode="a")
logger = logging.getLogger(__name__)

app = FastAPI(title="Dataset Management System API")

app.include_router(secure.router, prefix="/api/v1",
                   dependencies=[Depends(get_user)])

@app.get("/")
async def root():
    return {"message": "Welcome to Dataset Management System API"}

##
## DATASETS TABLE ENDPOINTS
##

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
        user_permissions =  database.determine_user_permissions(user)
        datasets = database.get_datasets(limit=limit, user_permissions=user_permissions)
        if not datasets:
            return []
        return datasets
    except Exception as e:
        logger.error(f"Error retrieving datasets: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve datasets. Contact support.")
    
@app.post("/api/v1/datasets")
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
    
@app.post("/api/v1/dataset_versions")
async def create_dataset_version(version: DatasetVersionCreate, user: User = Depends(get_user)):
    """
    Create a new dataset version.
    """
    if not database.check_if_admin(user):
        raise HTTPException(status_code=403, detail="You are not authorized to create a dataset version")
        
    try:
        created_version = database.create_dataset_version(version)
        return created_version
    except Exception as e:
        logger.error(f"Error creating dataset version: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create dataset version. Contact support.")
    
##
## FILESTORE MODIFICATION ENDPOINTS
##
    
@app.post("/api/v1/datasets/{dataset_id}/versions/{version_id}/files")
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
    
@app.get("/api/v1/datasets/{dataset_id}/versions/{version_id}/files")
async def get_dataset_version_files(dataset_id: str, version_id: str, user: User = Depends(get_user)):
    if not database.check_if_admin(user):
        raise HTTPException(status_code=403, detail="You are not authorized to get a dataset version files")
    try:
        dataset_version_s3 = DatasetVersionS3(dataset_id, version_id)
        return dataset_version_s3.list_files_in_s3()
    except Exception as e:
        logger.error(f"Failed to get dataset version files: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get dataset version files. Contact support.")

@app.delete("/api/v1/datasets/{dataset_id}/versions/{version_id}/files/{file_name}")
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