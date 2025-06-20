from fastapi import HTTPException, Depends, APIRouter
import logging
from dataio.api import database
from dataio.api.api.models import User, UserCreate
from dataio.api.api.auth import get_user
import sqlalchemy.exc

from dataio.api import database
from dataio.api.api.models import DatasetCreate, User, DatasetUpdate, VersionType
from fastapi import HTTPException, Depends, UploadFile, APIRouter
from dataio.api.api.filestore import DatasetS3

logger = logging.getLogger(__name__)

admin_router = APIRouter(
    prefix = "/api/v1/admin",
    tags = ["admin"]
)

###
### USER MANAGEMENT ENDPOINTS
###

@admin_router.post("/users")
async def create_user(user_to_be_created: UserCreate, logged_in_user: User = Depends(get_user)):
    if not database.check_if_admin(logged_in_user):
        raise HTTPException(status_code=403, detail="You are not authorized to create a user")
    try:
        created_user = database.create_user(user_to_be_created)
        return created_user
    except sqlalchemy.exc.IntegrityError:
        raise HTTPException(status_code=400, detail="Error creating user. User already exists")
    except Exception as e:
        logger.error(f"Failed to create user: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create user. Contact support.")


@admin_router.post("/")
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

@admin_router.put("/datasets/{dataset_id}")
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

@admin_router.post("/datasets/{dataset_id}/{bucket_type}/tables")
async def create_dataset_table(dataset_id: str, bucket_type: VersionType, file: UploadFile, user: User = Depends(get_user)):

    # Table metadata should also be provided

    if not database.check_if_admin(user):
            raise HTTPException(status_code=403, detail="You are not authorized to create a dataset file")
    try:
        dataset_s3 = DatasetS3(dataset_id, bucket_type)
        dataset_s3.upload_file(file)
        return {"message": "File uploaded successfully"}
    except Exception as e:
        logger.error(f"Failed to upload file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to upload file. Contact support.")
    
@admin_router.delete("/datasets/{dataset_id}/{bucket_type}/tables/{table_name}")
async def delete_dataset_table(dataset_id: str, bucket_type: VersionType, table_name: str, user: User = Depends(get_user)):
    if not database.check_if_admin(user):
        raise HTTPException(status_code=403, detail="You are not authorized to delete a dataset file")
    try:
        dataset_s3 = DatasetS3(dataset_id, bucket_type)
        dataset_s3.delete_file(table_name)
        return {"message": "File deleted successfully"}
    except Exception as e:
        logger.error(f"Failed to delete dataset version file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete dataset version file. Contact support.")