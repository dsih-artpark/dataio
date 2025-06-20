from fastapi import HTTPException, Query, Depends, UploadFile, APIRouter
import logging
from dataio.api import database
from dataio.api.api.models import DatasetCreate, User, DatasetUpdate, VersionType
from dataio.api.api.auth import get_user
from dataio.api.api.filestore import DatasetS3
from dataio.api.api.utils import user_has_preprocessed_access, user_has_dataset_download_access

logger = logging.getLogger(__name__)

user_router = APIRouter(
    prefix = "/api/v1",
    tags = ["user"]
)

##
## USER ENDPOINTS
##

@user_router.get("/datasets")
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

##
## FILESTORE MODIFICATION ENDPOINTS
##
    
@user_router.get("/datasets/{dataset_id}/{bucket_type}/tables")
async def get_dataset_table_list(dataset_id: str, bucket_type: VersionType, user: User = Depends(get_user)):

    # TODO: Response should have table metadata as well. 

    try:
        user_permissions =  database.determine_user_permissions(user)

        if bucket_type == VersionType.PREPROCESSED and not user_has_preprocessed_access(user_permissions):
            raise HTTPException(status_code=403, detail="You are not authorized to get preprocessed files")
        
        if not user_has_dataset_download_access(user_permissions, dataset_id):
            raise HTTPException(status_code=403, detail="You are not authorized to get a dataset files")
        
        dataset_s3 = DatasetS3(dataset_id, bucket_type)
        files_list =  dataset_s3.list_files_in_s3()
        if not files_list:
            raise HTTPException(status_code=404, detail="No files found in bucket")
        return files_list

    except HTTPException as e:
        logger.error(f"Failed to get dataset files: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Failed to get dataset files: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get dataset files. Contact support.")