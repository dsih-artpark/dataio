from fastapi import HTTPException, Query, Depends, APIRouter
import logging
from dataio.api.models import User, VersionType
from dataio.api.auth import get_user
from dataio.api.services import UserService
from dataio.api.auth.exceptions import AuthenticationError

logger = logging.getLogger(__name__)

user_router = APIRouter(prefix="/api/v1", tags=["user"])

##
## USER ENDPOINTS
##


@user_router.get("/datasets")
async def get_datasets(
    limit: int = Query(100, ge=1, le=100, description="Number of records to return"),
    user: User = Depends(get_user),
    user_service: UserService = Depends(UserService),
):
    """
    Retrieve a list of datasets with pagination.

    Parameters:
    - limit: Maximum number of records to return (1-100)

    Returns:
    - List of datasets
    """
    return user_service.get_user_datasets(user, limit)


##
## FILESTORE MODIFICATION ENDPOINTS
##


@user_router.get("/datasets/{dataset_id}/{bucket_type}/tables")
async def get_dataset_table_list(
    dataset_id: str,
    bucket_type: VersionType,
    user: User = Depends(get_user),
    user_service: UserService = Depends(UserService),
):
    return user_service.get_dataset_table_list(dataset_id, bucket_type, user)
