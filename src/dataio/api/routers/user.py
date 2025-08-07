from fastapi import HTTPException, Query, Depends, APIRouter
from fastapi.responses import Response
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
    logger.info(f"CATALOGUE_VIEW_REQUEST: {user.email}")
    return user_service.get_user_datasets(user, limit)


@user_router.get("/datasets/{dataset_id}/{bucket_type}/tables")
async def get_dataset_table_list(
    dataset_id: str,
    bucket_type: VersionType,
    user: User = Depends(get_user),
    user_service: UserService = Depends(UserService),
):
    logger.info(
        f"DATASET_DOWNLOAD_REQUEST: {user.email} for dataset {dataset_id} bucket_type {bucket_type}"
    )
    return user_service.get_dataset_table_list(dataset_id, bucket_type, user)


@user_router.get("/shapefiles")
async def get_shapefiles_list(
    user: User = Depends(get_user), user_service: UserService = Depends(UserService)
):
    """
    Get list of shapefiles available on S3.

    Returns:
    - List of available shapefiles with metadata
    """
    logger.info(f"SHAPEFILE_LIST_REQUEST: {user.email}")
    return user_service.get_shapefiles_list(user)


@user_router.get("/shapefiles/{region_id}")
async def get_shapefile(
    region_id: str,
    user: User = Depends(get_user),
    user_service: UserService = Depends(UserService),
):
    logger.info(f"SHAPEFILE_DOWNLOAD_REQUEST: {user.email} for region {region_id}")
    return Response(
        content=user_service.get_shapefile(region_id, user.email),
        headers={
            "Content-Disposition": f'attachment; filename="{region_id}.geojson.gz"'
        },
        media_type="application/gzip",
    )
