import logging
from typing import List

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import Response

from dataio.api.auth import get_user_with_request_state
from dataio.api.models import RegionResponse, User, VersionType
from dataio.api.services import UserService

logger = logging.getLogger(__name__)

user_router = APIRouter(prefix="/api/v1", tags=["user"])

##
## USER ENDPOINTS
##


@user_router.get("/datasets")
async def get_datasets(
    request: Request,
    limit: int = Query(100, ge=1, le=100, description="Number of records to return"),
    user: User = Depends(get_user_with_request_state),
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
    request: Request,
    dataset_id: str,
    bucket_type: VersionType,
    user: User = Depends(get_user_with_request_state),
    user_service: UserService = Depends(UserService),
):
    logger.info(
        f"DATASET_DOWNLOAD_REQUEST: {user.email} for dataset {dataset_id} bucket_type {bucket_type}"
    )
    return user_service.get_dataset_table_list(dataset_id, bucket_type, user)


@user_router.get("/shapefiles")
async def get_shapefiles_list(
    request: Request,
    user: User = Depends(get_user_with_request_state),
    user_service: UserService = Depends(UserService),
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
    request: Request,
    region_id: str,
    user: User = Depends(get_user_with_request_state),
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


@user_router.get("/regions/{region_id}/children", response_model=List[RegionResponse])
async def get_children_regions(
    request: Request,
    region_id: str,
    user: User = Depends(get_user_with_request_state),
    user_service: UserService = Depends(UserService),
):
    """
    Get all direct children regions for a given region_id.

    Parameters:
    - region_id: The ID of the parent region

    Returns:
    - List of direct children regions with their metadata
    """
    logger.info(f"CHILDREN_REGIONS_REQUEST: {user.email} for region {region_id}")
    return user_service.get_children_regions(region_id, user)
