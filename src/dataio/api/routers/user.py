from typing import List
from fastapi import HTTPException, Query, Depends, APIRouter
from fastapi.responses import Response
import logging
from dataio.api.models import (
    User,
    VersionType,
    RegionResponse,
    WeatherDatasetMetadata,
    WeatherDataRequest,
)
from dataio.api.auth import (
    get_user,
    determine_user_permissions,
    user_has_weather_data_view_access,
    user_has_weather_data_download_access,
)
from dataio.api.services import UserService
from dataio.api.services.weather_service import WeatherService
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


@user_router.get("/regions/{region_id}/children", response_model=List[RegionResponse])
async def get_children_regions(
    region_id: str,
    user: User = Depends(get_user),
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


##
## WEATHER DATA ENDPOINTS
##


@user_router.get(
    "/weather/datasets", response_model=List[WeatherDatasetMetadata]
)
async def get_weather_datasets(
    user: User = Depends(get_user),
    weather_service: WeatherService = Depends(WeatherService),
):
    """
    List all available weather datasets with metadata.

    Requires VIEW or DOWNLOAD permission for WEATHER_DATA_API resource.

    Returns:
    - List of weather datasets with variables, temporal coverage, spatial bounds, and resolution info
    """
    logger.info(f"WEATHER_DATASETS_LIST_REQUEST: {user.email}")

    # Check permissions
    user_permissions = determine_user_permissions(user)
    if not user_has_weather_data_view_access(user_permissions):
        raise HTTPException(
            status_code=403,
            detail="You are not authorized to view weather data. Contact admin for access."
        )

    return weather_service.list_weather_datasets()


@user_router.post("/weather/datasets/{dataset_name}/download")
async def download_weather_data(
    dataset_name: str,
    request: WeatherDataRequest,
    user: User = Depends(get_user),
    weather_service: WeatherService = Depends(WeatherService),
):
    """
    Download weather data with spatial and temporal filtering.

    Requires DOWNLOAD permission for WEATHER_DATA_API resource.

    Parameters:
    - dataset_name: Name of the weather dataset (e.g., "era5_sfc")
    - request: WeatherDataRequest with filtering parameters (variables, dates, geojson)

    Returns:
    - NetCDF file with filtered weather data
    """
    logger.info(
        f"WEATHER_DATA_DOWNLOAD_REQUEST: {user.email} for dataset {dataset_name}, "
        f"variables {request.variables}, {request.start_date} to {request.end_date}"
    )

    # Check permissions
    user_permissions = determine_user_permissions(user)
    if not user_has_weather_data_download_access(user_permissions):
        raise HTTPException(
            status_code=403,
            detail="You are not authorized to download weather data. Contact admin for DOWNLOAD access."
        )

    netcdf_data = weather_service.get_weather_data(dataset_name, request)

    # Build descriptive filename
    # Format: dataset_name_var1_var2_YYYYMMDD_YYYYMMDD_region_id.netcdf
    from datetime import datetime

    variables_str = "_".join(request.variables)

    # Parse and format dates
    start_date_obj = datetime.fromisoformat(request.start_date.replace('Z', '+00:00'))
    end_date_obj = datetime.fromisoformat(request.end_date.replace('Z', '+00:00'))
    start_str = start_date_obj.strftime("%Y%m%d")
    end_str = end_date_obj.strftime("%Y%m%d")

    # Extract region_id from geojson properties if available
    region_id = None
    if "properties" in request.geojson:
        region_id = request.geojson["properties"].get("region_id")
    elif request.geojson.get("type") == "FeatureCollection" and request.geojson.get("features"):
        # Check first feature
        first_feature = request.geojson["features"][0]
        if "properties" in first_feature:
            region_id = first_feature["properties"].get("region_id")

    # Construct filename
    filename_parts = [dataset_name, variables_str, start_str, end_str]
    if region_id:
        filename_parts.append(str(region_id))
    filename = "_".join(filename_parts) + ".nc"

    return Response(
        content=netcdf_data,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
        media_type="application/x-netcdf",
    )
