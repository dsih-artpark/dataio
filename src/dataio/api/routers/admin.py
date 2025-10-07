import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, UploadFile

from dataio.api.auth import admin_required, get_user_with_request_state
from dataio.api.models import (
    CollectionCreate,
    DataOwnerCreate,
    DatasetCreate,
    RawDatasetCreate,
    ResourceGroupCreate,
    ResourceGroupMemberCreate,
    User,
    UserCreate,
    UserGroupCreate,
    UserPermissionCreate,
    VersionType,
)
from dataio.api.services import (
    AdminDatasetService,
    AdminUserManagementService,
    UsageTrackingService,
)

logger = logging.getLogger(__name__)

admin_router = APIRouter(prefix="/api/v1/admin", tags=[])

###
### USER MANAGEMENT ENDPOINTS
###


@admin_router.post("/users", tags=["admin/users"])
@admin_required
async def create_user(
    request: Request,
    user_to_be_created: UserCreate,
    logged_in_user: User = Depends(get_user_with_request_state),
    admin_user_service: AdminUserManagementService = Depends(
        AdminUserManagementService
    ),
):
    return admin_user_service.create_user(user_to_be_created)


@admin_router.get("/users", tags=["admin/users"])
@admin_required
async def get_users(
    user: User = Depends(get_user),
    admin_user_service: AdminUserManagementService = Depends(
        AdminUserManagementService
    ),
):
    return admin_user_service.get_users()


@admin_router.post("/user-groups", tags=["admin/user-groups"])
@admin_required
async def create_user_group(
    user_group: UserGroupCreate,
    user: User = Depends(get_user),
    admin_user_service: AdminUserManagementService = Depends(
        AdminUserManagementService
    ),
):
    return admin_user_service.create_user_group(user_group)


@admin_router.post("/resource-groups", tags=["admin/resource-groups"])
@admin_required
async def create_resource_group(
    resource_group: ResourceGroupCreate,
    user: User = Depends(get_user),
    admin_user_service: AdminUserManagementService = Depends(
        AdminUserManagementService
    ),
):
    return admin_user_service.create_resource_group(resource_group)


@admin_router.post("/resource-group-members", tags=["admin/resource-group-members"])
@admin_required
async def create_resource_group_member(
    resource_group_member: ResourceGroupMemberCreate,
    user: User = Depends(get_user),
    admin_user_service: AdminUserManagementService = Depends(
        AdminUserManagementService
    ),
):
    return admin_user_service.create_resource_group_member(resource_group_member)


@admin_router.post("/user-permissions", tags=["admin/user-permissions"])
@admin_required
async def create_user_permission(
    user_permission: UserPermissionCreate,
    user: User = Depends(get_user),
    admin_user_service: AdminUserManagementService = Depends(
        AdminUserManagementService
    ),
):
    return admin_user_service.create_user_permission(user_permission)


###
### RAW DATASETS ENDPOINTS
###


@admin_router.post("/raw-datasets", tags=["admin/raw-datasets"])
@admin_required
async def create_raw_dataset(
    raw_dataset: RawDatasetCreate,
    user: User = Depends(get_user),
    admin_dataset_service: AdminDatasetService = Depends(AdminDatasetService),
):
    return admin_dataset_service.create_raw_dataset(raw_dataset)


@admin_router.post("/datasets", tags=["admin/datasets"])
@admin_required
async def create_dataset(
    dataset: DatasetCreate,
    user: User = Depends(get_user),
    admin_dataset_service: AdminDatasetService = Depends(AdminDatasetService),
):
    """
    Create a new dataset.
    """
    return admin_dataset_service.create_dataset(dataset)


@admin_router.post(
    "/datasets/{dataset_id}/{bucket_type}/tables", tags=["admin/datasets"]
)
@admin_required
async def create_dataset_table(
    dataset_id: str,
    bucket_type: VersionType,
    file: UploadFile,
    table_metadata_file: UploadFile,
    user: User = Depends(get_user),
    admin_dataset_service: AdminDatasetService = Depends(AdminDatasetService),
):
    return admin_dataset_service.create_dataset_table(
        dataset_id, bucket_type, file, table_metadata_file
    )


@admin_router.delete(
    "/datasets/{dataset_id}/{bucket_type}/tables/{table_name}", tags=["admin/datasets"]
)
@admin_required
async def delete_dataset_table(
    dataset_id: str,
    bucket_type: VersionType,
    table_name: str,
    user: User = Depends(get_user),
    admin_dataset_service: AdminDatasetService = Depends(AdminDatasetService),
):
    return admin_dataset_service.delete_dataset_table(
        dataset_id, bucket_type, table_name
    )


####
#### data owners and collections endpoints
####


@admin_router.post("/data-owners", tags=["admin/data-owners"])
@admin_required
async def create_data_owner(
    data_owner: DataOwnerCreate,
    user: User = Depends(get_user),
    admin_dataset_service: AdminDatasetService = Depends(AdminDatasetService),
):
    return admin_dataset_service.create_data_owner(data_owner)


@admin_router.get("/data-owners", tags=["admin/data-owners"])
@admin_required
async def get_data_owners(
    user: User = Depends(get_user),
    admin_dataset_service: AdminDatasetService = Depends(AdminDatasetService),
):
    return admin_dataset_service.get_data_owners()


@admin_router.post("/collections", tags=["admin/collections"])
@admin_required
async def create_collection(
    collection: CollectionCreate,
    user: User = Depends(get_user),
    admin_dataset_service: AdminDatasetService = Depends(AdminDatasetService),
):
    return admin_dataset_service.create_collection(collection)


@admin_router.get("/collections", tags=["admin/collections"])
@admin_required
async def get_collections(
    user: User = Depends(get_user),
    admin_dataset_service: AdminDatasetService = Depends(AdminDatasetService),
):
    return admin_dataset_service.get_collections()


#### SHAPEFILES


@admin_router.post("/shapefiles/{region_id}", tags=["admin/shapefiles"])
@admin_required
async def post_shapefile(
    shapefile: UploadFile,
    region_id: str,
    user: User = Depends(get_user),
    admin_dataset_service: AdminDatasetService = Depends(AdminDatasetService),
):
    return admin_dataset_service.upload_shapefile(shapefile, region_id)


#### USAGE TRACKING ENDPOINTS


@admin_router.get("/usage/stats", tags=["admin/usage"])
@admin_required
async def get_usage_stats(
    request: Request,
    user_email: str = Query(None, description="Filter by specific user email"),
    days: int = Query(30, description="Number of days to look back"),
    user: User = Depends(get_user_with_request_state),
    usage_service: UsageTrackingService = Depends(UsageTrackingService),
):
    """
    Get usage statistics from the tracking database.
    """
    return usage_service.get_usage_stats(user_email, days)


@admin_router.get("/usage/user/{user_email}", tags=["admin/usage"])
@admin_required
async def get_user_activity(
    request: Request,
    user_email: str,
    days: int = Query(30, description="Number of days to look back"),
    user: User = Depends(get_user_with_request_state),
    usage_service: UsageTrackingService = Depends(UsageTrackingService),
):
    """
    Get detailed activity for a specific user.
    """
    return usage_service.get_user_activity(user_email, days)


@admin_router.get("/usage/dataset/{dataset_id}", tags=["admin/usage"])
@admin_required
async def get_dataset_usage(
    request: Request,
    dataset_id: str,
    days: int = Query(30, description="Number of days to look back"),
    user: User = Depends(get_user_with_request_state),
    usage_service: UsageTrackingService = Depends(UsageTrackingService),
):
    """
    Get usage statistics for a specific dataset.
    """
    return usage_service.get_dataset_usage(dataset_id, days)


@admin_router.post("/usage/export", tags=["admin/usage"])
@admin_required
async def export_usage_data(
    request: Request,
    user_email: str = Query(None, description="Filter by specific user email"),
    days: int = Query(30, description="Number of days to look back"),
    user: User = Depends(get_user_with_request_state),
    usage_service: UsageTrackingService = Depends(UsageTrackingService),
):
    """
    Export usage data to CSV format.
    Returns the number of records exported.
    """
    import os
    import tempfile

    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
        temp_path = tmp_file.name

    try:
        exported_count = usage_service.export_to_csv(temp_path, user_email, days)

        # Read the file content
        with open(temp_path, "r", encoding="utf-8") as f:
            csv_content = f.read()

        # Clean up temp file
        os.unlink(temp_path)

        return {
            "exported_records": exported_count,
            "csv_content": csv_content,
            "filename": f"usage_export_{user_email or 'all'}_{days}days.csv",
        }

    except Exception as e:
        # Clean up temp file on error
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise HTTPException(
            status_code=500, detail=f"Error exporting usage data: {str(e)}"
        )


@admin_router.post("/usage/cleanup", tags=["admin/usage"])
@admin_required
async def cleanup_old_usage_data(
    request: Request,
    days_to_keep: int = Query(365, description="Number of days of data to keep"),
    user: User = Depends(get_user_with_request_state),
    usage_service: UsageTrackingService = Depends(UsageTrackingService),
):
    """
    Clean up old usage data to manage database size.
    """
    deleted_count = usage_service.cleanup_old_data(days_to_keep)
    return {
        "deleted_records": deleted_count,
        "days_kept": days_to_keep,
        "message": f"Cleaned up {deleted_count} old usage records, keeping {days_to_keep} days of data",
    }
