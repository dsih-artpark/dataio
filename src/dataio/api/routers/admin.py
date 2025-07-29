from fastapi import HTTPException, Depends, APIRouter, UploadFile
import logging
from dataio.api.auth import get_user, admin_required
from dataio.api.services import AdminUserManagementService, AdminDatasetService
from dataio.api.models import (
    DatasetCreate,
    User,
    UserCreate,
    VersionType,
    DataOwnerCreate,
    CollectionCreate,
    RawDatasetCreate,
    UserGroupCreate,
    ResourceGroupCreate,
    ResourceGroupMemberCreate,
    UserPermissionCreate,
)

logger = logging.getLogger(__name__)

admin_router = APIRouter(prefix="/api/v1/admin", tags=[])

###
### USER MANAGEMENT ENDPOINTS
###


@admin_router.post("/users", tags=["admin/users"])
@admin_required
async def create_user(
    user_to_be_created: UserCreate,
    logged_in_user: User = Depends(get_user),
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
