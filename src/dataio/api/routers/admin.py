from fastapi import HTTPException, Depends, APIRouter
import logging
from dataio.api.auth import get_user, admin_required

from dataio.api.database import functions as database
from dataio.api.services import DatasetService, UserService
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
from fastapi import HTTPException, Depends, UploadFile, APIRouter

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
    user_service: UserService = Depends(UserService),
):
    return user_service.create_user(user_to_be_created)


@admin_router.get("/users", tags=["admin/users"])
@admin_required
async def get_users(
    user: User = Depends(get_user),
    user_service: UserService = Depends(UserService),
):
    return user_service.get_users()


@admin_router.post("/user-groups", tags=["admin/user-groups"])
@admin_required
async def create_user_group(
    user_group: UserGroupCreate,
    user: User = Depends(get_user),
    user_service: UserService = Depends(UserService),
):
    return user_service.create_user_group(user_group)


@admin_router.post("/resource-groups", tags=["admin/resource-groups"])
@admin_required
async def create_resource_group(
    resource_group: ResourceGroupCreate,
    user: User = Depends(get_user),
    user_service: UserService = Depends(UserService),
):
    return user_service.create_resource_group(resource_group)


@admin_router.post("/resource-group-members", tags=["admin/resource-group-members"])
@admin_required
async def create_resource_group_member(
    resource_group_member: ResourceGroupMemberCreate,
    user: User = Depends(get_user),
    user_service: UserService = Depends(UserService),
):
    return user_service.create_resource_group_member(resource_group_member)


@admin_router.post("/user-permissions", tags=["admin/user-permissions"])
@admin_required
async def create_user_permission(
    user_permission: UserPermissionCreate,
    user: User = Depends(get_user),
    user_service: UserService = Depends(UserService),
):
    return user_service.create_user_permission(user_permission)


###
### RAW DATASETS ENDPOINTS
###


@admin_router.post("/raw-datasets", tags=["admin/raw-datasets"])
@admin_required
async def create_raw_dataset(
    raw_dataset: RawDatasetCreate, user: User = Depends(get_user)
):
    try:
        created_raw_dataset = database.create_raw_dataset(raw_dataset)
        return created_raw_dataset
    except Exception as e:
        logger.error(f"Failed to create raw dataset: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create raw dataset. Contact support."
        )


@admin_router.post("/datasets", tags=["admin/datasets"])
@admin_required
async def create_dataset(
    dataset: DatasetCreate,
    user: User = Depends(get_user),
    dataset_service: DatasetService = Depends(DatasetService),
):
    """
    Create a new dataset.
    """
    return dataset_service.create_dataset(dataset)


# @admin_router.put("/datasets/{dataset_id}", tags=["admin/datasets"])
# async def update_dataset(
#     dataset_id: str, dataset: DatasetCreate, user: User = Depends(get_user)
# ):
#     """
#     Update a dataset.
#     """
#     if not database.check_if_admin(user):
#         raise HTTPException(
#             status_code=403, detail="You are not authorized to update a dataset"
#         )
#     try:
#         updated_dataset = database.update_dataset(dataset_id, dataset)
#         return updated_dataset
#     except Exception as e:
#         logger.error(f"Error updating dataset: {str(e)}")
#         raise HTTPException(
#             status_code=500, detail="Failed to update dataset. Contact support."
#         )


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
    dataset_service: DatasetService = Depends(DatasetService),
):
    return dataset_service.create_dataset_table(
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
    dataset_service: DatasetService = Depends(DatasetService),
):
    return dataset_service.delete_dataset_table(dataset_id, bucket_type, table_name)


####
#### data owners and collections endpoints
####


@admin_router.post("/data-owners", tags=["admin/data-owners"])
@admin_required
async def create_data_owner(
    data_owner: DataOwnerCreate, user: User = Depends(get_user)
):
    try:
        created_data_owner = database.create_data_owner(data_owner)
        return created_data_owner
    except Exception as e:
        logger.error(f"Failed to create data owner: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create data owner. Contact support."
        )


@admin_router.get("/data-owners", tags=["admin/data-owners"])
@admin_required
async def get_data_owners(user: User = Depends(get_user)):
    try:
        data_owners = database.get_data_owners()
        return data_owners
    except Exception as e:
        logger.error(f"Failed to get data owners: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get data owners. Contact support."
        )


@admin_router.post("/collections", tags=["admin/collections"])
@admin_required
async def create_collection(
    collection: CollectionCreate, user: User = Depends(get_user)
):
    try:
        created_collection = database.create_collection(collection)
        return created_collection
    except Exception as e:
        logger.error(f"Failed to create collection: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create collection. Contact support."
        )


@admin_router.get("/collections", tags=["admin/collections"])
@admin_required
async def get_collections(user: User = Depends(get_user)):
    try:
        collections = database.get_collections()
        return collections
    except Exception as e:
        logger.error(f"Failed to get collections: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get collections. Contact support."
        )


# @admin_router.put("/data-owners/{data_owner_id}", tags=["admin/data-owners"])
# async def update_data_owner(
#     data_owner_id: int, data_owner: DataOwnerUpdate, user: User = Depends(get_user)
# ):
#     if not database.check_if_admin(user):
#         raise HTTPException(
#             status_code=403, detail="You are not authorized to update a data owner"
#         )
#     try:
#         updated_data_owner = database.update_data_owner(data_owner_id, data_owner)
#         return updated_data_owner
#     except Exception as e:
#         logger.error(f"Failed to update data owner: {str(e)}")
#         raise HTTPException(
#             status_code=500, detail=f"Failed to update data owner. Contact support."
#         )


# @admin_router.put("/collections/{collection_id}", tags=["admin/collections"])
# async def update_collection(
#     collection_id: int, collection: CollectionUpdate, user: User = Depends(get_user)
# ):
#     if not database.check_if_admin(user):
#         raise HTTPException(
#             status_code=403, detail="You are not authorized to update a collection"
#         )
#     try:
#         updated_collection = database.update_collection(collection_id, collection)
#         return updated_collection
#     except Exception as e:
#         logger.error(f"Failed to update collection: {str(e)}")
#         raise HTTPException(
#             status_code=500, detail=f"Failed to update collection. Contact support."
#         )
