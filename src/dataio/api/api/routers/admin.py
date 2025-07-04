from fastapi import HTTPException, Depends, APIRouter
import logging
from dataio.api.api.auth import get_user
import sqlalchemy.exc

from typing import Annotated

from dataio.api.database import functions as database
from dataio.api.api.models import (
    DatasetCreate,
    User,
    UserCreate,
    VersionType,
    DataOwnerCreate,
    CollectionCreate,
    DataOwnerUpdate,
    CollectionUpdate,
    RawDatasetCreate,
    TableMetadata,
    UserGroupCreate,
    ResourceGroupCreate,
    ResourceGroupMemberCreate,
    UserPermissionCreate,
)
from fastapi import HTTPException, Depends, UploadFile, APIRouter, Form
from dataio.api.api.filestore import DatasetS3, ValidationError

logger = logging.getLogger(__name__)

admin_router = APIRouter(prefix="/api/v1/admin", tags=[])

###
### USER MANAGEMENT ENDPOINTS
###


@admin_router.post("/users", tags=["admin/users"])
async def create_user(
    user_to_be_created: UserCreate, logged_in_user: User = Depends(get_user)
):
    if not database.check_if_admin(logged_in_user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to create a user"
        )
    try:
        created_user = database.create_user(user_to_be_created)
        return created_user
    except sqlalchemy.exc.IntegrityError:
        raise HTTPException(
            status_code=400, detail="Error creating user. User already exists"
        )
    except Exception as e:
        logger.error(f"Failed to create user: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create user. Contact support."
        )


@admin_router.get("/users", tags=["admin/users"])
async def get_users(user: User = Depends(get_user)):
    try:
        if not database.check_if_admin(user):
            raise HTTPException(
                status_code=403, detail="You are not authorized to get users"
            )
        return database.get_users()
    except Exception as e:
        logger.error(f"Failed to get users: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get users. Contact support."
        )


@admin_router.post("/user-groups", tags=["admin/user-groups"])
async def create_user_group(
    user_group: UserGroupCreate, user: User = Depends(get_user)
):
    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to create a user group"
        )
    try:
        created_user_group = database.create_user_group(user_group)
        return created_user_group
    except Exception as e:
        logger.error(f"Failed to create user group: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create user group. Contact support."
        )


@admin_router.post("/resource-groups", tags=["admin/resource-groups"])
async def create_resource_group(
    resource_group: ResourceGroupCreate, user: User = Depends(get_user)
):
    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to create a resource group"
        )
    try:
        created_resource_group = database.create_resource_group(resource_group)
        return created_resource_group
    except Exception as e:
        logger.error(f"Failed to create resource group: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create resource group. Contact support."
        )


@admin_router.post("/resource-group-members", tags=["admin/resource-group-members"])
async def create_resource_group_member(
    resource_group_member: ResourceGroupMemberCreate, user: User = Depends(get_user)
):
    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403,
            detail="You are not authorized to create a resource group member",
        )
    try:
        created_resource_group_member = database.create_resource_group_member(
            resource_group_member
        )
        return created_resource_group_member
    except Exception as e:
        logger.error(f"Failed to create resource group member: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create resource group member. Contact support.",
        )


@admin_router.post("/user-permissions", tags=["admin/user-permissions"])
async def create_user_permission(
    user_permission: UserPermissionCreate, user: User = Depends(get_user)
):
    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to create a user permission"
        )
    try:
        created_user_permission = database.create_user_permission(user_permission)
        return created_user_permission
    except Exception as e:
        logger.error(f"Failed to create user permission: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create user permission. Contact support.",
        )


###
### RAW DATASETS ENDPOINTS
###


@admin_router.post("/raw-datasets", tags=["admin/raw-datasets"])
async def create_raw_dataset(
    raw_dataset: RawDatasetCreate, user: User = Depends(get_user)
):
    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to create a raw dataset"
        )
    try:
        created_raw_dataset = database.create_raw_dataset(raw_dataset)
        return created_raw_dataset
    except Exception as e:
        logger.error(f"Failed to create raw dataset: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create raw dataset. Contact support."
        )


@admin_router.post("/datasets", tags=["admin/datasets"])
async def create_dataset(dataset: DatasetCreate, user: User = Depends(get_user)):
    """
    Create a new dataset.
    """
    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to create a dataset"
        )
    try:
        created_dataset = database.create_dataset(dataset)
        return created_dataset
    except Exception as e:
        logger.error(f"Error creating dataset: {str(e)}")
        raise HTTPException(
            status_code=500, detail="Failed to create dataset. Contact support."
        )


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
async def create_dataset_table(
    dataset_id: str,
    bucket_type: VersionType,
    file: UploadFile,
    table_metadata_file: UploadFile,
    user: User = Depends(get_user),
):
    # Table metadata should also be provided

    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to create a dataset file"
        )
    try:
        table_metadata = TableMetadata.model_validate_json(
            table_metadata_file.file.read()
        )
        dataset_s3 = DatasetS3(dataset_id, bucket_type)
        dataset_s3.upload_file(file, table_metadata)
        return {"message": "File uploaded successfully"}
    except ValidationError as e:
        logger.error(f"Failed to upload file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Validation error raised: {e}")
    except Exception as e:
        logger.error(f"Failed to upload file: {str(e)}")
        raise HTTPException(
            status_code=500, detail="Failed to upload file. Contact support."
        )


@admin_router.delete(
    "/datasets/{dataset_id}/{bucket_type}/tables/{table_name}", tags=["admin/datasets"]
)
async def delete_dataset_table(
    dataset_id: str,
    bucket_type: VersionType,
    table_name: str,
    user: User = Depends(get_user),
):
    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to delete a dataset file"
        )
    try:
        dataset_s3 = DatasetS3(dataset_id, bucket_type)
        dataset_s3.delete_file(table_name)
        return {"message": "File deleted successfully"}
    except Exception as e:
        logger.error(f"Failed to delete dataset version file: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete dataset version file. Contact support.",
        )


####
#### data owners and collections endpoints
####


@admin_router.post("/data-owners", tags=["admin/data-owners"])
async def create_data_owner(
    data_owner: DataOwnerCreate, user: User = Depends(get_user)
):
    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to create a data owner"
        )
    try:
        created_data_owner = database.create_data_owner(data_owner)
        return created_data_owner
    except Exception as e:
        logger.error(f"Failed to create data owner: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create data owner. Contact support."
        )


@admin_router.get("/data-owners", tags=["admin/data-owners"])
async def get_data_owners(user: User = Depends(get_user)):
    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to get data owners"
        )
    try:
        data_owners = database.get_data_owners()
        return data_owners
    except Exception as e:
        logger.error(f"Failed to get data owners: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get data owners. Contact support."
        )


@admin_router.post("/collections", tags=["admin/collections"])
async def create_collection(
    collection: CollectionCreate, user: User = Depends(get_user)
):
    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to create a collection"
        )
    try:
        created_collection = database.create_collection(collection)
        return created_collection
    except Exception as e:
        logger.error(f"Failed to create collection: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create collection. Contact support."
        )


@admin_router.get("/collections", tags=["admin/collections"])
async def get_collections(user: User = Depends(get_user)):
    if not database.check_if_admin(user):
        raise HTTPException(
            status_code=403, detail="You are not authorized to get collections"
        )
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
