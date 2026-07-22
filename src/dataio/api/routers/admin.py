from fastapi import HTTPException, Depends, APIRouter, Form, UploadFile
from typing import List
import logging
from dataio.api.auth import get_user, admin_required
from dataio.api.services import AdminUserManagementService, AdminDatasetService, DraftReviewService
from dataio.api.models import (
    DatasetCreate,
    ManifestDraftFlagField,
    ManifestDraftReject,
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


@admin_router.put(
    "/datasets/{dataset_id}/{bucket_type}/manifest", tags=["admin/datasets"]
)
@admin_required
async def upsert_dataset_manifest(
    dataset_id: str,
    bucket_type: VersionType,
    manifest_file: UploadFile,
    user: User = Depends(get_user),
    admin_dataset_service: AdminDatasetService = Depends(AdminDatasetService),
):
    return admin_dataset_service.upsert_dataset_manifest(
        dataset_id, bucket_type, manifest_file, user.email
    )


@admin_router.get(
    "/datasets/{dataset_id}/{bucket_type}/manifest", tags=["admin/datasets"]
)
@admin_required
async def get_dataset_manifest(
    dataset_id: str,
    bucket_type: VersionType,
    user: User = Depends(get_user),
    admin_dataset_service: AdminDatasetService = Depends(AdminDatasetService),
):
    return admin_dataset_service.get_dataset_manifest(dataset_id, bucket_type)


###
### MANIFEST DRAFTS (LLM-drafted metadata.yaml, pending curator review)
###


@admin_router.post("/manifest-drafts/generate", tags=["admin/manifest-drafts"])
@admin_required
async def generate_manifest_draft(
    csv_files: List[UploadFile],
    category_id: str = Form(...),
    collection_id: str = Form(...),
    data_owner_name: str = Form(...),
    created_by: str = Form(None),
    dataset_id: str = Form(None),
    digitization_log_file: UploadFile = None,
    user: User = Depends(get_user),
    draft_review_service: DraftReviewService = Depends(DraftReviewService),
):
    return draft_review_service.generate_draft_from_upload(
        csv_files=csv_files,
        category_id=category_id,
        collection_id=collection_id,
        data_owner_name=data_owner_name,
        created_by=created_by or user.email,
        dataset_id=dataset_id,
        digitization_log_file=digitization_log_file,
    )


@admin_router.get("/manifest-drafts", tags=["admin/manifest-drafts"])
@admin_required
async def list_manifest_drafts(
    status: str = None,
    dataset_id: str = None,
    limit: int = 50,
    offset: int = 0,
    user: User = Depends(get_user),
    draft_review_service: DraftReviewService = Depends(DraftReviewService),
):
    return draft_review_service.list_drafts(status=status, dataset_id=dataset_id, limit=limit, offset=offset)


@admin_router.get("/manifest-drafts/{draft_id}", tags=["admin/manifest-drafts"])
@admin_required
async def get_manifest_draft(
    draft_id: str,
    user: User = Depends(get_user),
    draft_review_service: DraftReviewService = Depends(DraftReviewService),
):
    return draft_review_service.get_draft(draft_id)


@admin_router.delete("/manifest-drafts/{draft_id}", tags=["admin/manifest-drafts"])
@admin_required
async def delete_manifest_draft(
    draft_id: str,
    user: User = Depends(get_user),
    draft_review_service: DraftReviewService = Depends(DraftReviewService),
):
    draft_review_service.delete_draft(draft_id)
    return {"message": "Manifest draft deleted", "draft_id": draft_id}


@admin_router.post("/manifest-drafts/{draft_id}/validate", tags=["admin/manifest-drafts"])
@admin_required
async def revalidate_manifest_draft(
    draft_id: str,
    user: User = Depends(get_user),
    draft_review_service: DraftReviewService = Depends(DraftReviewService),
):
    return draft_review_service.revalidate_draft(draft_id)


@admin_router.post("/manifest-drafts/{draft_id}/approve", tags=["admin/manifest-drafts"])
@admin_required
async def approve_manifest_draft(
    draft_id: str,
    user: User = Depends(get_user),
    draft_review_service: DraftReviewService = Depends(DraftReviewService),
):
    return draft_review_service.approve_draft(draft_id, user.email)


@admin_router.post("/manifest-drafts/{draft_id}/reject", tags=["admin/manifest-drafts"])
@admin_required
async def reject_manifest_draft(
    draft_id: str,
    body: ManifestDraftReject,
    user: User = Depends(get_user),
    draft_review_service: DraftReviewService = Depends(DraftReviewService),
):
    return draft_review_service.reject_draft(draft_id, user.email, reason=body.reason)


@admin_router.post("/manifest-drafts/{draft_id}/flag-field", tags=["admin/manifest-drafts"])
@admin_required
async def flag_manifest_draft_field(
    draft_id: str,
    body: ManifestDraftFlagField,
    user: User = Depends(get_user),
    draft_review_service: DraftReviewService = Depends(DraftReviewService),
):
    return draft_review_service.flag_field(draft_id, body.field_path, body.note, user.email)


@admin_router.post("/manifest-drafts/{draft_id}/regenerate", tags=["admin/manifest-drafts"])
@admin_required
async def regenerate_manifest_draft(
    draft_id: str,
    user: User = Depends(get_user),
    draft_review_service: DraftReviewService = Depends(DraftReviewService),
):
    return draft_review_service.regenerate_draft(draft_id, user.email)


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
