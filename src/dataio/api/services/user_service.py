from fastapi import HTTPException
from dataio.api.models import User, VersionType
from dataio.api.database import functions as database
from dataio.api.auth import (
    determine_user_permissions,
    user_has_preprocessed_access,
    user_has_dataset_download_access,
)
from dataio.api.services.filestore_service import FilestoreService
from dataio.api.services.base_service import BaseService


class UserService(BaseService):
    """Service for user-facing operations."""

    def __init__(self):
        super().__init__()
        self.filestore_service = FilestoreService()

    def get_user_datasets(self, user: User, limit: int = 100):
        """
        Get datasets for a user with permissions applied.
        """
        try:
            user_permissions = determine_user_permissions(user)
            datasets = database.get_datasets(
                limit=limit, user_permissions=user_permissions
            )
            if not datasets:
                return []
            return datasets
        except Exception as e:
            self.logger.error(f"Error retrieving datasets: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to retrieve datasets. Contact support."
            )

    def get_dataset_table_list(
        self, dataset_id: str, bucket_type: VersionType, user: User
    ):
        """
        Get dataset table list with permission checks.
        """
        # TODO: Response should have table metadata as well.

        try:
            user_permissions = determine_user_permissions(user)
            dataset = database.get_dataset(dataset_id)
            if (
                bucket_type == VersionType.PREPROCESSED
                and not user_has_preprocessed_access(user_permissions)
            ):
                raise HTTPException(
                    status_code=403,
                    detail="You are not authorized to get preprocessed files",
                )

            if not user_has_dataset_download_access(user_permissions, dataset):
                raise HTTPException(
                    status_code=403,
                    detail="You are not authorized to get the dataset files",
                )

            files_list = self.filestore_service.list_files_in_s3(
                dataset_id, bucket_type
            )
            if not files_list:
                raise HTTPException(status_code=404, detail="No files found in bucket")
            return files_list

        except HTTPException as e:
            self.logger.error(f"Failed to get dataset files: {str(e)}")
            raise e
        except Exception as e:
            self.logger.error(f"Failed to get dataset files: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to get dataset files. Contact support."
            )

    def get_dataset_manifest(self, dataset_id: str, user: User):
        """
        Get the canonical standardised manifest with API-key auth and dataset view checks.
        """
        try:
            user_permissions = determine_user_permissions(user)
            dataset = database.get_dataset(dataset_id)
            if dataset is None:
                raise HTTPException(status_code=404, detail="Dataset not found")

            accessible_ids = {
                item.ds_id
                for item in database.get_datasets(limit=10000, user_permissions=user_permissions)
            }
            if dataset_id not in accessible_ids:
                raise HTTPException(
                    status_code=403,
                    detail="You are not authorized to view this dataset manifest",
                )

            manifest = self.filestore_service.get_manifest(
                dataset_id,
                VersionType.STANDARDISED,
            )
            if not manifest.get("has_manifest"):
                raise HTTPException(status_code=404, detail="Manifest not found")

            return {
                "dataset_id": dataset_id,
                "bucket_type": VersionType.STANDARDISED.value,
                **manifest,
                "manifest_updated_at": (
                    dataset.manifest_updated_at.isoformat()
                    if hasattr(dataset, "manifest_updated_at") and dataset.manifest_updated_at
                    else None
                ),
                "manifest_updated_by": (
                    dataset.manifest_updated_by
                    if hasattr(dataset, "manifest_updated_by")
                    else None
                ),
            }
        except HTTPException as e:
            self.logger.error(f"Failed to get dataset manifest: {e!s}")
            raise e
        except Exception as e:
            self.logger.error(f"Failed to get dataset manifest: {e!s}")
            raise HTTPException(
                status_code=500,
                detail="Failed to get dataset manifest. Contact support.",
            ) from e

    def get_children_regions(self, region_id: str, user: User):
        """
        Get all direct children regions for a given region_id.
        """
        try:
            children_regions = database.get_children_regions(region_id)

            if not children_regions:
                return []

            # Convert to response format
            children_list = [
                {
                    "region_id": region.region_id,
                    "region_name": region.region_name,
                    "parent_region_id": region.parent_region_id,
                }
                for region in children_regions
            ]

            return children_list
        except Exception as e:
            self.logger.error(f"Failed to get children regions: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to get children regions. Contact support.",
            )
