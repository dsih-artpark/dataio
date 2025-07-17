from fastapi import HTTPException
from dataio.api.models import User, VersionType
from dataio.api.database import functions as database
from dataio.api.auth import (
    determine_user_permissions,
    user_has_preprocessed_access,
    user_has_dataset_download_access,
)
from .filestore_service import FilestoreService
from .base_service import BaseService


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
            print(datasets)
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
            # for permission in user_permissions:
            #     print(permission.__dict__)
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
