from fastapi import HTTPException, UploadFile
from typing import List
from dataio.api.models import User, DatasetCreate, VersionType, TableMetadata
from dataio.api.database import functions as database
from dataio.api.auth import (
    determine_user_permissions,
    user_has_preprocessed_access,
    user_has_dataset_download_access,
)
from dataio.api.filestore import DatasetS3, ValidationError
from .base_service import BaseService


class DatasetService(BaseService):
    """Service for dataset-related operations."""

    def get_user_datasets(self, user: User, limit: int = 100):
        """
        Get datasets for a user with permissions applied.

        EXACT BUSINESS LOGIC from user.py:35-46
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

        EXACT BUSINESS LOGIC from user.py:58-91
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

            dataset_s3 = DatasetS3(dataset_id, bucket_type)
            files_list = dataset_s3.list_files_in_s3()
            if not files_list:
                raise HTTPException(status_code=404, detail="No files found in bucket")
            return files_list

        except HTTPException as e:
            self.logger.error(f"Failed to get dataset files: {str(e)}")
            raise e
        except Exception as e:
            self.logger.error(f"Failed to get dataset files: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Failed to get dataset files. Contact support."
            )

    def create_dataset(self, dataset: DatasetCreate):
        """
        Create a new dataset.

        EXACT BUSINESS LOGIC from admin.py:158-165
        """
        try:
            created_dataset = database.create_dataset(dataset)
            return created_dataset
        except Exception as e:
            self.logger.error(f"Error creating dataset: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to create dataset. Contact support."
            )

    def create_dataset_table(
        self,
        dataset_id: str,
        bucket_type: VersionType,
        file: UploadFile,
        table_metadata_file: UploadFile,
    ):
        """
        Create/upload a dataset table.

        EXACT BUSINESS LOGIC from admin.py:201-220
        """
        # Table metadata should also be provided
        try:
            # Check if dataset exists

            if not database.check_if_dataset_exists(dataset_id):
                raise ValidationError("Dataset does not exist")

            table_metadata = TableMetadata.model_validate_json(
                table_metadata_file.file.read()
            )
            dataset_s3 = DatasetS3(dataset_id, bucket_type)
            dataset_s3.upload_file(file, table_metadata)
            return {"message": "File uploaded successfully"}
        except ValidationError as e:
            self.logger.error(f"Failed to upload file: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Validation error raised: {e}")
        except Exception as e:
            self.logger.error(f"Failed to upload file: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to upload file. Contact support."
            )

    def delete_dataset_table(
        self, dataset_id: str, bucket_type: VersionType, table_name: str
    ):
        """
        Delete a dataset table.

        EXACT BUSINESS LOGIC from admin.py:233-242
        """
        try:
            dataset_s3 = DatasetS3(dataset_id, bucket_type)
            dataset_s3.delete_file(table_name)
            return {"message": "File deleted successfully"}
        except Exception as e:
            self.logger.error(f"Failed to delete dataset version file: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete dataset version file. Contact support.",
            )
