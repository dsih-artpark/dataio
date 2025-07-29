import json
from fastapi import HTTPException
import gzip
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

    def get_shapefile(self, region_id: str, user_email: str):
        try:
            parent_id = database.get_parentID_of_region(region_id)
            if database.check_rate_limit_exceeded(user_email, "shapefile"):
                raise HTTPException(
                    status_code=429,
                    detail="You have reached the maximum number of requests. Please try again later.",
                )
            database.update_shapefile_download_count(user_email)
            compressed_shapefile_geojson = self.filestore_service.get_shapefile(
                region_id, parent_id
            )
            shapefile_geojson = gzip.decompress(compressed_shapefile_geojson)
            shapefile_geojson = json.loads(shapefile_geojson)
            return shapefile_geojson
        except Exception as e:
            self.logger.error(f"Failed to get shapefile: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to get shapefile. Contact support."
            )
