import json
from fastapi import HTTPException
import gzip
from botocore.exceptions import ClientError
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
            # check if region id is valid & whether we have it
            parent_id = database.get_parentID_of_region(region_id)

            if parent_id is None:
                raise HTTPException(status_code=400, detail="Invalid region id")

            if database.check_rate_limit_exceeded(user_email, "shapefile"):
                raise HTTPException(
                    status_code=429,
                    detail="You have reached the maximum number of requests. Please try again later.",
                )
            database.update_shapefile_rate_limit(user_email)
            compressed_shapefile_geojson = self.filestore_service.get_shapefile(
                region_id, parent_id
            )
            # shapefile_geojson = gzip.decompress(compressed_shapefile_geojson)
            # shapefile_geojson = json.loads(shapefile_geojson)
            return compressed_shapefile_geojson
        except HTTPException as e:
            self.logger.error(f"Failed to get shapefile: {str(e)}")
            raise e
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                self.logger.error("Shapefile not present in S3")
                raise HTTPException(
                    status_code=500,
                    detail="Shapefile not present in our data-store. Please check shapefile list for available shapefiles.",
                )
        except Exception as e:
            self.logger.error(f"Failed to get shapefile: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to get shapefile. Contact support."
            )

    def get_shapefiles_list(self, user: User):
        """
        Get list of available shapefiles from S3 with region names.
        """
        try:
            shapefiles_list = self.filestore_service.list_shapefiles()

            # Get unique region_ids to fetch region names
            region_ids = [shapefile["region_id"] for shapefile in shapefiles_list]
            if not region_ids:
                return []

            # Get region names from database
            region_names = database.get_regions_by_ids(region_ids)

            # Add region names to the shapefile info
            for shapefile in shapefiles_list:
                shapefile["region_name"] = region_names.get(
                    shapefile["region_id"], "Unknown"
                )

            # setting order of fields

            shapefiles_list = [
                {
                    "region_id": shapefile["region_id"],
                    "region_name": shapefile["region_name"],
                    "parent_id": shapefile["parent_id"],
                    "last_modified": shapefile["last_modified"],
                }
                for shapefile in shapefiles_list
            ]

            return shapefiles_list
        except Exception as e:
            self.logger.error(f"Failed to get shapefiles list: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to get shapefiles list. Contact support.",
            )
