from fastapi import HTTPException, UploadFile
import gzip
from dataio.api.models import (
    RawDatasetCreate,
    DataOwnerCreate,
    CollectionCreate,
    DatasetCreate,
    VersionType,
    TableMetadata,
)
from dataio.api.database import functions as database
from dataio.api.services.filestore_service import FilestoreService, ValidationError
from dataio.api.services.base_service import BaseService


class AdminDatasetService(BaseService):
    """Service for admin dataset management operations."""

    def __init__(self):
        super().__init__()
        self.filestore_service = FilestoreService()

    def create_raw_dataset(self, raw_dataset: RawDatasetCreate):
        """
        Create a new raw dataset.
        """
        try:
            created_raw_dataset = database.create_raw_dataset(raw_dataset)
            return created_raw_dataset
        except Exception as e:
            self.logger.error(f"Failed to create raw dataset: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to create raw dataset. Contact support."
            )

    def create_data_owner(self, data_owner: DataOwnerCreate):
        """
        Create a new data owner.
        """
        try:
            created_data_owner = database.create_data_owner(data_owner)
            return created_data_owner
        except Exception as e:
            self.logger.error(f"Failed to create data owner: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to create data owner. Contact support."
            )

    def get_data_owners(self):
        """
        Get all data owners.
        """
        try:
            data_owners = database.get_data_owners()
            return data_owners
        except Exception as e:
            self.logger.error(f"Failed to get data owners: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to get data owners. Contact support."
            )

    def create_collection(self, collection: CollectionCreate):
        """
        Create a new collection.
        """
        try:
            created_collection = database.create_collection(collection)
            return created_collection
        except Exception as e:
            self.logger.error(f"Failed to create collection: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to create collection. Contact support."
            )

    def get_collections(self):
        """
        Get all collections.
        """
        try:
            collections = database.get_collections()
            return collections
        except Exception as e:
            self.logger.error(f"Failed to get collections: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to get collections. Contact support."
            )

    def create_dataset(self, dataset: DatasetCreate):
        """
        Create a new dataset.
        """
        try:
            if not dataset.ds_id[:6] == dataset.collection_id:
                raise ValidationError("Dataset ID must start with collection ID")
            if not len(dataset.ds_id) == 12:
                raise ValidationError("Dataset ID must be 12 characters long")
            created_dataset = database.create_dataset(dataset)
            return created_dataset
        except ValidationError as e:
            self.logger.error(f"Failed to create dataset: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Validation error raised: {e}")
        except ValueError as e:
            self.logger.error(f"Failed to create dataset: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Value error raised: {e}")
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
        """
        # Table metadata should also be provided
        try:
            # Check if dataset exists
            if not database.check_if_dataset_exists(dataset_id):
                raise ValidationError("Dataset does not exist")

            table_metadata = TableMetadata.model_validate_json(
                table_metadata_file.file.read()
            )
            self.filestore_service.upload_file(
                dataset_id, bucket_type, file, table_metadata
            )
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
        """
        try:
            self.filestore_service.delete_file(dataset_id, bucket_type, table_name)
            return {"message": "File deleted successfully"}
        except Exception as e:
            self.logger.error(f"Failed to delete dataset version file: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to delete dataset version file. Contact support.",
            )

    def upload_shapefile(self, file: UploadFile, region_id: str):
        """
        Compress and upload shapefile to S3
        """
        try:
            file_contents = file.file.read()
            compressed_contents = gzip.compress(file_contents)
            parent_id = database.get_parentID_of_region(region_id)
            self.filestore_service.upload_shapefile(
                compressed_contents, region_id, parent_id
            )
        except Exception as e:
            self.logger.error("Failed to upload shapefile.")
            raise HTTPException(status_code=500, detail="Failed to upload shape file.")
