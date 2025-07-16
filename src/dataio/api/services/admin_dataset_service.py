from fastapi import HTTPException, UploadFile
from dataio.api.models import (
    RawDatasetCreate,
    DataOwnerCreate,
    CollectionCreate,
    DatasetCreate,
    VersionType,
    TableMetadata,
)
from dataio.api.database import functions as database
from .filestore_service import FilestoreService, ValidationError
from .base_service import BaseService


class AdminDatasetService(BaseService):
    """Service for admin dataset management operations."""

    def __init__(self):
        super().__init__()
        self.filestore_service = FilestoreService()

    def create_raw_dataset(self, raw_dataset: RawDatasetCreate):
        """
        Create a new raw dataset.

        EXACT BUSINESS LOGIC from admin.py:95-107
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

        EXACT BUSINESS LOGIC from admin.py:180-192
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

        EXACT BUSINESS LOGIC from admin.py:195-205
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

        EXACT BUSINESS LOGIC from admin.py:208-220
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

        EXACT BUSINESS LOGIC from admin.py:223-233
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

        EXACT BUSINESS LOGIC from admin.py:158-165
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

        EXACT BUSINESS LOGIC from admin.py:233-242
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
