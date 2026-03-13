import gzip

import yaml
from fastapi import HTTPException, UploadFile

from dataio.api.database import functions as database
from dataio.api.models import (
    CollectionCreate,
    DataOwnerCreate,
    DatasetCreate,
    RawDatasetCreate,
    TableMetadata,
    VersionType,
)
from dataio.api.services.base_service import BaseService
from dataio.api.services.filestore_service import FilestoreService, ValidationError
from dataio.api.services.platform_manifest_validation_service import (
    apply_platform_manifest_checks,
)
from dataio.validate import DataIOValidationService, DatasetKind, ValidationRequest


class AdminDatasetService(BaseService):
    """Service for admin dataset management operations."""

    def __init__(self):
        super().__init__()
        self.filestore_service = FilestoreService()
        self.validation_service = DataIOValidationService(
            platform_manifest_checker=apply_platform_manifest_checks
        )

    def create_raw_dataset(self, raw_dataset: RawDatasetCreate):
        """
        Create a new raw dataset.
        """
        try:
            created_raw_dataset = database.create_raw_dataset(raw_dataset)
            return created_raw_dataset
        except Exception as e:
            self.logger.error(f"Failed to create raw dataset: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to create raw dataset. Contact support."
            ) from e

    def create_data_owner(self, data_owner: DataOwnerCreate):
        """
        Create a new data owner.
        """
        try:
            created_data_owner = database.create_data_owner(data_owner)
            return created_data_owner
        except Exception as e:
            self.logger.error(f"Failed to create data owner: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to create data owner. Contact support."
            ) from e

    def get_data_owners(self):
        """
        Get all data owners.
        """
        try:
            data_owners = database.get_data_owners()
            return data_owners
        except Exception as e:
            self.logger.error(f"Failed to get data owners: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to get data owners. Contact support."
            ) from e

    def create_collection(self, collection: CollectionCreate):
        """
        Create a new collection.
        """
        try:
            created_collection = database.create_collection(collection)
            return created_collection
        except Exception as e:
            self.logger.error(f"Failed to create collection: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to create collection. Contact support."
            ) from e

    def get_collections(self):
        """
        Get all collections.
        """
        try:
            collections = database.get_collections()
            return collections
        except Exception as e:
            self.logger.error(f"Failed to get collections: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to get collections. Contact support."
            ) from e

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
            self.logger.error(f"Failed to create dataset: {e!s}")
            raise HTTPException(status_code=400, detail=f"Validation error raised: {e}") from e
        except ValueError as e:
            self.logger.error(f"Failed to create dataset: {e!s}")
            raise HTTPException(status_code=400, detail=f"Value error raised: {e}") from e
        except Exception as e:
            self.logger.error(f"Error creating dataset: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to create dataset. Contact support."
            ) from e

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
            self.logger.error(f"Failed to upload file: {e!s}")
            raise HTTPException(status_code=500, detail=f"Validation error raised: {e}") from e
        except Exception as e:
            self.logger.error(f"Failed to upload file: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to upload file. Contact support."
            ) from e

    def upsert_dataset_manifest(
        self,
        dataset_id: str,
        bucket_type: VersionType,
        manifest_file: UploadFile,
        updated_by: str,
    ):
        try:
            if not database.check_if_dataset_exists(dataset_id):
                raise ValidationError("Dataset does not exist")

            manifest_text = manifest_file.file.read().decode("utf-8")
            parsed_manifest = yaml.safe_load(manifest_text)
            if not isinstance(parsed_manifest, dict):
                raise ValidationError("Manifest must deserialize to an object")

            dataset_kind = parsed_manifest.get("datasetKind")
            if not dataset_kind:
                raise ValidationError("Manifest must define datasetKind")
            try:
                dataset_kind_enum = DatasetKind(dataset_kind)
            except ValueError as e:
                raise ValidationError(f"Unsupported datasetKind '{dataset_kind}'") from e

            validation_request = ValidationRequest(
                dataset_kind=dataset_kind_enum,
                manifest_source=manifest_text,
                validate_data=True,
            )
            if dataset_kind_enum == DatasetKind.TABULAR:
                validation_request.data_files = (
                    self.filestore_service.get_tabular_validation_sources(
                        dataset_id,
                        bucket_type,
                    )
                )
                if not validation_request.data_files:
                    raise ValidationError(
                        "Cannot upload manifest until tabular data files exist in filestore"
                    )
            elif dataset_kind_enum == DatasetKind.GEOJSON:
                validation_request.data = self.filestore_service.get_geojson_validation_source(
                    dataset_id,
                    bucket_type,
                )

            validation_result = self.validation_service.validate(validation_request)
            if validation_result.status == "fail":
                raise HTTPException(
                    status_code=400,
                    detail={
                        "message": "Manifest and stored data validation failed",
                        "findings": [
                            finding.model_dump() for finding in validation_result.findings
                        ],
                    },
                )

            self.filestore_service.upload_manifest(
                dataset_id=dataset_id,
                version_type=bucket_type,
                manifest_yaml=manifest_text,
                manifest_json=parsed_manifest,
            )
            database.update_dataset_manifest_cache(
                dataset_id,
                manifest_yaml=manifest_text,
                manifest_json=parsed_manifest,
                updated_by=updated_by,
            )
            return {
                "message": "Manifest uploaded successfully",
                "dataset_id": dataset_id,
                "bucket_type": bucket_type.value,
                "manifest_json": parsed_manifest,
            }
        except HTTPException:
            raise
        except ValidationError as e:
            self.logger.error(f"Failed to upload manifest: {e!s}")
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            self.logger.error(f"Failed to upload manifest: {e!s}")
            raise HTTPException(
                status_code=500,
                detail="Failed to upload manifest. Contact support.",
            ) from e

    def get_dataset_manifest(self, dataset_id: str, bucket_type: VersionType):
        try:
            if not database.check_if_dataset_exists(dataset_id):
                raise ValidationError("Dataset does not exist")
            return self.filestore_service.get_manifest(dataset_id, bucket_type)
        except ValidationError as e:
            self.logger.error(f"Failed to get manifest: {e!s}")
            raise HTTPException(status_code=404, detail=str(e)) from e
        except Exception as e:
            self.logger.error(f"Failed to get manifest: {e!s}")
            raise HTTPException(
                status_code=500,
                detail="Failed to get manifest. Contact support.",
            ) from e

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
            self.logger.error(f"Failed to delete dataset version file: {e!s}")
            raise HTTPException(
                status_code=500,
                detail="Failed to delete dataset version file. Contact support.",
            ) from e

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
        except Exception:
            self.logger.error("Failed to upload shapefile.")
            raise HTTPException(status_code=500, detail="Failed to upload shape file.") from None
