import gzip

import yaml
from fastapi import HTTPException, UploadFile
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from dataio.api.database import functions as database
from dataio.api.database.config import engine
from dataio.api.models import (
    CollectionCreate,
    DataOwnerCreate,
    DatasetCreate,
    DatasetUpdate,
    RawDatasetCreate,
    RawDatasetUpdate,
    TableMetadata,
    VersionType,
)
from dataio.api.services.base_service import BaseService
from dataio.api.services.dataset_documentation_sync_service import (
    get_dataset_documentation_status,
    sync_dataset_documentation,
)
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
        self.db_session_factory = sessionmaker(bind=engine)

    def refresh_dataset_documentation_cache(self, dataset_id: str):
        session = self.db_session_factory()
        try:
            sync_dataset_documentation(
                session,
                self.filestore_service.bucket,
                dataset_id,
                force=True,
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

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

    def update_raw_dataset(self, raw_dataset_id: str, raw_dataset: RawDatasetUpdate):
        try:
            return database.update_raw_dataset(raw_dataset_id, raw_dataset)
        except ValueError as e:
            self.logger.error(f"Failed to update raw dataset: {e!s}")
            raise HTTPException(status_code=404, detail=str(e)) from e
        except Exception as e:
            self.logger.error(f"Failed to update raw dataset: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to update raw dataset. Contact support."
            ) from e

    def list_raw_datasets(self, search: str | None = None, limit: int = 100, offset: int = 0):
        try:
            raw_datasets, total = database.list_raw_datasets(search=search, limit=limit, offset=offset)
            return {
                "raw_datasets": [
                    {
                        "id": item.id,
                        "rds_id": item.rds_id,
                        "title": item.title,
                        "source": item.source,
                    }
                    for item in raw_datasets
                ],
                "total": total,
                "limit": limit,
                "offset": offset,
            }
        except Exception as e:
            self.logger.error(f"Failed to list raw datasets: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to list raw datasets. Contact support."
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

    def update_dataset(self, dataset_id: str, dataset: DatasetUpdate):
        try:
            updated_dataset = database.update_dataset(dataset_id, dataset)
            if dataset.ds_id and dataset.ds_id != dataset_id:
                self.filestore_service.rename_dataset(dataset_id, dataset.ds_id)
            return updated_dataset
        except ValidationError as e:
            self.logger.error(f"Failed to update dataset: {e!s}")
            raise HTTPException(status_code=400, detail=f"Validation error raised: {e}") from e
        except ValueError as e:
            self.logger.error(f"Failed to update dataset: {e!s}")
            raise HTTPException(status_code=400, detail=f"Value error raised: {e}") from e
        except Exception as e:
            self.logger.error(f"Failed to update dataset: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to update dataset. Contact support."
            ) from e

    def get_next_dataset_id_number(self):
        try:
            return {"next_number": database.get_next_dataset_serial_number()}
        except Exception as e:
            self.logger.error(f"Failed to get next dataset id number: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to get next dataset ID number. Contact support."
            ) from e

    def suggest_next_raw_dataset_id_for_category(self, category_id: str):
        try:
            if not category_id.strip():
                raise ValidationError("Category ID is required")
            return {
                "category_id": category_id,
                "suggested_raw_dataset_id": database.suggest_next_raw_dataset_id_for_category(category_id),
            }
        except ValidationError as e:
            self.logger.error(f"Failed to suggest raw dataset id for category: {e!s}")
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            self.logger.error(f"Failed to suggest raw dataset id for category: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to suggest raw dataset ID. Contact support."
            ) from e

    def suggest_next_dataset_id(self, collection_id: str):
        try:
            if not collection_id.strip():
                raise ValidationError("Collection ID is required")
            return {
                "collection_id": collection_id,
                "suggested_dataset_id": database.suggest_next_dataset_id(collection_id),
            }
        except ValidationError as e:
            self.logger.error(f"Failed to suggest dataset id: {e!s}")
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            self.logger.error(f"Failed to suggest dataset id: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to suggest dataset ID. Contact support."
            ) from e

    def suggest_next_raw_dataset_id(self, collection_id: str):
        try:
            if not collection_id.strip():
                raise ValidationError("Collection ID is required")
            return {
                "collection_id": collection_id,
                "suggested_raw_dataset_id": database.suggest_next_raw_dataset_id(collection_id),
            }
        except ValidationError as e:
            self.logger.error(f"Failed to suggest raw dataset id: {e!s}")
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            self.logger.error(f"Failed to suggest raw dataset id: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to suggest raw dataset ID. Contact support."
            ) from e

    def delete_dataset(self, dataset_id: str):
        try:
            if not database.check_if_dataset_exists(dataset_id):
                raise ValidationError("Dataset does not exist")
            database.delete_dataset(dataset_id)
            self.filestore_service.delete_dataset(dataset_id)
            return {"deleted": True, "dataset_id": dataset_id}
        except ValidationError as e:
            self.logger.error(f"Failed to delete dataset: {e!s}")
            raise HTTPException(status_code=404, detail=str(e)) from e
        except ValueError as e:
            self.logger.error(f"Failed to delete dataset: {e!s}")
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            self.logger.error(f"Failed to delete dataset: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to delete dataset. Contact support."
            ) from e

    def get_dataset_admin_detail(self, dataset_id: str):
        try:
            dataset = database.get_dataset(dataset_id)
            if dataset is None:
                raise ValidationError("Dataset does not exist")
            return {
                "ds_id": dataset.ds_id,
                "title": dataset.title,
                "collection_id": dataset.collection.collection_id if dataset.collection else None,
                "collection_name": dataset.collection.collection_name if dataset.collection else None,
                "data_owner_name": dataset.data_owner.name if dataset.data_owner else None,
                "description": dataset.description,
                "spatial_coverage_region_id": dataset.spatial_coverage_region_id,
                "spatial_resolution": dataset.spatial_resolution.value if dataset.spatial_resolution else None,
                "temporal_coverage_start_date": dataset.temporal_coverage_start_date.isoformat() if dataset.temporal_coverage_start_date else None,
                "temporal_coverage_end_date": dataset.temporal_coverage_end_date.isoformat() if dataset.temporal_coverage_end_date else None,
                "temporal_resolution": dataset.temporal_resolution.value if dataset.temporal_resolution else None,
                "access_level": dataset.access_level.value if dataset.access_level else None,
                "additional_metadata": dataset.additional_metadata,
                "tags": [tag.tag_name for tag in (dataset.tags or [])],
                "raw_dataset_ids": [raw_dataset.rds_id for raw_dataset in (dataset.raw_datasets or [])],
                "raw_datasets": [
                    {
                        "id": raw_dataset.id,
                        "rds_id": raw_dataset.rds_id,
                        "title": raw_dataset.title,
                        "source": raw_dataset.source,
                    }
                    for raw_dataset in (dataset.raw_datasets or [])
                ],
                "readme_md": dataset.readme_md,
                "data_dictionary_json": dataset.data_dictionary_json,
                "manifest_yaml": dataset.manifest_yaml,
                "manifest_json": dataset.manifest_json,
                "manifest_updated_at": dataset.manifest_updated_at.isoformat() if dataset.manifest_updated_at else None,
                "manifest_updated_by": dataset.manifest_updated_by,
                "documentation_synced_at": dataset.documentation_synced_at.isoformat() if dataset.documentation_synced_at else None,
            }
        except ValidationError as e:
            self.logger.error(f"Failed to get dataset detail: {e!s}")
            raise HTTPException(status_code=404, detail=str(e)) from e
        except Exception as e:
            self.logger.error(f"Failed to get dataset detail: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to get dataset detail. Contact support."
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
            self.refresh_dataset_documentation_cache(dataset_id)
            return {"message": "File uploaded successfully"}
        except ValidationError as e:
            self.logger.error(f"Failed to upload file: {e!s}")
            raise HTTPException(status_code=500, detail=f"Validation error raised: {e}") from e
        except Exception as e:
            self.logger.error(f"Failed to upload file: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to upload file. Contact support."
            ) from e

    def list_dataset_tables(self, dataset_id: str, bucket_type: VersionType):
        try:
            if not database.check_if_dataset_exists(dataset_id):
                raise ValidationError("Dataset does not exist")
            return {
                "dataset_id": dataset_id,
                "bucket_type": bucket_type.value,
                "tables": self.filestore_service.list_files_in_s3(dataset_id, bucket_type),
            }
        except ValidationError as e:
            self.logger.error(f"Failed to list dataset tables: {e!s}")
            raise HTTPException(status_code=404, detail=str(e)) from e
        except Exception as e:
            self.logger.error(f"Failed to list dataset tables: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to list dataset tables. Contact support."
            ) from e

    def _validate_and_persist_manifest(
        self,
        dataset_id: str,
        bucket_type: VersionType,
        manifest_text: str,
        parsed_manifest: dict,
        updated_by: str,
    ):
        """Validates a manifest against the dataset's stored data files,
        then writes it to filestore + the DB manifest cache and refreshes
        doc-sync. Raises ValidationError/HTTPException on failure.

        Used by the direct manifest-upload endpoint. An LLM draft that has
        been through DraftReviewService.approve_draft has NOT gone through
        this method yet - approval only flips the draft's status. A curator
        must download the approved draft_yaml and re-upload it through the
        normal manifest-upload flow (which does call this) before it's
        actually validated and persisted. If the draft-approval path is ever
        wired to call this directly instead, this docstring should say so.
        """
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
        self.refresh_dataset_documentation_cache(dataset_id)
        return {
            "message": "Manifest uploaded successfully",
            "dataset_id": dataset_id,
            "bucket_type": bucket_type.value,
            "manifest_json": parsed_manifest,
        }

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

            return self._validate_and_persist_manifest(
                dataset_id, bucket_type, manifest_text, parsed_manifest, updated_by
            )
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

    def check_dataset_documentation_sync(self, dataset_id: str | None = None, *, check_all: bool = False):
        """Interactive per-dataset check by default - a curator picks one
        dataset in the sync workspace and checks just that one. `check_all`
        is a separate opt-in for summary use (e.g. the admin overview's
        outdated-docs count), where checking every dataset at once is the
        whole point rather than an accident of a missing dataset_id.
        """
        session = self.db_session_factory()
        try:
            if dataset_id is None and not check_all:
                raise ValidationError(
                    "Dataset ID is required for interactive documentation sync"
                )
            if dataset_id:
                dataset_ids = [dataset_id]
            else:
                rows = session.execute(text("SELECT ds_id FROM datasets ORDER BY ds_id")).all()
                dataset_ids = [row[0] for row in rows]

            results = []
            outdated = 0
            for current_dataset_id in dataset_ids:
                status = get_dataset_documentation_status(
                    session,
                    self.filestore_service.bucket,
                    current_dataset_id,
                )
                results.append(
                    {
                        "ds_id": current_dataset_id,
                        "needs_update": status["needs_update"],
                        "changed_fields": status["changed_fields"],
                        "has_remote_documentation": status["has_remote_documentation"],
                        "manifest_updated_at": status["manifest_updated_at"],
                        "documentation_synced_at": status["documentation_synced_at"],
                    }
                )
                if status["needs_update"]:
                    outdated += 1

            return {
                "datasets": results,
                "total": len(results),
                "outdated": outdated,
            }
        except (ValueError, ValidationError) as e:
            session.rollback()
            self.logger.error(f"Failed to check documentation sync: {e!s}")
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to check documentation sync: {e!s}")
            raise HTTPException(
                status_code=500,
                detail="Failed to check dataset documentation sync. Contact support.",
            ) from e
        finally:
            session.close()

    def sync_dataset_documentation(
        self,
        dataset_id: str | None = None,
        *,
        only_outdated: bool = True,
        force: bool = False,
    ):
        session = self.db_session_factory()
        try:
            if dataset_id is None:
                raise ValidationError(
                    "Dataset ID is required for interactive documentation sync"
                )
            if dataset_id:
                dataset_ids = [dataset_id]
            else:
                rows = session.execute(text("SELECT ds_id FROM datasets ORDER BY ds_id")).all()
                dataset_ids = [row[0] for row in rows]

            results = []
            updated = 0
            for current_dataset_id in dataset_ids:
                status = get_dataset_documentation_status(
                    session,
                    self.filestore_service.bucket,
                    current_dataset_id,
                )
                if only_outdated and not force and not status["needs_update"]:
                    results.append(
                        {
                            "ds_id": current_dataset_id,
                            "changed_fields": status["changed_fields"],
                            "needs_update": False,
                            "updated": False,
                            "skipped": True,
                        }
                    )
                    continue

                result = sync_dataset_documentation(
                    session,
                    self.filestore_service.bucket,
                    current_dataset_id,
                    force=force,
                )
                result["skipped"] = False
                results.append(result)
                if result["updated"]:
                    updated += 1

            return {
                "datasets": results,
                "total": len(results),
                "updated": updated,
            }
        except (ValueError, ValidationError) as e:
            session.rollback()
            self.logger.error(f"Failed to sync documentation: {e!s}")
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to sync documentation: {e!s}")
            raise HTTPException(
                status_code=500,
                detail="Failed to sync dataset documentation. Contact support.",
            ) from e
        finally:
            session.close()

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
