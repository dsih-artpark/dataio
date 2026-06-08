import json
import os
from pathlib import Path

import boto3
import dotenv
from botocore.client import Config
from botocore.exceptions import ClientError
from fastapi import UploadFile

from dataio.api.models import TableMetadata, VersionType
from dataio.api.services.base_service import BaseService, get_aws_access_key_id

dotenv.load_dotenv()


class ValidationError(Exception):
    pass


class FilestoreService(BaseService):
    """Service for S3 filestore operations."""

    def __init__(self):
        super().__init__()
        self.session = boto3.Session(
            aws_access_key_id=get_aws_access_key_id(),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self.s3 = self.session.resource("s3")
        self.s3_client = self.session.client(
            "s3", region_name="ap-south-1", config=Config(signature_version="s3v4")
        )
        self.bucket = self.s3.Bucket(os.getenv("AWS_BUCKET_NAME"))

    def _get_prefix_for_dataset(self, dataset_id: str, version_type: VersionType):
        return f"filestore/{version_type.value}/{dataset_id}"

    def _object_exists(self, key: str) -> bool:
        try:
            self.bucket.Object(key).load()
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
                return False
            raise

    def _manifest_yaml_key(self, dataset_id: str, version_type: VersionType) -> str:
        return f"{self._get_prefix_for_dataset(dataset_id, version_type)}/manifest.yaml"

    def _manifest_json_key(self, dataset_id: str, version_type: VersionType) -> str:
        return f"{self._get_prefix_for_dataset(dataset_id, version_type)}/manifest.json"

    def _documentation_keys(self, dataset_id: str, filename: str) -> list[str]:
        keys = [
            f"{self._get_prefix_for_dataset(dataset_id, version_type)}/{filename}"
            for version_type in (VersionType.STANDARDISED, VersionType.PREPROCESSED)
            if self._object_exists(f"{self._get_prefix_for_dataset(dataset_id, version_type)}/{filename}")
        ]
        if keys:
            return keys
        return [
            f"{self._get_prefix_for_dataset(dataset_id, VersionType.STANDARDISED)}/{filename}"
        ]

    def _list_dataset_objects(self, dataset_id: str, version_type: VersionType) -> list[str]:
        prefix = self._get_prefix_for_dataset(dataset_id, version_type)
        return [
            obj.key
            for obj in self.bucket.objects.filter(Prefix=prefix)
            if not obj.key.endswith("/")
        ]

    def _move_dataset_objects(
        self,
        old_dataset_id: str,
        new_dataset_id: str,
        version_type: VersionType,
    ) -> None:
        old_prefix = self._get_prefix_for_dataset(old_dataset_id, version_type)
        new_prefix = self._get_prefix_for_dataset(new_dataset_id, version_type)
        for key in self._list_dataset_objects(old_dataset_id, version_type):
            new_key = key.replace(old_prefix, new_prefix, 1)
            self.bucket.copy({"Bucket": self.bucket.name, "Key": key}, new_key)
            self.bucket.delete_objects(Delete={"Objects": [{"Key": key}]})

    def _get_metadata_object(self, dataset_id: str, version_type: VersionType):
        prefix = self._get_prefix_for_dataset(dataset_id, version_type)
        try:
            obj = self.bucket.Object(f"{prefix}/metadata.json")
            return json.loads(obj.get()["Body"].read().decode("utf-8"))
        except ClientError as e:
            # Handle NoSuchKey error - create empty metadata if file doesn't exist
            if e.response.get("Error", {}).get("Code") == "NoSuchKey":
                self.logger.info(
                    "No metadata.json found for %s/%s, creating empty one",
                    dataset_id,
                    version_type.value,
                )
                self.bucket.put_object(
                    Body=json.dumps({"tables": {}}),
                    Key=f"{prefix}/metadata.json",
                )
                return {"tables": {}}
            # Re-raise other errors
            raise

    def upload_file(
        self,
        dataset_id: str,
        version_type: VersionType,
        file: UploadFile,
        table_metadata: TableMetadata,
    ):
        """
        Upload a file to S3 with metadata.
        """
        try:
            prefix = self._get_prefix_for_dataset(dataset_id, version_type)
            metadata_object = self._get_metadata_object(dataset_id, version_type)

            # validation checks
            if Path(file.filename).stem != table_metadata.table_name:
                raise ValidationError(
                    "table_name in metadata and filename are not matching!"
                )

            # check if table of same name already exists
            if table_metadata.table_name in metadata_object["tables"]:
                raise ValidationError("table of same name already exists!")

            remote_filepath = f"{prefix}/{os.path.basename(file.filename)}"
            metadata_object["tables"][table_metadata.table_name] = (
                table_metadata.model_dump()
            )
            self.bucket.upload_fileobj(file.file, remote_filepath)
            self.bucket.put_object(
                Body=json.dumps(metadata_object).encode("UTF-8"),
                Key=f"{prefix}/metadata.json",
            )
        except ValidationError as e:
            self.logger.error(f"Validation error uploading file: {e!s}")
            raise e
        except Exception as e:
            self.logger.error(f"Failed to upload file: {e!s}")
            raise e

    def upload_manifest(
        self,
        dataset_id: str,
        version_type: VersionType,
        manifest_yaml: str,
        manifest_json: dict,
    ) -> None:
        self.bucket.put_object(
            Body=manifest_yaml.encode("utf-8"),
            Key=self._manifest_yaml_key(dataset_id, version_type),
            ContentType="application/x-yaml",
        )
        self.bucket.put_object(
            Body=json.dumps(manifest_json).encode("utf-8"),
            Key=self._manifest_json_key(dataset_id, version_type),
            ContentType="application/json",
        )

    def get_manifest(self, dataset_id: str, version_type: VersionType) -> dict:
        manifest_yaml = None
        manifest_json = None
        try:
            manifest_yaml = (
                self.bucket.Object(self._manifest_yaml_key(dataset_id, version_type))
                .get()["Body"]
                .read()
                .decode("utf-8")
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "NoSuchKey":
                raise

        try:
            manifest_json = json.loads(
                self.bucket.Object(self._manifest_json_key(dataset_id, version_type))
                .get()["Body"]
                .read()
                .decode("utf-8")
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "NoSuchKey":
                raise

        return {
            "manifest_yaml": manifest_yaml,
            "manifest_json": manifest_json,
            "has_manifest": manifest_yaml is not None or manifest_json is not None,
        }

    def upsert_dataset_readme(self, dataset_id: str, readme_md: str | None) -> None:
        keys = self._documentation_keys(dataset_id, "README.md")
        if readme_md is None:
            delete_objects = [{"Key": key} for key in keys]
            if delete_objects:
                self.bucket.delete_objects(Delete={"Objects": delete_objects})
            return

        for key in keys:
            self.bucket.put_object(
                Body=readme_md.encode("utf-8"),
                Key=key,
                ContentType="text/markdown; charset=utf-8",
            )

    def upsert_dataset_metadata_json(
        self, dataset_id: str, metadata_json: dict | list | None
    ) -> None:
        keys = self._documentation_keys(dataset_id, "metadata.json")
        if metadata_json is None:
            delete_objects = [{"Key": key} for key in keys]
            if delete_objects:
                self.bucket.delete_objects(Delete={"Objects": delete_objects})
            return

        payload = json.dumps(metadata_json, indent=2, sort_keys=True).encode("utf-8")
        for key in keys:
            self.bucket.put_object(
                Body=payload,
                Key=key,
                ContentType="application/json",
            )

    def get_tabular_validation_sources(
        self,
        dataset_id: str,
        version_type: VersionType,
    ) -> dict[str, str]:
        table_sources: dict[str, str] = {}
        for key in self._list_dataset_objects(dataset_id, version_type):
            file_name = Path(key).name
            if file_name in {"metadata.json", "manifest.yaml", "manifest.json"}:
                continue
            if Path(file_name).suffix.lower() != ".csv":
                continue

            table_sources[Path(file_name).stem] = (
                self.bucket.Object(key).get()["Body"].read().decode("utf-8")
            )
        return table_sources

    def get_geojson_validation_source(
        self,
        dataset_id: str,
        version_type: VersionType,
    ) -> str:
        candidate_keys = []
        for key in self._list_dataset_objects(dataset_id, version_type):
            file_name = Path(key).name
            if file_name in {"metadata.json", "manifest.yaml", "manifest.json"}:
                continue
            if Path(file_name).suffix.lower() not in {".geojson", ".json"}:
                continue
            candidate_keys.append(key)

        if not candidate_keys:
            raise ValidationError("No stored GeoJSON data found for dataset")
        if len(candidate_keys) > 1:
            raise ValidationError(
                "Multiple stored GeoJSON files found; unable to determine canonical source"
            )

        return self.bucket.Object(candidate_keys[0]).get()["Body"].read().decode("utf-8")

    def list_files_in_s3(self, dataset_id: str, version_type: VersionType):
        """
        List files in S3 bucket with metadata.
        """
        try:
            metadata_object = self._get_metadata_object(dataset_id, version_type)

            files_list = [
                Path(key).name
                for key in self._list_dataset_objects(dataset_id, version_type)
            ]
            self.logger.info(
                "Found %s files in S3 for %s/%s: %s",
                len(files_list),
                dataset_id,
                version_type.value,
                files_list,
            )

            return_json_list = []
            for file in files_list:
                if file == "metadata.json":
                    continue

                file_stem = Path(file).stem
                # Handle case where file exists in S3 but not in metadata
                if file_stem not in metadata_object.get("tables", {}):
                    self.logger.warning(
                        "File %s exists in S3 but not in metadata.json for %s",
                        file,
                        dataset_id,
                    )
                    # Still include the file with minimal metadata
                    return_json = {
                        "table_name": file_stem,
                        "download_link": self._get_download_link(dataset_id, version_type, file),
                        "metadata": {},
                    }
                    return_json_list.append(return_json)
                    continue

                # Make a copy to avoid mutating the original
                table_metadata = dict(metadata_object["tables"][file_stem])
                return_json = {}
                return_json["table_name"] = table_metadata.pop("table_name", file_stem)
                download_link = self._get_download_link(dataset_id, version_type, file)
                return_json["download_link"] = download_link
                return_json["metadata"] = table_metadata
                return_json_list.append(return_json)

            self.logger.info(f"Returning {len(return_json_list)} tables for {dataset_id}")
            return return_json_list
        except Exception as e:
            self.logger.error(f"Failed to list files for {dataset_id}: {e!s}", exc_info=True)
            raise e

    def delete_file(self, dataset_id: str, version_type: VersionType, file_name: str):
        """
        Delete a file from S3.
        """
        try:
            prefix = self._get_prefix_for_dataset(dataset_id, version_type)
            metadata_object = self._get_metadata_object(dataset_id, version_type)
            metadata_object["tables"].pop(file_name)
            self.bucket.put_object(
                Body=json.dumps(metadata_object).encode("UTF-8"),
                Key=f"{prefix}/metadata.json",
            )
            self.bucket.delete_objects(
                Delete={"Objects": [{"Key": f"{prefix}/{file_name + '.csv'}"}]}
            )
        except Exception as e:
            self.logger.error(f"Failed to delete file: {e!s}")
            raise e

    def rename_dataset(self, old_dataset_id: str, new_dataset_id: str) -> None:
        for version_type in (VersionType.STANDARDISED, VersionType.PREPROCESSED):
            self._move_dataset_objects(old_dataset_id, new_dataset_id, version_type)

    def delete_dataset(self, dataset_id: str) -> None:
        keys_to_delete = []
        for version_type in (VersionType.STANDARDISED, VersionType.PREPROCESSED):
            for key in self._list_dataset_objects(dataset_id, version_type):
                keys_to_delete.append({"Key": key})
        if keys_to_delete:
            self.bucket.delete_objects(Delete={"Objects": keys_to_delete})

    def _get_download_link(
        self, dataset_id: str, version_type: VersionType, file_name: str
    ):
        """
        Generate a presigned download link.
        """
        prefix = self._get_prefix_for_dataset(dataset_id, version_type)
        download_link = self.s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket.name, "Key": f"{prefix}/{file_name}"},
            ExpiresIn=3600,
        )
        return download_link

    def upload_shapefile(self, file: bytes, region_id: str, parent_id: str):
        """
        Upload shapefile to S3
        """
        self.bucket.put_object(
            Body=file,
            Key=f"shapefiles/{parent_id}/{region_id}.geojson.gz",
        )

    def get_shapefile(self, region_id: str, parent_id: str):
        """
        Get shapefile from S3
        """
        return (
            self.bucket.Object(f"shapefiles/{parent_id}/{region_id}.geojson.gz")
            .get()["Body"]
            .read()
        )

    def list_shapefiles(self):
        """
        List all available shapefiles from S3 shapefiles/ prefix.
        Returns organized data with parent_id and region_id information.
        """
        try:
            shapefiles_list = []

            for obj in self.bucket.objects.filter(Prefix="shapefiles/"):
                if obj.key.endswith(".geojson.gz"):
                    # Parse the key structure: shapefiles/{parent_id}/{region_id}.geojson.gz
                    key_parts = obj.key.split("/")
                    if len(key_parts) >= 3:
                        parent_id = key_parts[1]
                        region_filename = key_parts[2]
                        region_id = region_filename.replace(".geojson.gz", "")

                        shapefile_info = {
                            "region_id": region_id,
                            "parent_id": parent_id,
                            "last_modified": obj.last_modified.isoformat()
                            if obj.last_modified
                            else None,
                        }
                        shapefiles_list.append(shapefile_info)

            return shapefiles_list
        except Exception as e:
            self.logger.error(f"Failed to list shapefiles: {e!s}")
            raise e
