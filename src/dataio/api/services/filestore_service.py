import boto3
from botocore.client import Config
import dotenv
import os
from fastapi import UploadFile
from dataio.api.models import VersionType, TableMetadata
import json
from pathlib import Path
from dataio.api.services.base_service import BaseService

dotenv.load_dotenv()


class ValidationError(Exception):
    pass


class FilestoreService(BaseService):
    """Service for S3 filestore operations."""

    def __init__(self):
        super().__init__()
        self.session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self.s3 = self.session.resource("s3")
        self.s3_client = self.session.client(
            "s3", region_name="ap-south-1", config=Config(signature_version="s3v4")
        )
        self.bucket = self.s3.Bucket(os.getenv("AWS_BUCKET_NAME"))

    def _get_prefix_for_dataset(self, dataset_id: str, version_type: VersionType):
        return f"filestore/{version_type.value}/{dataset_id}"

    def _get_metadata_object(self, dataset_id: str, version_type: VersionType):
        prefix = self._get_prefix_for_dataset(dataset_id, version_type)
        try:
            obj = self.bucket.Object(f"{prefix}/metadata.json")
            return json.loads(obj.get()["Body"].read().decode("utf-8"))
        except self.s3_client.exceptions.NoSuchKey:
            self.bucket.put_object(
                Body=json.dumps({"tables": {}}),
                Key=f"{prefix}/metadata.json",
            )
            return {"tables": {}}

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
            self.logger.error(f"Validation error uploading file: {str(e)}")
            raise e
        except Exception as e:
            self.logger.error(f"Failed to upload file: {str(e)}")
            raise e

    def list_files_in_s3(self, dataset_id: str, version_type: VersionType):
        """
        List files in S3 bucket with metadata.
        """
        try:
            prefix = self._get_prefix_for_dataset(dataset_id, version_type)
            metadata_object = self._get_metadata_object(dataset_id, version_type)

            files_list = [
                obj.key.split("/")[-1]
                for obj in self.bucket.objects.filter(Prefix=prefix)
            ]
            return_json_list = []
            for file in files_list:
                if file == "metadata.json":
                    continue

                table_metadata = metadata_object["tables"][Path(file).stem]
                return_json = {}
                return_json["table_name"] = table_metadata.pop("table_name", None)
                download_link = self._get_download_link(dataset_id, version_type, file)
                return_json["download_link"] = download_link
                return_json["metadata"] = table_metadata
                return_json_list.append(return_json)
            return return_json_list
        except Exception as e:
            self.logger.error(f"Failed to list files: {str(e)}")
            raise e

    def delete_file(self, dataset_id: str, version_type: VersionType, file_name: str):
        """
        Delete a file from S3.
        """
        try:
            prefix = self._get_prefix_for_dataset(dataset_id, version_type)
            self.bucket.delete_objects(
                Delete={"Objects": [{"Key": f"{prefix}/{file_name}"}]}
            )
        except Exception as e:
            self.logger.error(f"Failed to delete file: {str(e)}")
            raise e

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
