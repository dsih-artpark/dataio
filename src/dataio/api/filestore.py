import boto3
from botocore.client import Config
import dotenv
import os
from fastapi import UploadFile
from dataio.api.models import VersionType, TableMetadata
import json
import logging
from pathlib import Path

dotenv.load_dotenv()

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    pass


class DatasetS3:
    def __init__(self, dataset_id: str, type: VersionType):
        self.dataset_id = dataset_id
        self.type = type

        self.session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

        self.s3 = self.session.resource("s3")
        self.s3_client = self.session.client(
            "s3", region_name="ap-south-1", config=Config(signature_version="s3v4")
        )

        self.bucket = self.s3.Bucket(os.getenv("AWS_BUCKET_NAME"))

        self.prefix = self._get_prefix_for_dataset(self.dataset_id)
        self.metadata_object = self._get_metadata_object()

    def _get_prefix_for_dataset(self, dataset_id: str):
        return f"filestore/{self.type.value}/{dataset_id}"

    def _get_metadata_object(self):
        try:
            obj = self.bucket.Object(f"{self.prefix}/metadata.json")
            return json.loads(obj.get()["Body"].read().decode("utf-8"))
        except self.s3_client.exceptions.NoSuchKey:
            self.bucket.put_object(
                Body=json.dumps({"tables": {}}),
                Key=f"{self.prefix}/metadata.json",
            )
            return {"tables": {}}

    def upload_file(self, file: UploadFile, table_metadata: TableMetadata):
        try:
            # validation checks

            if Path(file.filename).stem != table_metadata.table_name:
                raise ValidationError(
                    "table_name in metadata and filename are not matching!"
                )

            remote_filepath = f"{self.prefix}/{os.path.basename(file.filename)}"
            self.metadata_object["tables"][table_metadata.table_name] = (
                table_metadata.model_dump()
            )
            self.bucket.upload_fileobj(file.file, remote_filepath)
            self.bucket.put_object(
                Body=json.dumps(self.metadata_object).encode("UTF-8"),
                Key=f"{self.prefix}/metadata.json",
            )
        except Exception as e:
            logger.error(f"Failed to upload file: {str(e)}")
            raise e

    def list_files_in_s3(self):
        # print("here")
        # return
        files_list = [
            obj.key.split("/")[-1]
            for obj in self.bucket.objects.filter(Prefix=self.prefix)
        ]
        return_json_list = []
        for file in files_list:
            if file == "metadata.json":
                continue

            table_metadata = self.metadata_object["tables"][Path(file).stem]
            return_json = {}
            return_json["table_name"] = table_metadata.pop("table_name", None)
            download_link = self._get_download_link(file)
            return_json["download_link"] = download_link
            return_json["metadata"] = table_metadata
            return_json_list.append(return_json)
        print(return_json_list)
        return return_json_list

    def delete_file(self, file_name: str):
        self.bucket.delete_objects(
            Delete={"Objects": [{"Key": f"{self.prefix}/{file_name}"}]}
        )

    def _get_download_link(self, file_name: str):
        download_link = self.s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket.name, "Key": f"{self.prefix}/{file_name}"},
            ExpiresIn=3600,
        )
        return download_link
