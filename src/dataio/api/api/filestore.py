import boto3
import dotenv
import os
from fastapi import UploadFile

dotenv.load_dotenv()

class DatasetVersionS3:
    def __init__(self, dataset_id: str, version_id: str):
        self.dataset_id = dataset_id
        self.version_id = version_id

        self.session = boto3.Session(aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),)

        self.s3 = self.session.resource('s3')

        self.bucket = self.s3.Bucket(os.getenv('AWS_BUCKET_NAME'))

    def _get_prefix_for_dataset(self, dataset_id: str, version_id: str):
        return f"filestore/{dataset_id}/{version_id}"

    def upload_file(self, file: UploadFile):
        prefix = self._get_prefix_for_dataset(self.dataset_id, self.version_id)
        remote_filepath = f"{prefix}/{os.path.basename(file.filename)}"
        self.bucket.upload_fileobj(file.file, remote_filepath)
    
    def list_files_in_s3(self):
        prefix = self._get_prefix_for_dataset(self.dataset_id, self.version_id)
        files_list = [obj.key.split('/')[-1] for obj in self.bucket.objects.filter(Prefix=prefix)]
        return files_list