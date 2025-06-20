import boto3
from botocore.client import Config
import dotenv
import os
from fastapi import UploadFile
from dataio.api.api.models import VersionType

dotenv.load_dotenv()

class DatasetS3:
    def __init__(self, dataset_id: str, type: VersionType):
        self.dataset_id = dataset_id
        self.type = type

        self.session = boto3.Session(aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),)

        self.s3 = self.session.resource('s3')
        self.s3_client = self.session.client('s3', region_name = 'ap-south-1', config=Config(signature_version='s3v4'))

        self.bucket = self.s3.Bucket(os.getenv('AWS_BUCKET_NAME'))

        self.prefix = self._get_prefix_for_dataset(self.dataset_id)

    def _get_prefix_for_dataset(self, dataset_id: str):
        return f"filestore/{self.type.value}/{dataset_id}"

    def upload_file(self, file: UploadFile):
        remote_filepath = f"{self.prefix}/{os.path.basename(file.filename)}"
        self.bucket.upload_fileobj(file.file, remote_filepath)
    
    def list_files_in_s3(self):
        return_json = {}
        files_list = [obj.key.split('/')[-1] for obj in self.bucket.objects.filter(Prefix=self.prefix)]
        for file in files_list:
            return_json['table_name'] = file
            download_link = self._get_download_link(file)
            return_json['download_link'] = download_link
        return return_json
    
    def delete_file(self, file_name: str):
        self.bucket.delete_objects(Delete={
            'Objects': [{'Key': f"{self.prefix}/{file_name}"}]
        })

    def _get_download_link(self, file_name: str):
        download_link = self.s3_client.generate_presigned_url('get_object', Params={'Bucket': self.bucket.name, 'Key': f"{self.prefix}/{file_name}"}, ExpiresIn=3600)
        return download_link