import boto3


def upload_file_to_URI(URI, file):

    client = boto3.client('s3')

    URI = URI.removeprefix("s3://")
    Bucket = URI.split("/")[0]
    Key = URI.removeprefix(Bucket + "/")

    client.upload_file(Filename=file.name,
                       Bucket=Bucket,
                       Key=Key
                       )
