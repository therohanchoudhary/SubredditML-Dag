import boto3
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY


def connect_to_s3():
    try:
        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_ACCESS_KEY)
        return s3
    except Exception as e:
        print(e)


def create_bucket_if_not_exist(s3: boto3.client, bucket: str):
    response = s3.list_buckets()
    if bucket in [bucket['Name'] for bucket in response['Buckets']]:
        print("Bucket already exists")
        return

    try:
        s3.create_bucket(Bucket=bucket)
        print("Bucket created successfully")
    except Exception as e:
        print(f'Bucket creation exception: {e}')


def upload_to_s3(s3: boto3.client, file_path: str, bucket: str, s3_file_name: str):
    print(bucket, s3_file_name)
    try:
        s3.upload_file(file_path, bucket, bucket + '/raw/' + s3_file_name)
        print('File uploaded to s3')
    except FileNotFoundError:
        print('File not found')
    except Exception as e:
        print(f'Exception occurred while uploading to S3 : {e}')
