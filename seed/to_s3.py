import os
import boto3 as aws

from dotenv import load_dotenv

load_dotenv()

s3_client = aws.client('s3', aws_access_key_id=os.getenv('aws_access_key_id'), aws_secret_access_key=os.getenv('aws_secret_access_key_id'))

print(s3_client)

for file in os.listdir('./csv'):
    upload_file_bucket = os.getenv('aws_s3_bucket')
    upload_file_key = os.getenv('aws_s3_file_key') + str(file)
    s3_client.upload_file('./csv/' + file, upload_file_bucket, upload_file_key)