import os

from dotenv import load_dotenv

load_dotenv()

config = {
        "appEnv": os.getenv("appEnv"),
        "aws": {
            "aws_s3_bucket": os.getenv("aws_s3_bucket"),
            "aws_s3_file_key": os.getenv("aws_s3_file_key"),
            "aws_access_key_id": os.getenv('aws_access_key_id'),
            "aws_secret_access_key": os.getenv('aws_secret_access_key_id'),
        },
        "gcp": {
            "gb_api_key": "AIzaSyC7jWti_dhdFrocUbARikM3B_DHHpQNN-k",
        },
        "port": os.getenv("port"),
    }

def get_config():
    return config