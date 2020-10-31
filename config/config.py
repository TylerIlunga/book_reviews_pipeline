import os
import json
from dotenv import load_dotenv

load_dotenv()

config = {
    "app_env": os.getenv("app_env"),
    "aws": {
        "aws_s3_bucket": os.getenv("aws_s3_bucket"),
        "aws_s3_file_key": os.getenv("aws_s3_file_key"),
        "aws_access_key_id": os.getenv('aws_access_key_id'),
        "aws_secret_access_key": os.getenv('aws_secret_access_key_id'),
    },
    "gcp": {
        "gb_api_key": os.getenv("gcp_books_api_key"),
    },
    "kafka": {
        "bootstrap_servers": os.getenv("kafka_bootstrap_servers"),
        "value_serializer": lambda x: json.dumps(x).encode('utf-8'),
        "value_deserializer": lambda x: json.loads(x.decode('utf-8'))
    },
    "psql": {
        "user": os.getenv("psql_user"),
        "pass": os.getenv("psql_pass"),
        "database": os.getenv("psql_database"),
        "host": os.getenv("psql_host"),
        "port": os.getenv("psql_port"),
    },
    "port": os.getenv("port"),
}


def get_config():
    return config
