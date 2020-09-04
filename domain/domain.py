import pandas as pd
import os
import io
from config.config import get_config

from services.services import GoogleBooksService


def collect_book_data_frames(data_frames):
    print("collect_streams()")
    aws_config = get_config()["aws"]
    for file in os.listdir('./data'):
        bucket = aws_config['aws_s3_bucket']
        data_key = aws_config['aws_s3_file_key']+ str(file)
        print("bucket, data_key", bucket, data_key)
        data_location = 's3://{}/{}'.format(bucket, data_key)
        print("data_location:",data_location)
        df = pd.read_csv(data_location, error_bad_lines=False, encoding='iso-8859-1', delimiter=";")
        data_frames.append(df)
    return data_frames


def fetch_more_book_data(isbn):
    print("fetch_more_book_data()")
    return GoogleBooksService().getBookDataByISBN(isbn=isbn)

def merge_book_data(book_data_tables, google_book_data):
    print("merge_book_data()")

def persist_transformed_data(table_data):
    print("persist_transformed_data()")
    