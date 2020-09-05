import pandas as pd
import os
import io
from config.config import get_config

from services.gbs import GoogleBooksService
from services.database import DatabaseService
from database.queries import QueryBuilder

GBS = GoogleBooksService()

def collect_book_data_frames(data_frames):
    print("collect_streams()")
    aws_config = get_config()["aws"]
    for file in os.listdir('./csv'):
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
    return GBS.getBookDataByISBN(isbn=isbn)

def merge_book_data(book_data_tables, google_book_data):
    print("merge_book_data()")
    for gbs_data in google_book_data:
        print("gbs_data:", gbs_data)
        book_df = book_data_tables["Books"]
        print("book_df:", book_df)
        book_records = book_df.loc[book_df['ISBN'] == gbs_data["ISBN"]]
        print("book_records:", len(book_records), book_records)
        if len(book_records) != 0:
            # ADD NEW COLUMNS
            print ("ADD NEW COLUMNS")


def persist_transformed_data(table, table_data):
    print("persist_transformed_data()")
    DBS = DatabaseService()
    QB = QueryBuilder()

    DBS.connect()
    
    query = QB.persist_data_frame(table, table_data)
    print("query:", query)

    record_set_df = DBS.execute(query)
    print("record_set_df:", record_set_df)

    print("success")

    