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
        # data_location = 's3://{}/{}'.format(bucket, data_key)
        data_location = './csv/{}'.format(str(file))
        print("data_location:",data_location)
        df = pd.read_csv(data_location, error_bad_lines=False, encoding='iso-8859-1', delimiter=";")
        data_frames.append(df)
    return data_frames


def fetch_more_book_data(isbn):
    print("fetch_more_book_data()")
    return GBS.getBookDataByISBN(isbn=isbn)

def updated_book_data_table(books_df):
    books_df["Country"] = pd.Series([])
    books_df["Isebook"] = pd.Series([])
    books_df["Saleability"] = pd.Series([])
    books_df["Description"] = pd.Series([])
    books_df["Authors"] = pd.Series([])
    books_df["Canonicalvolumelink"] = pd.Series([])
    books_df["Categories"] = pd.Series([])
    books_df["Language"] = pd.Series([])
    books_df["Description"] = pd.Series([])
    books_df["Pagecount"] = pd.Series([])
    books_df["Publisheddate"] = pd.Series([])
    books_df["Publisher"] = pd.Series([])
    return books_df

def assign_new_value_to_column(column, gbs_data_key, gbs_data, record):
    if gbs_data_key in gbs_data and column in gbs_data[gbs_data_key]:
        if (type(gbs_data[gbs_data_key][column]) == list):
            gbs_data[gbs_data_key][column] = ','.join(gbs_data[gbs_data_key][column])
        print("record[column.capitalize()]:", record[column.capitalize()])
        print("gbs_data[gbs_data_key][column]:", gbs_data[gbs_data_key][column])
        record[column.capitalize()] = gbs_data[gbs_data_key][column]

def merge_book_data(book_data_tables, google_book_data):
    print("merge_book_data()")
    for gbs_data in google_book_data:
        print("gbs_data:", gbs_data)
        book_df = updated_book_data_table(book_data_tables["Books"])
        print("book_df:", book_df)
        book_records = []
        if "ISBN" in gbs_data and "ISBN" in book_df:
            book_records = book_df.loc[book_df["ISBN"] == gbs_data["ISBN"]] # Check logs for columns
        print("book_records:", len(book_records), book_records)
        if len(book_records) != 0:
            # ADD NEW COLUMNS
            # "saleInfo": country(str), isEbook(bool) saleability(str)
            print ("ADD NEW COLUMNS")
            assign_new_value_to_column("country", "saleInfo", gbs_data, book_records)
            assign_new_value_to_column("isEbook", "saleInfo", gbs_data, book_records)
            # "volumeInfo": authors(list of str), description(str), canonicalVolumeLink(str), 
            # categories(list of strs), language(str), pageCount(int), publishedDate(str), publisher(str)
            assign_new_value_to_column("authors", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column("description", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column("canonicalVolumeLink", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column("categories", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column("language", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column("pageCount", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column("publishedDate", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column("publisher", "volumeInfo", gbs_data, book_records)
        print("book_records:", book_records)
        print("book_df(post):", book_df)

def persist_transformed_data(table, table_data, QB, DBS):
    print("persist_transformed_data()")

    for index in table_data[table].index:
        query = QB.persist_data_frame(table, table_data[table], index)
        print("query:", query)

        record_set_df = DBS.execute(query)
        print("record_set_df:", record_set_df)
        print("SUCCESS")

    