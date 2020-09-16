import pandas as pd
import os
import io
import json
from kafka import KafkaProducer
from config.config import get_config
from services.aws import AWSService
from services.gbs import GoogleBooksService
from services.database import DatabaseService
from database.queries import QueryBuilder

AWS = AWSService()
GBS = GoogleBooksService()

config = get_config()


def collect_book_data_frames():
    print("collect_streams()")
    data_frames = []
    aws_config = config["aws"]
    for file in os.listdir('./csv'):
        bucket = aws_config['aws_s3_bucket']
        data_key = aws_config['aws_s3_file_key'] + str(file)
        print("bucket, data_key", bucket, data_key)
        df = AWS.pull_data_from_s3_bucket(bucket, data_key, file)
        data_frames.append(df)
    return data_frames


def fetch_more_book_data(isbn):
    print("fetch_more_book_data()")
    return GBS.getBookDataByISBN(isbn=isbn)


def updated_book_data_table(books_df):
    books_df["country"] = pd.Series([], dtype=str)
    books_df["isebook"] = pd.Series([], dtype=str)
    books_df["saleability"] = pd.Series([], dtype=str)
    books_df["description"] = pd.Series([], dtype=str)
    books_df["authors"] = pd.Series([], dtype=str)
    books_df["canonicalvolumelink"] = pd.Series([], dtype=str)
    books_df["categories"] = pd.Series([], dtype=str)
    books_df["language"] = pd.Series([], dtype=str)
    books_df["description"] = pd.Series([], dtype=str)
    books_df["pagecount"] = pd.Series([], dtype=str)
    books_df["publisheddate"] = pd.Series([], dtype=str)
    return books_df


def assign_new_value_to_column(column, gbs_data_key, gbs_data, record):
    print("assign_new_value_to_column()")
    # print("gbs_data_key, gbs_data:", gbs_data_key, gbs_data)
    if gbs_data_key in gbs_data and column in gbs_data[gbs_data_key]:
        print("column:", column)
        if (type(gbs_data[gbs_data_key][column]) == list):
            gbs_data[gbs_data_key][column] = ','.join(
                gbs_data[gbs_data_key][column])
        print("record[column.lower()]:", record[column.lower()])
        print("gbs_data[gbs_data_key][column]:",
              gbs_data[gbs_data_key][column])
        record[column.lower()] = str(gbs_data[gbs_data_key][column])


def merge_book_data(book_data_tables, google_book_data):
    print("merge_book_data()")
    for gbs_data in google_book_data:
        print("gbs_data:", gbs_data)
        book_df = updated_book_data_table(book_data_tables["Books"])
        # print("book_df:", book_df)
        book_records = []
        if "ISBN" in gbs_data and "ISBN" in book_df:
            # Check logs for columns
            book_records = book_df.loc[book_df["ISBN"] == gbs_data["ISBN"]]
        print("book_records:", len(book_records), book_records)
        if len(book_records) > 0:
            # ADD NEW COLUMNS
            # "saleInfo": country(str), isEbook(bool) saleability(str)
            print("ADD NEW COLUMNS")
            assign_new_value_to_column(
                "country", "saleInfo", gbs_data, book_records)
            assign_new_value_to_column(
                "isEbook", "saleInfo", gbs_data, book_records)
            assign_new_value_to_column(
                "saleability", "saleInfo", gbs_data, book_records)
            # "volumeInfo": authors(list of str), description(str), canonicalVolumeLink(str),
            # categories(list of strs), language(str), pageCount(int), publishedDate(str), publisher(str)
            assign_new_value_to_column(
                "authors", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column(
                "description", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column(
                "canonicalVolumeLink", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column(
                "categories", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column(
                "language", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column(
                "pageCount", "volumeInfo", gbs_data, book_records)
            assign_new_value_to_column(
                "publishedDate", "volumeInfo", gbs_data, book_records)
            book_df.loc[book_df["ISBN"] == gbs_data["ISBN"]] = book_records
        print("book_records(post):", book_records)
        print("book_df(post):", book_df)


def publish_new_data_to_kafka_topic(book_data_tables):
    print("publish_new_data_to_kafka_topic")
    # Connect to Kafka Broker
    kafka_producer = KafkaProducer(
        bootstrap_servers=[config["kafka"]["bootstrap_servers"]],
        value_serializer=config["kafka"]["value_serializer"]
    )

    users_df_json = book_data_tables["Users"].to_json()
    books_df_json = book_data_tables["Books"].to_json()
    bookratings_df_json = book_data_tables["Book-Ratings"].to_json()

    # Send/Publish data to topic
    # future = kafka_producer.send('test', b'test')
    users_public_future = kafka_producer.send(
        'users', json.loads(users_df_json))
    books_public_future = kafka_producer.send(
        'books', json.loads(books_df_json))
    bookreviews_public_future = kafka_producer.send(
        'bookratings', json.loads(bookratings_df_json))
    # result = future.get(timeout=60)
    upf_result = users_public_future.get(timeout=60)
    bpf_result = books_public_future.get(timeout=60)
    brpf_result = bookreviews_public_future.get(timeout=60)
    # Print ACK
    # print("result:", result)
    print("upf_result:", upf_result)
    print("bpf_result:", bpf_result)
    print("brpf_result:", brpf_result)


def persist_transformed_data(table, table_data, QB, DBS):
    print("persist_transformed_data()")

    for index in table_data[table].index:
        query = QB.persist_data_frame(table, table_data[table], index)
        print("query:", query)

        record_set_df = DBS.execute(query)
        print("record_set_df:", record_set_df)
        print("SUCCESS")
