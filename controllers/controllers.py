import os
from flask_restful import reqparse, Resource
from domain.domain import *
from services.database import DatabaseService
from services.gbs import GoogleBooksService
from database.queries import QueryBuilder

GBS = GoogleBooksService()

class Ping(Resource):
    def get(self):
        return "pong"


class LoadBookDataIntoKafka(Resource):
    def get(self):
        print("/loaddata")
        # Extract Data as csv streams
        data_frames = collect_book_data_frames()
        book_data_tables = {}
        index = 0
        N = int(os.getenv("ingest_limit"))
        for df in data_frames:
            if index == 0:
                book_data_tables["Book-Ratings"] = df[:N] # Top N
            elif index == 1:
                book_data_tables["Books"] = df[:N]  
            else:
                book_data_tables["Users"] = df[:N]
            index = index + 1
        # 1) Transform CSV Data: Pull data from online source that can give us more data on books based on their isbn
        google_book_data = []
        for isbn in book_data_tables["Books"]['ISBN'].tolist():
            more_book_data = GBS.getBookDataByISBN(isbn=str(isbn))
            print("Additional book data received:", more_book_data)
            google_book_data.append(more_book_data)
        print("TOTAL google_book_data:", len(google_book_data))
        # 2) Add that data to CSV table (1) Total revenue made off of book, (2) Total money spent on books per user
        merge_book_data(book_data_tables, google_book_data)
        # 3) Convert Pandas DataFrame to PySpark DataFrame (RDD) and Publish data to Kakfa Topic
        publish_new_data_to_kafka_topic(book_data_tables)

        return {"error": False}
