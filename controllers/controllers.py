from flask_restful import reqparse, Resource
from domain.domain import *

class Ping(Resource):
    def get(self):
        return "pong"

class RunPipeline(Resource):
    def post(self):
        print("/run")
        # Extract Data as csv streams
        data_frames = collect_book_data_frames([])
        book_data_tables = {}
        index = 0
        for df in data_frames:
            if index == 0:
                book_data_tables["Book-Ratings"] = df
            elif index == 1:
                 book_data_tables["Books"] = df
            else:
                 book_data_tables["Users"] = df
            index = index + 1
        # Transform CSV Data
        # 1) Pull data from online source that can give us more data on books based on their isbn
        google_book_data = []
        for isbn in book_data_tables["Books"]['ISBN'].tolist():
            google_book_data.append(fetch_more_book_data(str(isbn)))
        for data in google_book_data:
            print("****DATA****:", data)
        # 2) Add that data to CSV table (1) Total revenue made off of book, (2) Total money spent on books per user
        merge_book_data(book_data_tables, google_book_data)

        books_table_data = {}
        users_table_data = {}
        book_ratings_table_data = {}
        # Load Transform Data into PSQL
        table_data = {
            "books": books_table_data,
            "users": users_table_data,
            "book_ratings": book_ratings_table_data
        }
        persist_transformed_data(table_data)


        return {"error": False}
