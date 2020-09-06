from flask_restful import reqparse, Resource
from domain.domain import *
from services.database import DatabaseService
from database.queries import QueryBuilder

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
                book_data_tables["Book-Ratings"] = df[:2] # Top 100
            elif index == 1:
                 book_data_tables["Books"] = df[:2]
            else:
                 book_data_tables["Users"] = df[:2]
            index = index + 1
        # Transform CSV Data
        # 1) Pull data from online source that can give us more data on books based on their isbn
        google_book_data = []
        for isbn in book_data_tables["Books"]['ISBN'].tolist():
            more_book_data = fetch_more_book_data(str(isbn))
            print("Additional book data received:", more_book_data)
            google_book_data.append(more_book_data)
        print("TOTAL google_book_data:", len(google_book_data))
        # 2) Add that data to CSV table (1) Total revenue made off of book, (2) Total money spent on books per user
        merge_book_data(book_data_tables, google_book_data)

        # Load Transform Data into PSQL
        table_data = {
            "books": book_data_tables["Books"],
            "users": book_data_tables["Users"],
            "bookratings": book_data_tables["Book-Ratings"]
        }

        DBS = DatabaseService()
        QB = QueryBuilder()

        DBS.connect()

        table_data["users"] = table_data["users"].rename(columns = {'User-ID': 'userid'}, inplace = False)
        table_data["bookratings"] = table_data["bookratings"].rename(columns = {'User-ID': 'userid', 'Book-Rating': 'bookrating'}, inplace = False)
        table_data["books"] = table_data["books"].rename(columns = {'Book-Title': 'booktitle', 'Book-Author': 'bookauthor', 'Year-Of-Publication ': 'yearofpublication', 'Image-URL-S ': 'imageurls', 'Image-URL-M ': 'imageurlm', 'Image-URL-L ': 'imageurll'}, inplace = False)
        print("table_data*****:", table_data)
        for table in table_data.keys():
            print("table_data table:", table)
            print("table_data[table]:", table_data[table])
            persist_transformed_data(table, table_data, QB, DBS)


        return {"error": False}
