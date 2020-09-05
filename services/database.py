import psycopg2
import pandas as pd
from config.config import get_config

config = get_config()

user = config["psql"]["user"]
password = config["psql"]["pass"]
host = config["psql"]["host"]
port = config["psql"]["port"]
db = config["psql"]["database"]

class DatabaseService:
    def __init__(self):
        self.conn = None
    def connect(self):
        if self.conn == None:
            self.conn = psycopg2.connect(user = user,
                                         password = password,
                                         host = host,
                                         port = port,
                                         database = db)
            print("Connected to PSQL Database")
    def execute(self, statement):
        cursor = self.conn.cursor()
        record_set = None
        try:
            cursor.execute(statement)
            self.conn.commit()
            print("cursor.rowcount:", cursor.rowcount)
            if cursor.rowcount > 0:
                record_set = pd.DataFrame(cursor.fetchall()) 
        except (Exception, psycopg2.DatabaseError) as error :
            print ("Error while creating PostgreSQL table", error)
            self.conn.rollback()
            cursor.close()
        # finally:
            #closing database connection.
            # if(self.conn):
            #     cursor.close()
            #     self.conn.close()
            #     print("PostgreSQL connection is closed")
        cursor.close()
        return record_set


# DBS = DatabaseService()
# DBS.connect()
# statement0 = """CREATE TABLE users (
#         id INT PRIMARY KEY,
#         email TEXT NOT NULL
#     );"""
# df0 = DBS.execute(statement0)
# print("df0:", df0)
# statement1 = "SELECT * from users;"
# df1 = DBS.execute(statement1)
# statement1 = "INSERT INTO users VALUES (1, 't@x.com');"
# df1 = DBS.execute(statement1)
# statement1 = "SELECT * from users;"
# df1 = DBS.execute(statement1)
# print("df1:", df1)

