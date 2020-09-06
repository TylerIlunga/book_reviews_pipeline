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
            print ("Error while creating PostgreSQL table:", error)
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


DBS = DatabaseService()
DBS.connect()
statement0 = """CREATE TABLE users (
        UserID TEXT PRIMARY KEY,
        Location TEXT NOT NULL,
        Age TEXT NOT NULL
    );"""
statement1 = """CREATE TABLE books (
        ISBN TEXT PRIMARY KEY,
        BookTitle TEXT NOT NULL,
        BookAuthor TEXT NOT NULL,
        YearOfPublication TEXT NOT NULL,
        ImageURLS TEXT NOT NULL,
        ImageURLM TEXT NOT NULL,
        ImageURLL TEXT NOT NULL,
        Country TEXT NOT NULL,
        Isebook TEXT NOT NULL,
        Authors  TEXT NOT NULL,
        Description TEXT NOT NULL,
        Canonicalvolumelink TEXT NOT NULL,
        Categories TEXT NOT NULL,
        Language  TEXT NOT NULL,
        Pagecount TEXT NOT NULL,
        Publisheddate TEXT NOT NULL,
        Publisher TEXT NOT NULL
    );"""
statement2 = """CREATE TABLE bookratings (
        UserID TEXT NOT NULL,
        ISBN TEXT NOT NULL,
        BookRating TEXT NOT NULL
    );"""

df0 = DBS.execute(statement0)
print("df0:", df0)
df1 = DBS.execute(statement1)
print("df1:", df1)
df2 = DBS.execute(statement2)
print("df2:", df2)
statement1 = "SELECT * from users;"
df1 = DBS.execute(statement1)
statement1 = "INSERT INTO users VALUES (1, 't@x.com');"
df1 = DBS.execute(statement1)
statement1 = "SELECT * from users;"
df1 = DBS.execute(statement1)
print("df1:", df1)

