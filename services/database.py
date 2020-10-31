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
            self.conn = psycopg2.connect(user=user,
                                         password=password,
                                         host=host,
                                         port=port,
                                         database=db)
            print("Connected to PSQL Database")
    def disconnect(self):
        self.conn.close()
    def execute(self, statement):
        cursor = self.conn.cursor()
        record_set = None
        try:
            cursor.execute(statement)
            self.conn.commit()
            print("cursor.rowcount:", cursor.rowcount)
            if cursor.rowcount > 0:
                record_set = pd.DataFrame(cursor.fetchall())
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while creating PostgreSQL table:", error)
            self.conn.rollback()
            cursor.close()
        cursor.close()
        return record_set