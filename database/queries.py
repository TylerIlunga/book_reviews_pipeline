
import pandas as pd

def convert_tuple(tup):
    tup = [col.lower() for col in tup]
    tuple_str = ', '.join(tup)
    return tuple_str


class QueryBuilder:
    def __init__(self):
        print("New Query Builder")
    def get_all_reviews(self):
        return 'SELECT * from reviews;'
    def get_all_books(self):
        return 'SELECT * from books;'
    def get_all_users(self):
        return 'SELECT * from users;'
    def persist_data_frame(self, table, data_frame, record_index):
        print("persist_data_frame()") # books, users, reviews (tables)
        cols = data_frame.columns.values.tolist()
        values = []
        for col in cols:
            values.append(f"'{data_frame.iloc[record_index][col]}'") # values to populate cell (index n-m)
        query = (table, convert_tuple(cols), convert_tuple(values))
        print("query(post):", query)
        for index in data_frame.index:
            print("index:", index)
            # return """INSERT into %s (%s, %s, %s) values(%s,%s,%s);""" % tuple(values)
            return  """INSERT INTO %s (%s) VALUES(%s);""" % query
    def custom(self, query):
        return query


# QB = QueryBuilder()
# print(QB.get_all_books())
# dt = {
#     "ISBN": "0195153448",
#     "Book-Title": "Classical Mythology",
#     "Book-Author": "Mark P. O. Morford"
# }
# dt = {
#     "id": 2,
#     "email": "a@b.com",
# }
# print(QB.persist_data_frame("users", pd.DataFrame([dt])))
