class QueryBuilder:
    def __init__(self):
        print("New Query Builder")
    def get_all_reviews(self):
        return 'SELECT * from reviews;'
    def get_all_books(self):
        return 'SELECT * from books;'
    def get_all_users(self):
        return 'SELECT * from users;'
    def persist_data_frame(self, table, row):
        print("persist_data_frame():", row) # {attr1: value1, attr2: value2, ...}
        attributes = ','.join(list(row.keys()))
        values = []
        for value in row.values():
            values.append(f"'{value}'")
        print("****values(after iter)*****:", values)
        values = ','.join(values)
        print(f"attributes: {attributes}, values: {values}")
        query = (table, attributes, values)
        print("query(post):", query)
        return  """INSERT INTO %s (%s) VALUES(%s);""" % query
    def custom(self, query):
        return query