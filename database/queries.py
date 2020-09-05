
class QueryBuilder:
    def __init__(self):
        print("New Query Builder")
    def get_all_reviews(self):
        return 'SELECT * from reviews;'
    def get_all_books(self):
        return 'SELECT * from books;'
    def get_all_users(self):
        return 'SELECT * from users;'
    def custom(self, query):
        return query