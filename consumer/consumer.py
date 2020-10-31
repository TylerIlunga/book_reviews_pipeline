from kafka import KafkaConsumer
from config.config import get_config
from domain.domain import persist_transformed_data
from services.gbs import DatabaseService
from services.gbs import QueryBuilder


config = get_config()
kafka_config = config["kafka"]

class StreamBufferConsumer:
    def __init__(self):
        books_consumer = KafkaConsumer(
            "books",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            bootstrap_servers=kafka_config["bootstrap_servers"],
            value_deserializer=kafka_config["value_deserializer"]
        )

        bookratings_consumer = KafkaConsumer(
            "bookratings",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            bootstrap_servers=kafka_config["bootstrap_servers"],
            value_deserializer=kafka_config["value_deserializer"]
        )
        
        users_consumer = KafkaConsumer(
            "users",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            bootstrap_servers=kafka_config["bootstrap_servers"],
            value_deserializer=kafka_config["value_deserializer"]
        )

        self.consumers = {
            "books": books_consumer,
            "bookratings": bookratings_consumer,
            "users": users_consumer
        }
    
    def consume(self, topic):
        if topic not in self.consumers:
            print(f"Topic {topic} does not exist.")
            return
        
        consumer = self.consumers[topic]
        
        while True:
            for msg in consumer:
                print(f"{topic} message: {msg.value}")
                record_data = msg.value

                # table_data = {
                #     "bookratings": book_data_tables["Book-Ratings"],
                #     "books": book_data_tables["Books"],
                #     "users": book_data_tables["Users"]
                # }

                DBS = DatabaseService()
                QB = QueryBuilder()

                DBS.connect()

                if topic == "books":
                    record_data['booktitle'] = record_data.pop('Book-Title')
                    record_data['bookauthor'] = record_data.pop('Book-Author')
                    for key, value in record_data.items():
                        if value == "'":
                            record_data[key] = "''"
                if topic == "bookratings":
                    record_data['userid'] = record_data.pop('User-ID')
                    record_data['bookrating'] = record_data.pop('Book-Rating')
                if topic == "users":
                    record_data['userid'] = record_data.pop('User-ID')
                
                print("******record_data*****:", record_data)

                for table in record_data.keys():
                    print("record_data table:", table)
                    print("record_data[table]:", record_data[table])
                    persist_transformed_data(table, record_data, QB, DBS)


                # table_data["users"] = table_data["users"].rename(
                #     columns={'User-ID': 'userid'}, inplace=False, errors='raise')
                # table_data["bookratings"] = table_data["bookratings"].rename(
                #     columns={'User-ID': 'userid', 'Book-Rating': 'bookrating'}, inplace=False, errors='raise')
                # table_data["books"] = table_data["books"].rename(
                #     columns={'Book-Title': 'booktitle', 'Book-Author': 'bookauthor'}, inplace=False, errors='raise')
                # table_data["books"] = table_data["books"].replace(
                #     "'", "''", regex=True).astype(str)
                # print("table_data*****:", table_data)

                # for table in table_data.keys():
                #     print("table_data table:", table)
                #     print("table_data[table]:", table_data[table])
                #     persist_transformed_data(table, table_data, QB, DBS)

if __name__ == "__main__":
    consumer = StreamBufferConsumer()
    # consumer.consume("books")
    # consumer.consume("bookratings")
    consumer.consume("users")
