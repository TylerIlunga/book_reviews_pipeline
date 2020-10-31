from kafka import KafkaConsumer
from config.config import get_config

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

                # table_data = {
                #     "bookratings": book_data_tables["Book-Ratings"],
                #     "books": book_data_tables["Books"],
                #     "users": book_data_tables["Users"]
                # }

                # DBS = DatabaseService()
                # QB = QueryBuilder()

                # DBS.connect()

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
