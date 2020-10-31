from kafka import KafkaConsumer
from config.config import get_config
from domain.domain import persist_transformed_data
from services.database import DatabaseService
from database.queries import QueryBuilder


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

                num_of_rows = 0
                rows = []
                for key in record_data.keys():
                    num_of_rows = len(record_data[key].keys())
                    break
                for row_num in range(num_of_rows):
                    row = {}
                    for attribute in record_data.keys():
                        value = record_data[attribute][str(row_num)]
                        if value == None:
                            value = ''
                        elif type(value) == int or type(value) == float:
                            value = str(value)
                        row[attribute.lower()] = value
                    rows.append(row)
                
                print("***rows***:", rows)
                persist_transformed_data(topic, rows, QB, DBS)

                DBS.disconnect()

if __name__ == "__main__":
    consumer = StreamBufferConsumer()
    # consumer.consume("books")
    # consumer.consume("bookratings")
    consumer.consume("users")
