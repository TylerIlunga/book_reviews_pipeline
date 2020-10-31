from controllers.controllers import Ping, LoadBookDataIntoKafka

def get_resources():
    return [
        {"link": Ping, "path": "/ping"},
        {"link": LoadBookDataIntoKafka, "path": "/loaddata"}
    ]