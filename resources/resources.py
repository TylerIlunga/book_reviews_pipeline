from controllers.controllers import Ping, RunPipeline

def get_resources():
    return [
        {"link": Ping, "path": "/ping"},
        {"link": RunPipeline, "path": "/run"}
    ]