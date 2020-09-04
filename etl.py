import pandas as pd
import os
import boto3 as aws
import io
from config.config import get_config
from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
from resources.resources import get_resources
from domain import *

app = Flask(__name__)
api = Api(app)

for resource in get_resources():
    api.add_resource(resource["link"], resource["path"])

config = get_config()

app.run(
    port=int(config["port"]),
    debug=True if config["appEnv"] == "debug" else False
)

