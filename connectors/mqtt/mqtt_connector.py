import ujson as json
from connectors.connector import Connector

class MQTTConnector(Connector):
    @staticmethod
    def load_config(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)