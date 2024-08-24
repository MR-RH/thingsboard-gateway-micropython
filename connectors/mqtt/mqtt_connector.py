import ujson as json
from connectors.connector import Connector
from lib.tb_upy_sdk.umqtt import MQTTClient, MQTTException
from tb_utility.tb_logger import TbLogger
import time

class MQTTConnector(Connector):
    def __init__(self, config_path):
        self.logger = TbLogger("MQTTConnector")
        self.config = self.load_config(config_path)
        broker_config = self.config['broker']
        self.client = MQTTClient(
            client_id=broker_config['clientId'],
            server=broker_config['host'],
            port=broker_config.get('port', 1883),
            user=broker_config['security'].get('username'),
            password=broker_config['security'].get('password'),
            keepalive=broker_config.get('keepalive', 120)
        )
        self.client.set_callback(self.on_message)
        self.logger.info("MQTTConnector initialized with config from {}".format(config_path))

    @staticmethod
    def load_config(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)

    def connect(self):
        try:
            self.client.connect()
            self.logger.info("Connected to MQTT Broker")
        except MQTTException as e:
            self.logger.error(f"Connection to MQTT Broker failed: {e}")    

    def on_message(self, topic, msg):
        self.logger.info(f"Received message on topic {topic}: {msg}")

    def loop(self):
        while True:
            self.client.check_msg()
            time.sleep(1)