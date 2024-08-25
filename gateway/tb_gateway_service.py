import network
import ujson as json
from lib.tb_upy_sdk.tb_device_mqtt import TBDeviceMqttClient
from connectors.mqtt.mqtt_connector import MQTTConnector
import _thread
import time
from tb_utility.tb_logger import TbLogger

class TBGatewayService:
    def __init__(self, config_file=None):
        self.__init_variables()
        self.logger = TbLogger("TBGatewayService")
        self.stopped = False
        self.__lock = _thread.allocate_lock()
        self.logger.info("Acquired lock")

        # Load configuration
        if config_file is None:
            config_file = 'config/tb_gateway.json'
        self._config_dir = '/'.join(config_file.split('/')[:-1]) + '/'
        self.logger.info("Loaded configuration")

        self.__config = self.__load_general_config(config_file)
        self.logger.info("Got self.__config")

        self.connectors_configs = {"mqtt": []}
        self.connectors_configs["mqtt"].append({"name": "MYMQTT",
                                                "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
                                                "config": "/config/mqtt.json"})

        # Connect to WiFi
        self.__connect_wifi("___ssid___", "___password___")
        self.logger.info("Connected to WiFi")

        # Initialize MQTT client
        self.tb_client = TBDeviceMqttClient(host=self.__config["thingsboard"]["host"],
                                            port=self.__config["thingsboard"]["port"],
                                            access_token=self.__config["thingsboard"]["security"]["accessToken"])
        self.logger.info("Initialized tb_client")

        # Connect to ThingsBoard
        self.tb_client.set_callback(self.on_message)
        self.tb_client.connect()
        self.logger.info("Connected to ThingsBoard")

        # Initialize MQTT connector for local broker
        self.mqtt_connector = MQTTConnector(self, "/config/mqtt.json")
        self.mqtt_connector.connect()
        self.logger.info("Initialized and connected MQTTConnector")

        # Start MQTT connector loop
        self.__connect_with_connectors()
        self.logger.info("Started MQTTConnector loop")

        # Start main loop
        _thread.start_new_thread(self.main_loop, ())
        self.logger.info("Started main loop")
    
    def __init_variables(self):
        self.available_connectors_by_name = {}
        self.available_connectors_by_id = {}

    def __load_general_config(self, config_file):
        with open(config_file, 'r') as f:
            return json.load(f)

    def __connect_wifi(self, ssid, password):
        wlan = network.WLAN(network.STA_IF)
        wlan.active(True)
        wlan.connect(ssid, password)
        while not wlan.isconnected():
            pass
        self.logger.info(f'Network config: {wlan.ifconfig()}')

    def __connect_with_connectors (self):
        connector_name = self.connectors_configs["mqtt"][0]["name"]
        connector_id = self.connectors_configs["mqtt"][0]["id"]

        available_connector = self.available_connectors_by_id.get(connector_id)

        if available_connector is None or available_connector.is_stopped():
            connector = MQTTConnector(self,
                                     self.connectors_configs["mqtt"][0]["config"])
                                                                        
            connector.name = connector_name
            self.available_connectors_by_id[connector_id] = connector
            self.available_connectors_by_name[connector_name] = connector
            connector.open()
        else:
            self.logger.warning("[%r] Connector with name %s already exists and not stopped!",
                        connector_id, connector_name)

    def on_message(self, topic, msg):
        self.logger.info(f"Received message: {topic} {msg}")


    def main_loop(self):
        while not self.stopped:
            print("inside main loop")
            time.sleep(1)

    def stop(self):
        self.stopped = True
        self.tb_client.disconnect()
        self.logger.info("Disconnected from ThingsBoard")
