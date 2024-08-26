import network
import ujson as json
from json import dumps, loads
from lib.tb_upy_sdk.tb_device_mqtt import TBDeviceMqttClient
from connectors.mqtt.mqtt_connector import MQTTConnector
import _thread
import time
from tb_utility.tb_logger import TbLogger
from tb_utility.tb_utility import TBUtility

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

        self.__connectors_not_found = False
        self._load_connectors()
        self.__connectors_init_start_success = True

        # self.connectors_configs = {"mqtt": []}
        # self.connectors_configs["mqtt"].append({"name": "MYMQTT",
        #                                         "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        #                                         "config": "/config/mqtt.json"})

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
        self.__connect_with_connectors()
        self.logger.info("Initialized and connected MQTTConnector")

        # Start main loop
        _thread.start_new_thread(self.main_loop, ())
        self.logger.info("Started main loop")
    
    def __init_variables(self):
        self.available_connectors_by_name = {}
        self.available_connectors_by_id = {}
        self._implemented_connectors = {}
        self.__connectors_not_found = True

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

    def _load_connectors(self, config=None):
        global log
        self.connectors_configs = {}

        if config:
            configuration = config.get('connectors')
        else:
            configuration = self.__config.get('connectors')

        if configuration:
            for connector in configuration:
                try:
                    connector_persistent_key = None
                    connector_type = connector["type"].lower() if connector.get("type") is not None else None

                    if connector_type is None:
                        log.error("Connector type is not defined!")
                        continue
                    
                    if connector_type != "grpc":
                        connector_class = None
                        # import MQTT connector hard-coded
                        connector_class = MQTTConnector
                        if connector_class is None:
                            self.logger.warning("Connector implementation not found for %s", connector['name'])
                        else:
                            self._implemented_connectors[connector_type] = connector_class
                    
                    config_file_path = self._config_dir + connector['configuration']

                    with open(config_file_path, 'r', encoding="UTF-8") as conf_file:
                        connector_conf_file_data = conf_file.read()

                    connector_conf = connector_conf_file_data
                    try:
                        connector_conf = loads(connector_conf_file_data)
                    except Exception as e:
                        self.logger.debug(e)
                        self.logger.warning("Cannot parse connector configuration as a JSON, it will be passed as a string.")

                    connector_id = TBUtility.get_or_create_connector_id(connector_conf)

                    if isinstance(connector_conf, dict):
                        if connector_conf.get('id') is None:
                            connector_conf['id'] = connector_id
                            with open(config_file_path, 'w', encoding="UTF-8") as conf_file:
                                conf_file.write(dumps(connector_conf))
                    elif isinstance(connector_conf, str) and not connector_conf:
                        raise ValueError("Connector configuration is empty!")
                    elif isinstance(connector_conf, str):
                        start_find = connector_conf.find("{id_var_start}")
                        end_find = connector_conf.find("{id_var_end}")
                        if not (start_find > -1 and end_find > -1):
                            connector_conf = "{id_var_start}" + str(connector_id) + "{id_var_end}" + connector_conf

                    if not self.connectors_configs.get(connector_type):
                        self.connectors_configs[connector_type] = []
                    if connector_type != 'grpc' and isinstance(connector_conf, dict):
                        connector_conf["name"] = connector['name']
                    if connector_type != 'grpc':
                        connector_configuration = {connector['configuration']: connector_conf}

                    self.connectors_configs[connector_type].append({"name": connector['name'],
                                                                    "id": connector_id,
                                                                    "config": connector_configuration,
                                                                    "config_file_path": config_file_path,
                                                                    "grpc_key": connector_persistent_key})
                except Exception as e:
                    self.logger.exception("Error on loading connector: {}".format(e))
        else:
            self.logger.warning("Connectors - not found!")
            


    def __connect_with_connectors (self):
        connector_config = self.connectors_configs.get("mqtt")[0]
        
        connector_name = connector_config["name"]
        connector_id = connector_config["id"]

        available_connector = self.available_connectors_by_id.get(connector_id)

        if available_connector is None or available_connector.is_stopped():
            connector = MQTTConnector(self, connector_config["config"]["mqtt.json"])
                                                                        
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
