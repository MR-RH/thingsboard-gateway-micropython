import network
import ujson as json
from lib.tb_device_mqtt import TBDeviceMqttClient
import _thread
import time
from lib.tb_logger import TbLogger

class TBGatewayService:
    def __init__(self, config_file=None):
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

        # Start main loop
        _thread.start_new_thread(self.main_loop, ())
        self.logger.info("Started main loop")

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

    def on_message(self, topic, msg):
        self.logger.info(f"Received message: {topic} {msg}")

    def main_loop(self):
        while not self.stopped:
            # Placeholder for main loop logic
            time.sleep(1)

    def stop(self):
        self.stopped = True
        self.tb_client.disconnect()
        self.logger.info("Disconnected from ThingsBoard")
