import network
import ujson as json
from umqtt.simple import MQTTClient
import _thread
import time

class TBGatewayService:
    def __init__(self, config_file=None):
        self.stopped = False
        self.__lock = _thread.allocate_lock()
        print("gw: TBGatewayService - __init__() -- Acquired lock")

        # Load configuration
        if config_file is None:
            config_file = 'config/tb_gateway.json'
        self._config_dir = '/'.join(config_file.split('/')[:-1]) + '/'
        print("gw: TBGatewayService - __init__() -- Loaded configuration")

        self.__config = self.__load_general_config(config_file)
        print("gw: TBGatewayService - __init__() -- Got self.__config")

        # Connect to WiFi
        self.__connect_wifi("___ssid___", "___password___")
        print("gw: TBGatewayService - __init__() -- Connected to WiFi")

        # Initialize MQTT client
        self.tb_client = MQTTClient(self.__config["thingsboard"]["security"]["clientId"],
                                    self.__config["thingsboard"]["host"],
                                    port=self.__config["thingsboard"]["port"],
                                    user=self.__config["thingsboard"]["security"]["username"],
                                    password=self.__config["thingsboard"]["security"]["password"])
        print("gw: TBGatewayService - __init__() -- Initialized tb_client")

        # Connect to ThingsBoard
        self.tb_client.set_callback(self.on_message)
        self.tb_client.connect()
        print("gw: TBGatewayService - __init__() -- Connected to Thingsboard")

        # Start main loop
        _thread.start_new_thread(self.main_loop, ())
        print("gw: TBGatewayService - __init__() -- Started main loop")

    def __load_general_config(self, config_file):
        with open(config_file, 'r') as f:
            return json.load(f)

    def __connect_wifi(self, ssid, password):
        wlan = network.WLAN(network.STA_IF)
        wlan.active(True)
        wlan.connect(ssid, password)
        while not wlan.isconnected():
            pass
        print("gw: TBGatewayService - __connect_wifi() -- Network config:", wlan.ifconfig())

    def on_message(self, topic, msg):
        print("gw: TBGatewayService - on_message() -- Received message:", topic, msg)

    def main_loop(self):
        while not self.stopped:
            # Placeholder for main loop logic
            time.sleep(1)

    def stop(self):
        self.stopped = True
        self.tb_client.disconnect()
        print("gw: TBGatewayService - __init__() -- Disconnected from Thingsboard")
