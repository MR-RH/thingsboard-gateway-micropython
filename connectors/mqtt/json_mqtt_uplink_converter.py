from re import search
from json import dumps

from connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from gateway.constants import SEND_ON_CHANGE_PARAMETER


class JSONMqttUplinkConverter(MqttUplinkConverter):
    CONFIGURATION_OPTION_USE_EVAL = "useEval"

    def __init__(self, config, logger):
        self._log = logger
        self.__config = config.get('converter')
        self.__send_data_on_change = self.__config.get(SEND_ON_CHANGE_PARAMETER)
        self.__use_eval = self.__config.get(self.CONFIGURATION_OPTION_USE_EVAL, False)
        
    @property
    def config(self):
        return self.__config

    @config.setter
    def config(self, value):
        self.__config = value
        
    