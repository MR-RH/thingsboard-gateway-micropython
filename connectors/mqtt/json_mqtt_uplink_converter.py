from re import search
from json import dumps
import time

from connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from gateway.constants import SEND_ON_CHANGE_PARAMETER
from tb_utility.tb_utility import TBUtility

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
        
    def parse_device_info(self, topic, data, config, expression_source, expression):
        result = None
        device_info = config.get('deviceInfo', {})
        expression = device_info.get('deviceNameExpression') if expression == 'deviceNameExpression' \
            else device_info.get('deviceProfileExpression')
        try:
            if device_info.get(expression_source) == 'message' or device_info.get(expression_source) == 'constant':
                result_tags = TBUtility.get_values(expression, data, get_tag=True)
                result_values = TBUtility.get_values(expression, data, expression_instead_none=True)
                result = expression
                for (result_tag, result_value) in zip(result_tags, result_values):
                    is_valid_key = "${" in expression and "}" in expression
                    result = result.replace('${' + str(result_tag) + '}', str(result_value)) if is_valid_key else result_tag
            elif device_info.get(expression_source) == 'topic':
                search_result = search(expression.replace('+', '[^/]+'), topic)
                if search_result is not None:
                    result = search_result.group(0)
                else:
                    self._log.debug(
                        f"Regular expression result is None. deviceNameTopicExpression parameter will be interpreted as a deviceName\n Topic: {topic}\nRegex: {expression}")
                    result = expression
            else:
                self._log.error(f"The expression for looking \"deviceName\" not found in config {dumps(config)}")
        except Exception as e:
            self._log.error(f'Error in converter, for config: \n{dumps(config)}\n and message: \n{data}\n {e}')
        return result