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
        
    def _convert_single_item(self, topic, data):
        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}
        dict_result = {
            "deviceName": self.parse_device_name(topic, data, self.__config),
            "deviceType": self.parse_device_type(topic, data, self.__config),
            "attributes": [],
            "telemetry": []
        }

        if isinstance(self.__send_data_on_change, bool):
            dict_result[SEND_ON_CHANGE_PARAMETER] = self.__send_data_on_change

        try:
            for datatype in datatypes:
                timestamp = data.get("ts", data.get("timestamp")) if datatype == 'timeseries' else None
                dict_result[datatypes[datatype]] = []
                for datatype_config in self.__config.get(datatype, []):
                    if isinstance(datatype_config, str) and datatype_config == "*":
                        for item in data:
                            dict_result[datatypes[datatype]].append(
                                self.create_timeseries_record(item, data[item], timestamp))
                    else:
                        values = TBUtility.get_values(datatype_config["value"], data, datatype_config["type"],
                                                      expression_instead_none=False)
                        values_tags = TBUtility.get_values(datatype_config["value"], data, datatype_config["type"],
                                                           get_tag=True)

                        keys = TBUtility.get_values(datatype_config["key"], data, datatype_config["type"],
                                                    expression_instead_none=False)
                        keys_tags = TBUtility.get_values(datatype_config["key"], data, get_tag=True)

                        full_key = datatype_config["key"]
                        for (key, key_tag) in zip(keys, keys_tags):
                            is_valid_key = "${" in datatype_config["key"] and "}" in datatype_config["key"]
                            full_key = full_key.replace('${' + str(key_tag) + '}', str(key)) if is_valid_key else key_tag

                        full_value = datatype_config["value"]
                        for (value, value_tag) in zip(values, values_tags):
                            is_valid_value = "${" in datatype_config["value"] and "}" in datatype_config["value"]
                            full_value = full_value.replace('${' + str(value_tag) + '}', str(value)) if is_valid_value else value

                        if full_key != 'None' and full_value != 'None':
                            dict_result[datatypes[datatype]].append(
                                self.create_timeseries_record(full_key, TBUtility.convert_data_type(
                                    full_value, datatype_config["type"], self.__use_eval), timestamp))
        except Exception as e:
            self.logger.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config),
                            str(data), e)
        self.logger.debug(dict_result)
        return dict_result
        
    @staticmethod
    def create_timeseries_record(key, value, timestamp):
        value_item = {key: value}
        return {"ts": timestamp, 'values': value_item} if timestamp else value_item
            
    def parse_device_name(self, topic, data, config):
        return self.parse_device_info(
            topic, data, config, "deviceNameExpressionSource", "deviceNameExpression")

    def parse_device_type(self, topic, data, config):
        return self.parse_device_info(
            topic, data, config, "deviceProfileExpressionSource", "deviceProfileExpression")

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