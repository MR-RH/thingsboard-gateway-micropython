import time
from json import dumps

from connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter

class BytesMqttUplinkConverter(MqttUplinkConverter):
    
    def __init__(self, config, logger):
        self.__config = config
        self.logger = logger

    @property
    def config(self):
        return self.__config

    @config.setter
    def config(self, value):
        self.__config = value
    
    def convert(self, topic, data):
        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}
        dict_result = {
            "deviceName": self.parse_data(self.__config['deviceInfo']['deviceNameExpression'], data),
            "deviceType": self.parse_data(self.__config['deviceInfo']['deviceProfileExpression'], data),
            "attributes": [],
            "telemetry": []
        }

        try:
            for datatype in datatypes:
                dict_result[datatypes[datatype]] = []
                for datatype_config in self.__config.get(datatype, []):
                    value_item = {datatype_config['key']: self.parse_data(datatype_config['value'], data)}
                    if datatype == 'timeseries':
                        dict_result[datatypes[datatype]].append({"ts": int(time.time()) * 1000, 'values': value_item})
                    else:
                        dict_result[datatypes[datatype]].append(value_item)
        except Exception as e:
            self.logger.error('Error in converter, for config: \n{}\n and message: \n{}\n {}'.format(dumps(self.__config),
                            str(data), e))

        self.logger.error('Converted data: {}'.format(dict_result))
        return dict_result
    
    @staticmethod
    def parse_data(expression, data):
        def findall(pattern, string):
            matches = []
            start = 0
            while True:
                start = string.find(pattern[0], start)
                if start == -1:
                    break
                end = string.find(pattern[1], start + 1)
                if end == -1:
                    break
                matches.append(string[start:end + 1])
                start = end + 1
            return matches

        expression_arr = findall('[]', expression)
        converted_data = expression

        for exp in expression_arr:
            indexes = exp[1:-1].split(':')

            data_to_replace = ''
            if len(indexes) == 2:
                from_index, to_index = indexes
                concat_arr = data[
                             int(from_index) if from_index != '' else None:int(
                                 to_index) if to_index != '' else None]
                for sub_item in concat_arr:
                    data_to_replace += str(sub_item)
            else:
                data_to_replace += str(data[int(indexes[0])])

            converted_data = converted_data.replace(exp, data_to_replace)

        return converted_data