

from connectors.converter import Converter

class MqttUplinkConverter(Converter):

    def convert(self, config, data):
        raise NotImplementedError("Subclasses must implement convert method")
