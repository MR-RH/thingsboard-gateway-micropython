import ujson as json
import _thread
from connectors.connector import Connector
from lib.tb_upy_sdk.umqtt import MQTTClient, MQTTException
from tb_utility.tb_logger import TbLogger
from gateway.constants import SEND_ON_CHANGE_PARAMETER, DEFAULT_SEND_ON_CHANGE_VALUE, \
                            SEND_ON_CHANGE_TTL_PARAMETER, DEFAULT_SEND_ON_CHANGE_INFINITE_TTL_VALUE

import time

class MQTTConnector(Connector):
    def __init__(self, gateway, config_path):
        self.logger = TbLogger("MQTTConnector")
        self.config = self.load_config(config_path)

        self.__gateway = gateway  # Reference to TB Gateway
        self._connector_type = "mqtt"

        # Extract main sections from configuration ---------------------------------------------------------------------
        self.__broker = self.config.get('broker')
        self.__send_data_only_on_change = self.__broker.get(SEND_ON_CHANGE_PARAMETER, DEFAULT_SEND_ON_CHANGE_VALUE)
        self.__send_data_only_on_change_ttl = self.__broker.get(SEND_ON_CHANGE_TTL_PARAMETER,
                                                                DEFAULT_SEND_ON_CHANGE_INFINITE_TTL_VALUE)

        # for sendDataOnlyOnChange param
        self.__topic_content = {}

        self.__mapping = []
        self.__server_side_rpc = []
        self.__connect_requests = []
        self.__disconnect_requests = []
        self.__attribute_requests = []
        self.__attribute_updates = []

        self.__shared_custom_converters = {}

        mandatory_keys = {
            "dataMapping": ['topicFilter', 'converter'],
            "serverSideRpc": ['deviceNameFilter', 'methodFilter', 'requestTopicExpression', 'valueExpression'],
            "connectRequests": ['topicFilter'],
            "disconnectRequests": ['topicFilter'],
            "attributeRequests": ['topicFilter', 'topicExpression', 'valueExpression'],
            "attributeUpdates": ['deviceNameFilter', 'attributeFilter', 'topicExpression', 'valueExpression']
        }

        # Mappings, i.e., telemetry/attributes-push handlers provided by user via configuration file
        self.load_handlers('dataMapping', mandatory_keys['dataMapping'], self.__mapping)

        # RPCs, i.e., remote procedure calls (ThingsBoard towards devices) handlers
        self.load_handlers('serverSideRpc', mandatory_keys['serverSideRpc'], self.__server_side_rpc)

        # Connect requests, i.e., telling ThingsBoard that a device is online even if it does not post telemetry
        self.load_handlers('connectRequests', mandatory_keys['connectRequests'], self.__connect_requests)

        # Disconnect requests, i.e., telling ThingsBoard that a device is offline even if keep-alive has not expired yet
        self.load_handlers('disconnectRequests', mandatory_keys['disconnectRequests'], self.__disconnect_requests)

        # Shared attributes direct requests, i.e., asking ThingsBoard for some shared attribute value
        self.load_handlers('attributeRequests', mandatory_keys['attributeRequests'], self.__attribute_requests)

        # Attributes updates requests, i.e., asking ThingsBoard to send updates about an attribute
        self.load_handlers('attributeUpdates', mandatory_keys['attributeUpdates'], self.__attribute_updates)

        self._client = MQTTClient(
            client_id=self.__broker['clientId'],
            server=self.__broker['host'],
            port=self.__broker.get('port', 1883),
            user=self.__broker['security'].get('username'),
            password=self.__broker['security'].get('password'),
            keepalive=self.__broker.get('keepalive', 120)
        )
        self._client.set_callback(self.on_message)
        self.logger.info("MQTTConnector initialized with config from {}".format(config_path))

        self.__stopped = False
        self._connected = False

    @staticmethod
    def load_config(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)
        
    def load_handlers(self, handler_flavor, mandatory_keys, accepted_handlers_list):
        handler_configuration = self.config.get(handler_flavor)
        if handler_configuration is None:
            request_mapping_config = self.config.get("requestsMapping", {})
            if isinstance(request_mapping_config, list):
                handler_configuration = []
                for request_mapping in request_mapping_config:
                    if request_mapping.get("requestType") == handler_flavor:
                        handler_configuration.append(request_mapping.get("requestValue"))

        if not handler_configuration:
            self.logger.debug("'{}' section missing from configuration".format(handler_flavor))
        else:
            for handler in handler_configuration:
                discard = False

                for key in mandatory_keys:
                    if key not in handler:
                        # Will report all missing fields to user before discarding the entry => no break here
                        discard = True
                        self.logger.error("Mandatory key '{}' missing from {} handler: {}".format(
                                         key, handler_flavor, json.dumps(handler)))
                    else:
                        self.logger.debug("Mandatory key '{}' found in {} handler: {}".format(
                                         key, handler_flavor, json.dumps(handler)))

                if discard:
                    self.logger.warning("{} handler is missing some mandatory keys => rejected: {}".format(
                                       handler_flavor, json.dumps(handler)))
                else:
                    accepted_handlers_list.append(handler)
                    self.logger.debug("{} handler has all mandatory keys => accepted: {}".format(
                                     handler_flavor, json.dumps(handler)))

            self.logger.info("Number of accepted {} handlers: {}".format(
                            handler_flavor,
                            len(accepted_handlers_list)))

            self.logger.debug("Number of rejected {} handlers: {}".format(
                             handler_flavor,
                             len(handler_configuration) - len(accepted_handlers_list)))

    def open(self):
        self.__stopped = False
        _thread.start_new_thread(self.run, ())
    
    def run(self):
        try:
            self.__connect()
        except Exception as e:
            self.logger.exception(e)
            try:
                self.close()
            except Exception as e:
                self.logger.exception(e)

        while True:
            if self.__stopped:
                break
            elif not self._connected:
                self.__connect()
            time.sleep(.2)
    
    def __connect(self):
        while not self._connected and not self.__stopped:
            try:
                self.connect()
                self._connected = True
                if not self._connected:
                    time.sleep(1)
            except Exception as e:
                self.logger.error(e)
                time.sleep(10)
    def close(self):
        self.__stopped = True
        try:
            self._client.disconnect()
        except Exception as e:
            self.logger.exception(e)
        # TODO: stop all threads and other cleanup
        self.logger.info('%s has been stopped.', self.get_name())
        del self.logger

    def connect(self):
        try:
            self._client.connect()
            self.logger.info("Connected to MQTT Broker")
        except MQTTException as e:
            self.logger.error(f"Connection to MQTT Broker failed: {e}")    

    def on_message(self, topic, msg):
        self.logger.info(f"Received message on topic {topic}: {msg}")

    def loop(self):
        while True:
            self._client.check_msg()
            time.sleep(1)