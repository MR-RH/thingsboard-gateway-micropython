from gateway.constants import DEFAULT_SEND_ON_CHANGE_INFINITE_TTL_VALUE, \
    DEFAULT_SEND_ON_CHANGE_VALUE

class Connector:
    def open(self):
        raise NotImplementedError("Subclasses must implement open method")


    def close(self):
        raise NotImplementedError("Subclasses must implement close method")


    def get_id(self):
        raise NotImplementedError("Subclasses must implement get_id method")


    def get_name(self):
        raise NotImplementedError("Subclasses must implement get_name method")


    def get_type(self):
        raise NotImplementedError("Subclasses must implement get_type method")

    def get_config(self):
        raise NotImplementedError("Subclasses must implement get_config method")

    def is_connected(self):
        raise NotImplementedError("Subclasses must implement is_connected method")

    def is_stopped(self):
        raise NotImplementedError("Subclasses must implement is_stopped method")

    def on_attributes_update(self, content):
        raise NotImplementedError("Subclasses must implement on_attributes_update method")

    def server_side_rpc_handler(self, content):
        raise NotImplementedError("Subclasses must implement server_side_rpc_handler method")

    def is_filtering_enable(self, device_name):
        return DEFAULT_SEND_ON_CHANGE_VALUE

    def get_ttl_for_duplicates(self, device_name):
        return DEFAULT_SEND_ON_CHANGE_INFINITE_TTL_VALUE
