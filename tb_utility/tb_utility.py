from ujson import dumps, loads

from tb_utility.tb_logger import TbLogger


logger = TbLogger("TBUtility")
class TBUtility:
    # Data conversion methods
    @staticmethod
    def decode(message):
        try:
            if isinstance(message.payload, bytes):
                content = loads(message.payload.decode("utf-8", "ignore"))
            else:
                content = loads(message.payload)
        except Exception:
            try:
                content = message.payload.decode("utf-8", "ignore")
            except Exception:
                content = message.payload
        return content
    
    @staticmethod
    def validate_converted_data(data):
        error = None
        if error is None and not data.get("deviceName"):
            error = 'deviceName is empty in data: '
        if error is None:
            got_attributes = False
            got_telemetry = False
            if data.get("attributes") is not None and len(data.get("attributes")) > 0:
                got_attributes = True
            if data.get("telemetry") is not None:
                for entry in data.get("telemetry"):
                    if (entry.get("ts") is not None and len(entry.get("values")) > 0) or entry.get("ts") is None:
                        got_telemetry = True
                        break
            if got_attributes is False and got_telemetry is False:
                error = 'No telemetry and attributes in data: '
        if error is not None:
            json_data = dumps(data)
            if isinstance(json_data, bytes):
                print(error + json_data.decode("UTF-8"))
            else:
                print(error + json_data)
            return False
        return True
    