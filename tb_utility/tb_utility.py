from ujson import dumps, loads

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