import ujson

class BackwardCompatibilityAdapter:
    def __init__(self, config):
        self._config = ujson.loads(ujson.dumps(config))
