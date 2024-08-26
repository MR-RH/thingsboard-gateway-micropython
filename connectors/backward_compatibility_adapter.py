import ujson

class BackwardCompatibilityAdapter:
    def __init__(self, config):
        self._config = ujson.loads(ujson.dumps(config))

        @staticmethod
        def _parce_attribute_info(config): 
            attribute_name_expression = config.pop('attributeNameJsonExpression', None)
            if attribute_name_expression:
                config['attributeNameExpressionSource'] = 'message'
                config['attributeNameExpression'] = attribute_name_expression
                return

            attribute_name_expression = config.pop('attributeNameTopicExpression', None)
            if attribute_name_expression:
                config['attributeNameExpressionSource'] = 'topic'
                config['attributeNameExpression'] = attribute_name_expression

        @staticmethod
        def is_old_config_format(config):
            return config.get('mapping') is not None