from ujson import dumps, loads
from re import search

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
    
    @staticmethod
    def topic_to_regex(topic):
        return topic.replace("+", "[^/]+").replace("#", ".+").replace('$', '\\$')

    @staticmethod
    def regex_to_topic(regex):
        return regex.replace("[^/]+", "+").replace(".+", "#").replace('\\$', '$')

    @staticmethod
    def get_value(expression, body=None, value_type="string", get_tag=False, expression_instead_none=False):
        if isinstance(body, str):
            body = loads(body)
        if not expression:
            return ''
        
        # Extract the expression inside ${...}
        match = search(r'\${(.*?)}', expression)
        if not match:
            return expression if expression_instead_none else None
        
        target_str = match.group(1)
        if get_tag:
            return target_str
        
        try:
            keys = target_str.split('.')
            full_value = body
            for key in keys:
                if '[' in key and ']' in key:
                    key, index = key[:-1].split('[')
                    full_value = full_value[key][int(index)]
                else:
                    full_value = full_value[key]
        except (KeyError, IndexError, TypeError) as e:
            print(e)
            full_value = expression if expression_instead_none else None
        
        return full_value
    
    @staticmethod
    def extract_expressions(expression):
        expression_arr = []
        start = 0
        while True:
            start = expression.find('${', start)
            if start == -1:
                break
            end = expression.find('}', start)
            if end == -1:
                break
            expression_arr.append(expression[start:end+1])
            start = end + 1
        return expression_arr

    @staticmethod
    def get_values(expression, body=None, value_type="string", get_tag=False, expression_instead_none=False):
        expression_arr = TBUtility.extract_expressions(expression)
        values = [TBUtility.get_value(exp, body, value_type=value_type, get_tag=get_tag, expression_instead_none=expression_instead_none) for exp in expression_arr]
        if '${' not in expression:
            values.append(expression)
        return values

    @staticmethod
    def get_dict_key_by_value(dictionary: dict, value):
        return list(dictionary.keys())[list(dictionary.values()).index(value)]
