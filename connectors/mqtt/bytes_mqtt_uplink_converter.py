class BytesMqttUplinkConverter:
    
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