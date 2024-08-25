import time

class TbLogger:
    def __init__(self, name):
        self.name = name

    def log(self, level, message):
        timestamp = "{}-{}-{} {}:{}:{}".format(*time.localtime()[:6])
        print("{} - {} - {} - {}".format(timestamp, self.name, level, message))

    def info(self, message):
        self.log("INFO", message)

    def warning(self, message):
        self.log("WARNING", message)

    def error(self, message):
        self.log("ERROR", message)

    def debug(self, message):
        self.log("DEBUG", message)