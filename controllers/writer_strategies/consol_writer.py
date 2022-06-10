import json


class ConsoleWriter:
    def process(self, content):
        print(json.dumps(content))
