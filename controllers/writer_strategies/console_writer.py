import json


class ConsoleWriter:
    def __init__(self, *args, **kwargs):
        pass

    def process(self, content, offset):
        print(json.dumps(content))

    def finalise(self, *args, **kwargs):
        pass

    def set_in_progress_status(self, *args, **kwargs):
        pass

    def already_processed(self, *args, **kwargs):
        pass
