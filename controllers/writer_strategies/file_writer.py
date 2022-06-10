import os
from json import dump
from flask import request

OUTPUT_DESTINATION = os.getenv("OUTPUT__FILE", os.path.join("/opt", "data"))


class FileWriter:
    def __init__(self):
        self.destination = OUTPUT_DESTINATION
        self.filename = os.path.basename(request.args.get("path"))
        self.append = False

    def process(self, content):
        os.makedirs(os.path.basename(self.destination), exist_ok=True)
        with open(os.path.join(self.destination, self.filename), "a" if self.append else "w") as output_f:
            dump(content, output_f)
            output_f.write("\n")
        self.append = True
