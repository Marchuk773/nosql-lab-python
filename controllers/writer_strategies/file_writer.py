import os
from json import dump

from .base_writter import BaseWritter

OUTPUT_DESTINATION = os.getenv("OUTPUT__FILE", os.path.join("/opt", "data"))


class FileWriter(BaseWritter):
    def __init__(self, identifier):
        super().__init__(identifier, destination='file')
        self.filepath = OUTPUT_DESTINATION
        self.append = False

    def process(self, content, offset):
        if self.is_chunk_processed(offset):
            self.append = True
            return

        os.makedirs(os.path.basename(self.filepath), exist_ok=True)
        with open(os.path.join(self.filepath, f"{self.identifier}_{offset}.json"), "a" if self.append else "w") as output_f:
            dump(content, output_f)
            output_f.write("\n")

        self.append = True
        self.set_intermediate_status(offset)
