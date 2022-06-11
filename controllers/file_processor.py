import os
import asyncio

from urllib.parse import urlparse
from threading import Thread
from sodapy import Socrata

from utils.utils import get_chunks
from .writer_strategies.consol_writer import ConsoleWriter
from .writer_strategies.file_writer import FileWriter
from .writer_strategies.kafka_writer import KafkaWriter

OUTPUT_DESTINATION = os.getenv("OUTPUT__PROCESSOR", "console").lower()
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 200))

STRATEGIES = {
    "console": ConsoleWriter,
    "file": FileWriter,
    "kafka": KafkaWriter,
}


class FileProcessor:
    def __init__(self, destination, endpoint):
        self.output_destination = destination if (destination and destination in list(STRATEGIES)) \
            else OUTPUT_DESTINATION
        self.chunk_size = CHUNK_SIZE
        self.url, self.identifier = self._parse_endpoint(endpoint)
        self.processor = STRATEGIES[self.output_destination](self.identifier)

    def process(self):
        if self.processor.already_processed():
            return "Content was already written to {}".format(self.output_destination)
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(self._query_data())
        result = loop.run_until_complete(self._upload(data))
        return result

    async def _upload(self, content):
        threads = []
        try:
            self.processor.set_in_progress_status()
            for data_chunk, offset in get_chunks(content, self.chunk_size):
                thread = Thread(target=self.processor.process, args=(data_chunk, offset))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            self.processor.finalise()
        except Exception as e:
            raise ProcessingError("Error while writing to {}, reason: {}".format(
                self.output_destination, str(e)
            ))
        return "Content successfully written to {}".format(self.output_destination)

    async def _query_data(self):
        with Socrata(self.url, None) as client:
            return client.get_all(self.identifier)

    def _parse_endpoint(self, endpoint):
        url = urlparse(endpoint).netloc
        identifier = os.path.basename(endpoint).split(".")[0]
        return url, identifier


class ProcessingError(Exception):
    def __init__(self, msg):
        self.msg = msg
