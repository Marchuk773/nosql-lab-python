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
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 5))

STRATEGIES = {
    "console": ConsoleWriter,
    "file": FileWriter,
    "kafka": KafkaWriter,
}


class FileProcessor:
    def __init__(self, destination):
        self.output_destination = destination if (destination and destination in list(STRATEGIES)) \
            else OUTPUT_DESTINATION
        self.chunk_size = CHUNK_SIZE
        self.processor = STRATEGIES[self.output_destination]()

    def process(self, endpoint):
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(self._query_data(endpoint))
        result = loop.run_until_complete(self._upload(data))
        return result

    async def _upload(self, content):
        threads = []
        try:
            for data_chunk in get_chunks(content, self.chunk_size):
                thread = Thread(target=self.processor.process, args=(data_chunk,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()
        except Exception as e:
            raise ProcessingError("Error while writing to {}, reason: {}".format(
                self.output_destination, str(e)
            ))
        return "File successfully written to {}".format(self.output_destination)

    async def _query_data(self, endpoint):
        url, identifier = self._parse_endpoint(endpoint)
        with Socrata(url, None) as client:
            data = client.get_all(identifier)
        return data

    def _parse_endpoint(self, endpoint):
        url = urlparse(endpoint).netloc
        identifier = os.path.basename(endpoint).split(".")[0]
        return url, identifier


class ProcessingError(Exception):
    def __init__(self, msg):
        self.msg = msg
