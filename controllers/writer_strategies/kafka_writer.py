import os
import json

from azure.eventhub import EventHubProducerClient, EventData
from .base_writter import BaseWritter


class KafkaWriter(BaseWritter):
    def __init__(self, identifier):
        super().__init__(identifier, destination='kafka')
        self.conn_str = os.getenv("EVENT_HUB__CONNECTION")
        self.eventhub_name = os.getenv("EVENT_HUB__NAME")
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=self.conn_str, eventhub_name=self.eventhub_name
        )

    def process(self, content, offset):
        if self.is_chunk_processed(offset):
            return

        event_data_batch = self.producer.create_batch()
        for item in content:
            event_data_batch.add(EventData(json.dumps(item)))
        self.producer.send_batch(event_data_batch)

        self.set_intermediate_status(offset)

    def finalise(self):
        super().finalise()
        self.producer.close()
