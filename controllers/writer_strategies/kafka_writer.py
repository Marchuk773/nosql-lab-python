import os
import json

from azure.eventhub import EventHubProducerClient, EventData


class KafkaWriter:
    def __init__(self):
        self.conn_str = os.getenv("EVENT_HUB__CONNECTION")
        self.eventhub_name = os.getenv("EVENT_HUB__NAME")

    def process(self, content):
        producer = EventHubProducerClient.from_connection_string(
            conn_str=self.conn_str, eventhub_name=self.eventhub_name)
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(json.dumps(content)))
        with producer:
            producer.send_batch(event_data_batch)
