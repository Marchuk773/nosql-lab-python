import os
import redis
from abc import ABC

COMPLETED = "COMPLETED"
IN_PROGRESS = "IN_PROGRESS"


class BaseWritter(ABC):
    def __init__(self, identifier, destination):
        self.identifier = identifier
        self.status_key = f"{identifier}_{destination}_status"
        self.indexes_key = f"{identifier}_{destination}_indexes"
        host = os.getenv("REDIS__HOST")
        port = os.getenv("REDIS__PORT", 6380)
        password = os.getenv("REDIS__PASSWORD")
        self.redis_client = redis.StrictRedis(host=host, port=port,
                                              password=password, ssl=True)

    def finalise(self):
        self.redis_client.delete(self.indexes_key)
        self.set_completed_status()

    def set_in_progress_status(self):
        self.redis_client.set(self.status_key, IN_PROGRESS)

    def already_processed(self):
        redis_value = self.redis_client.get(self.status_key)
        if not redis_value:
            return False

        status = redis_value.decode()
        if status != COMPLETED:
            return False
        return True

    def is_chunk_processed(self, offset):
        processed_batches = set(int(index) for index in self.redis_client.lrange(self.indexes_key, 0, -1))
        if offset in processed_batches:
            return True
        return False

    def set_intermediate_status(self, offset):
        self.redis_client.lpush(self.indexes_key, offset)

    def set_completed_status(self):
        self.redis_client.set(self.status_key, COMPLETED)
