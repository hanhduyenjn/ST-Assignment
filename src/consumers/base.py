from abc import ABC, abstractmethod
from kafka import KafkaConsumer

class BaseConsumer(ABC):
    def __init__(self, catalog, kafka_config: dict):
        self.catalog = catalog
        self.consumer = KafkaConsumer(
            topics=kafka_config["topics"],
            bootstrap_servers=kafka_config["bootstrap_servers"],
            auto_offset_reset=kafka_config["offset_reset"],
            enable_auto_commit=False,
            group_id=kafka_config["group_id"],
            consumer_timeout_ms=kafka_config["consumer_timeout_ms"],
        )

    @abstractmethod
    def poll(self) -> dict:
        pass
