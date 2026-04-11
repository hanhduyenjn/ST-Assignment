from src.consumers.base import BaseConsumer
from src.common.logging import log

class JsonlConsumer(BaseConsumer):
    def poll(self) -> dict:
        low_volume_count = 0
        MAX_LOW_VOLUME_ATTEMPTS = 5
        MIN_MESSAGE_THRESHOLD = 5

        TIMEOUT = 1000  # ms
        MAX_REDORDS = 10

        try:

            all_messages = []

            while low_volume_count < MAX_LOW_VOLUME_ATTEMPTS:
                records = self.consumer.poll(timeout_ms=TIMEOUT, max_records=MAX_REDORDS)
                all_messages.extend(records)

                break
        except Exception as exc:
            self.consumer.close()

        return all_messages

                # for msg in self.consumer:
                #     raw_messages.append(msg.value)
                #     if len(raw_messages) >= MIN_MESSAGE_THRESHOLD:
                #         break

                # if len(raw_messages) < MIN_MESSAGE_THRESHOLD:
                #     low_volume_count += 1
                #     log.info("Low volume detected: %d messages. Attempt %d/%d",
                #              len(raw_messages), low_volume_count, MAX_LOW_VOLUME_ATTEMPTS)
                # else:
                #     low_volume_count = 0
                #     log.info("Received %d messages", len(raw_messages))
                #     return {"messages": raw_messages}


test = JsonlConsumer(catalog=None, kafka_config={
    "topics": "raw.interface.stats",
    "bootstrap_servers": "localhost:9092",
    "offset_reset": "earliest",
    "group_id": "jsonl-consumer-test-1",
    "consumer_timeout_ms": 1000,
})

all_messages = test.poll()

print(all_messages)