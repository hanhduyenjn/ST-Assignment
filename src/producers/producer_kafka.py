import csv
import json
import multiprocessing
import random
import time
import traceback

from kafka import KafkaProducer

from src.common.config import (
    DATA_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_INTERFACE_STATS,
    KAFKA_TOPIC_INVENTORY,
    KAFKA_TOPIC_SYSLOGS,
)

ENCODING_FORMAT = "utf-8"


def _log(name: str, msg: str):
    print(f"[{name}] {msg}", flush=True)



class BaseProducer:
    def __init__(self, topic: str, bootstrap_servers: str = "localhost:9092"):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: str(k).encode(ENCODING_FORMAT),
            value_serializer=lambda v: str(v).encode(ENCODING_FORMAT),
        )

    def produce(self, key, value):
        self.producer.send(self.topic, key=key, value=value)

    def terminate(self):
        self.producer.flush()
        self.producer.close()


class InterfaceStatsProducer(BaseProducer):
    def __init__(self, topic: str, bootstrap_servers: str):
        super().__init__(topic=topic, bootstrap_servers=bootstrap_servers)


class SyslogsProducer(BaseProducer):
    def __init__(self, topic: str, bootstrap_servers: str = "localhost:9092"):
        super().__init__(topic=topic, bootstrap_servers=bootstrap_servers)


class InventoryProducer(BaseProducer):
    def __init__(self, topic: str, bootstrap_servers: str = "localhost:9092"):
        super().__init__(topic=topic, bootstrap_servers=bootstrap_servers)


def produce_interface_stats():
    name = "interface-stats"
    try:
        _log(name, f"connecting to {KAFKA_BOOTSTRAP_SERVERS}, topic={KAFKA_TOPIC_INTERFACE_STATS}")
        producer = InterfaceStatsProducer(
            topic=KAFKA_TOPIC_INTERFACE_STATS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        )
        _log(name, "connected")

        with open(DATA_DIR / "interface_stats.csv", "r") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, start=1):
                producer.produce(key=row["device_id"], value=json.dumps(dict(row)))
                _log(name, f"sent #{i} device_id={row['device_id']}")
                if i % 50 == 0:
                    producer.producer.flush()
                    sleep_s = random.uniform(0.5, 2.0)
                    _log(name, f"flushed {i} messages, sleeping {sleep_s:.2f}s")
                    time.sleep(sleep_s)
        producer.terminate()
        _log(name, "all messages flushed and producer closed")
    except Exception:
        _log(name, "ERROR — traceback below")
        traceback.print_exc()
        raise


def produce_syslogs():
    name = "syslogs"
    try:
        _log(name, f"connecting to {KAFKA_BOOTSTRAP_SERVERS}, topic={KAFKA_TOPIC_SYSLOGS}")
        producer = SyslogsProducer(
            topic=KAFKA_TOPIC_SYSLOGS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        )
        _log(name, "connected")

        with open(DATA_DIR / "syslogs.jsonl", "r") as f:
            lines = f.readlines()
        _log(name, f"loaded {len(lines)} lines from syslogs.jsonl")

        for i, line in enumerate(lines, start=1):
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            producer.produce(key=record["device_id"], value=line)
            _log(name, f"sent #{i} device_id={record['device_id']}")
            if i % 50 == 0:
                producer.producer.flush()
                sleep_s = random.uniform(0.5, 2.0)
                _log(name, f"flushed {i} messages, sleeping {sleep_s:.2f}s")
                time.sleep(sleep_s)

        producer.terminate()
        _log(name, "all messages flushed and producer closed")
    except Exception:
        _log(name, "ERROR — traceback below")
        traceback.print_exc()
        raise


def produce_inventory():
    name = "inventory"
    try:
        _log(name, f"connecting to {KAFKA_BOOTSTRAP_SERVERS}, topic={KAFKA_TOPIC_INVENTORY}")
        producer = InventoryProducer(
            topic=KAFKA_TOPIC_INVENTORY,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        )
        _log(name, "connected")

        with open(DATA_DIR / "device_inventory.csv", "r") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, start=1):
                producer.produce(key=row["device_id"], value=json.dumps(dict(row)))
                _log(name, f"sent #{i} device_id={row['device_id']}")
                if i % 50 == 0:
                    producer.producer.flush()
                    sleep_s = random.uniform(0.5, 2.0)
                    _log(name, f"flushed {i} messages, sleeping {sleep_s:.2f}s")
                    time.sleep(sleep_s)

        producer.terminate()
        _log(name, "all messages flushed and producer closed")
    except Exception:
        _log(name, "ERROR — traceback below")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    processes = [
        multiprocessing.Process(target=produce_interface_stats, name="interface-stats"),
        multiprocessing.Process(target=produce_syslogs, name="syslogs"),
        multiprocessing.Process(target=produce_inventory, name="inventory"),
    ]
    for p in processes:
        p.start()
        print(f"[main] started process {p.name} (pid={p.pid})", flush=True)

    for p in processes:
        p.join()
        status = "OK" if p.exitcode == 0 else f"FAILED (exit code {p.exitcode})"
        print(f"[main] {p.name} finished — {status}", flush=True)
