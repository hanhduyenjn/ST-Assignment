from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
SRC_DIR = BASE_DIR / "src"


# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_INTERFACE_STATS = "raw.interface.stats"
KAFKA_TOPIC_SYSLOGS = "raw.syslogs"
KAFKA_TOPIC_INVENTORY = "raw.inventory"
KAFKA_TOPIC_ANOMALY_ALERTS = "output.anomaly-alerts"
