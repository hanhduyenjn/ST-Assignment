import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
SRC_DIR = BASE_DIR / "src"

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_INTERFACE_STATS = "raw.interface.stats"
KAFKA_TOPIC_SYSLOGS = "raw.syslogs"
KAFKA_TOPIC_INVENTORY = "raw.inventory"
KAFKA_TOPIC_ANOMALY_ALERTS = "output.anomaly-alerts"

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "network_health")

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", ":memory:")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9108"))

ICEBERG_REST_URL = os.environ.get("ICEBERG_REST_CATALOG_URL", "http://iceberg-rest:8181")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")

BATCH_MAX_MESSAGES = int(os.environ.get("BATCH_MAX_MESSAGES", "50000"))
CONSUMER_TIMEOUT_MS = int(os.environ.get("CONSUMER_TIMEOUT_MS", "15000"))

BRONZE_INTERFACE_STATS = "bronze.interface_stats"
BRONZE_SYSLOGS = "bronze.syslogs"
BRONZE_INVENTORY = "bronze.inventory"
