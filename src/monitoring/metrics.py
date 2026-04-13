from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

INGEST_RECORDS_TOTAL = Counter(
    "ingest_records_total",
    "Total records observed by streaming ingest",
    ["topic", "status"],
)

DLQ_RECORDS_TOTAL = Counter(
    "dlq_records_total",
    "Total records quarantined to DLQ",
    ["topic", "reason"],
)

ANOMALIES_TOTAL = Counter(
    "anomalies_total",
    "Total anomalies produced",
    ["anomaly_type"],
)

STREAM_BATCH_DURATION_SECONDS = Histogram(
    "stream_batch_duration_seconds",
    "Streaming micro-batch duration",
    ["stage"],
)

LATEST_HEALTH_SCORE = Gauge(
    "latest_site_health_score",
    "Most recent computed health score",
    ["site_id"],
)
