-- ClickHouse read-only view of Iceberg silver.dlq_quarantine via the Iceberg table engine.
-- Used by Grafana data-quality panels for reprocessing success and quarantine insights.

CREATE TABLE IF NOT EXISTS network_health.silver_dlq_quarantine_src
ENGINE = Iceberg(
    'http://minio:9000/lakehouse/silver/dlq_quarantine/',
    'minioadmin',
    'minioadmin123'
);