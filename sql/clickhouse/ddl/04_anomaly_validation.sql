-- Cross-validation between ingest-time and batch anomaly decisions.
-- Written by the nightly validation job. Used to compute false positive / miss rates
-- shown in the data quality Grafana dashboard.

CREATE TABLE IF NOT EXISTS network_health.anomaly_validation
(
    device_id       String,
    interface_name  String,
    window_start    DateTime,
    ingest_flagged  UInt8,     -- 1 if streaming Stage 3 flagged this window
    batch_flagged   UInt8,     -- 1 if batch IsolationForest flagged this window
    agreement       String,    -- 'CONFIRMED' | 'FALSE_POSITIVE' | 'MISSED' | 'BOTH_CLEAN'
    validated_at    DateTime
)
ENGINE = MergeTree()
ORDER BY (device_id, interface_name, window_start)
PARTITION BY toYYYYMMDD(window_start)
SETTINGS index_granularity = 8192;
