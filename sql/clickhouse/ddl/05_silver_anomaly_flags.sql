-- ClickHouse staging table for anomaly flags written by batch jobs.
-- mv_anomaly_summary fires on every INSERT here and populates gold_anomaly_flags.
--
-- Written by:
--   nightly batch  → FlatlineDetector (anomaly_type = FLATLINE)
--   nightly batch  → IsolationForestDetector (anomaly_type = MODEL)
--
-- This is NOT an Iceberg-backed table — batch jobs write directly via ClickHouseWriter
-- so that the regular (non-refreshable) mv_anomaly_summary MV fires on each INSERT.

CREATE TABLE IF NOT EXISTS network_health.silver_anomaly_flags
(
    device_id        String,
    site_id          String,
    interface_name   String,
    window_start     DateTime,
    window_end       DateTime,
    anomaly_type     String,     -- 'FLATLINE' | 'MODEL'
    variance         Float32,    -- 0.0 for FLATLINE; isolation score for MODEL
    mean_util_in     Float32,
    oper_status      Int32,
    detected_at      DateTime
)
ENGINE = MergeTree()
ORDER BY (device_id, interface_name, window_start)
PARTITION BY toYYYYMMDD(window_start)
SETTINGS index_granularity = 8192;
