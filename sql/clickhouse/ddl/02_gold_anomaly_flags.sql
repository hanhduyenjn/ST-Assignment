-- Gold layer: anomaly flags consolidated from silver interface/syslog sources.
-- Engine: ReplacingMergeTree — deduplicates on
-- (device_id, interface_name, anomaly_type, anomaly_subtype, window_start)
-- keeping the row with the latest detected_at (version column).

CREATE TABLE IF NOT EXISTS network_health.gold_anomaly_flags
(
    device_id        String,
    site_id          String,
    interface_name   String,
    window_start     DateTime,
    window_end       DateTime,
    anomaly_type     String,    -- 'FLATLINE' | 'HIGH_Z_SCORE' | 'IQR_OUTLIER' | 'THRESHOLD_SATURATED' | 'CRITICAL_SYSLOG_BURST' | 'MODEL'
    anomaly_subtype  String,    -- subtype per source detector
    score            Float32,   -- isolation score — lower = more anomalous
    mean_util_in     Float32,
    std_util_in      Float32,
    detected_at      DateTime
)
ENGINE = ReplacingMergeTree(detected_at)
ORDER BY (device_id, interface_name, anomaly_type, anomaly_subtype, window_start)
PARTITION BY toYYYYMMDD(window_start)
SETTINGS index_granularity = 8192;
