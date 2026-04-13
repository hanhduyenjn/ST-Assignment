-- Flatline-only anomaly staging table (assignment path).
--
-- This table is populated by src/pipeline/flatline_streaming_v1.py and stores
-- only FLATLINE window events. z-score/iqr now come from per-record ingest
-- scores on silver_interface_stats_src.

CREATE TABLE IF NOT EXISTS network_health.silver_flatline_anomaly_flags
(
    device_id        String,
    site_id          String,
    interface_name   String,
    window_start     DateTime,
    window_end       DateTime,
    anomaly_type     String,     -- 'FLATLINE'
    variance         Float32,    -- variance for flatline window
    mean_util_in     Float32,
    oper_status      Int32,
    detected_at      DateTime
)
ENGINE = MergeTree()
ORDER BY (device_id, interface_name, window_start)
PARTITION BY toYYYYMMDD(window_start)
SETTINGS index_granularity = 8192;
