-- Materialized view: silver_anomaly_flags (Iceberg engine) → gold_anomaly_flags
-- Increments on each new streaming flatline event written to silver.

CREATE MATERIALIZED VIEW IF NOT EXISTS network_health.mv_anomaly_summary
TO network_health.gold_anomaly_flags
AS
SELECT
    device_id,
    site_id,
    interface_name,
    window_start,
    window_end,
    anomaly_type,
    -- streaming path only produces FLATLINE; batch jobs write directly
    'LOW_VARIANCE'  AS anomaly_subtype,
    toFloat32(0.0)  AS score,
    toFloat32(mean_util_in) AS mean_util_in,
    toFloat32(0.0)  AS std_util_in,
    detected_at
FROM network_health.silver_anomaly_flags;
