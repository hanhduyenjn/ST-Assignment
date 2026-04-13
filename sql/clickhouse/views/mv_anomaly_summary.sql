-- Materialized view: promotes anomalies to gold from two sources:
--   1) deduplicated silver.interface_stats_src minute-points (z/iqr/threshold)
--   2) silver_flatline_anomaly_flags (flatline-only staging)
--
-- Steps:
--  1) Deduplicate silver records to one point per (device, interface, minute)
--     using the latest _ingested_at row.
--  2) Expand ingest_flags from those minute-points.
--  3) Promote to gold in 5-minute windows requiring >=3 violating points.

CREATE MATERIALIZED VIEW IF NOT EXISTS network_health.mv_anomaly_summary
REFRESH EVERY 1 MINUTE APPEND
TO network_health.gold_anomaly_flags
AS
SELECT *
FROM (
    SELECT
        device_id,
        any(site_id) AS site_id,
        interface_name,
        window_start,
        addMinutes(window_start, 5) AS window_end,
        anomaly_type,
        multiIf(
            anomaly_type = 'HIGH_Z_SCORE', 'Z_SCORE_SPIKE',
            anomaly_type = 'IQR_OUTLIER', 'IQR_FENCE_BREACH',
            anomaly_type = 'THRESHOLD_SATURATED', 'SATURATION',
            'INGEST_FLAG'
        ) AS anomaly_subtype,
        toFloat32(max(raw_score)) AS score,
        toFloat32(avg(effective_util_in)) AS mean_util_in,
        toFloat32(stddevPop(effective_util_in)) AS std_util_in,
        max(_ingested_at) AS detected_at
    FROM (
        SELECT
            device_id,
            site_id,
            interface_name,
            effective_util_in,
            latest_ingested_at AS _ingested_at,
            toStartOfInterval(minute_ts, INTERVAL 5 MINUTE) AS window_start,
            anomaly_type,
            if(
                anomaly_type = 'HIGH_Z_SCORE', ingest_z_score,
                if(anomaly_type = 'IQR_OUTLIER', ingest_iqr_score, 0.0)
            ) AS raw_score
        FROM (
            SELECT
                device_id,
                any(site_id) AS site_id,
                interface_name,
                minute_ts,
                argMax(effective_util_in, _ingested_at) AS effective_util_in,
                max(_ingested_at) AS latest_ingested_at,
                argMax(ifNull(ingest_z_score, 0.0), _ingested_at) AS ingest_z_score,
                argMax(ifNull(ingest_iqr_score, 0.0), _ingested_at) AS ingest_iqr_score,
                argMax(ifNull(ingest_flags, []), _ingested_at) AS ingest_flags,
                argMax(ifNull(ingest_anomaly, false), _ingested_at) AS ingest_anomaly
            FROM (
                SELECT
                    device_id,
                    site_id,
                    interface_name,
                    toStartOfMinute(device_ts) AS minute_ts,
                    effective_util_in,
                    _ingested_at,
                    ingest_z_score,
                    ingest_iqr_score,
                    ingest_flags,
                    ingest_anomaly
                FROM network_health.silver_interface_stats_src
                WHERE length(ifNull(device_id, '')) > 0
                  AND length(ifNull(interface_name, '')) > 0
                  AND device_ts >= now() - INTERVAL 25 HOUR
            )
            GROUP BY device_id, interface_name, minute_ts
        )
        ARRAY JOIN ifNull(ingest_flags, []) AS anomaly_type
        WHERE ifNull(ingest_anomaly, false) = true
    )
    GROUP BY device_id, interface_name, anomaly_type, window_start
    HAVING count() >= 3

    UNION ALL

    SELECT
        device_id,
        site_id,
        interface_name,
        window_start,
        window_end,
        'FLATLINE' AS anomaly_type,
        'LOW_VARIANCE' AS anomaly_subtype,
        toFloat32(variance) AS score,
        toFloat32(mean_util_in) AS mean_util_in,
        toFloat32(0.0) AS std_util_in,
        detected_at
    FROM network_health.silver_flatline_anomaly_flags
    WHERE anomaly_type = 'FLATLINE'
      AND detected_at >= now() - INTERVAL 25 HOUR
);
