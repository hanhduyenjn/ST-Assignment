-- Materialized views are part of the runtime dataflow and must live in the
-- ClickHouse init directory so they are created on container bootstrap.

CREATE MATERIALIZED VIEW IF NOT EXISTS network_health.mv_site_health_hourly
REFRESH EVERY 1 MINUTE APPEND
TO network_health.gold_site_health_hourly
AS
SELECT
    i.site_id,
    toStartOfHour(i.device_ts)                                        AS window_start,
    addHours(toStartOfHour(i.device_ts), 1)                           AS window_end,
    avg(i.effective_util_in)                                          AS avg_util_in,
    avg(i.effective_util_out)                                         AS avg_util_out,
    countIf(i.effective_util_in > 80 OR i.effective_util_out > 80)
        / count()                                                     AS pct_interfaces_saturated,
    least(toUInt32(any(coalesce(syslog_agg.critical_count, 0))), 4)   AS critical_syslog_count,
    count()                                                           AS total_interface_count,
    countIf(i.oper_status != 1) / count()                             AS pct_interfaces_down,
    now()                                                             AS refreshed_at
FROM network_health.silver_interface_stats_src AS i
LEFT JOIN (
    SELECT
        site_id,
        toStartOfHour(device_ts) AS syslog_hour,
        count()                  AS critical_count
    FROM network_health.silver_syslogs_src
    WHERE is_critical = 1
      AND device_ts >= now() - INTERVAL 25 HOUR
    GROUP BY site_id, syslog_hour
) AS syslog_agg
    ON  syslog_agg.site_id = i.site_id
    AND syslog_agg.syslog_hour = toStartOfHour(i.device_ts)
WHERE i.device_ts >= now() - INTERVAL 25 HOUR
GROUP BY i.site_id, window_start, window_end;

-- Debounced anomaly promotion from:
--   1) per-record ingest flags on silver interface stats (z-score/iqr/threshold)
--   2) flatline-only staging table (legacy assignment path)
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

-- Syslog anomaly monitoring using the same debounced promotion mechanism.
-- Promotes CRITICAL_SYSLOG_BURST when a device emits >=3 critical syslogs in 5 minutes.
CREATE MATERIALIZED VIEW IF NOT EXISTS network_health.mv_syslog_anomaly_summary
REFRESH EVERY 1 MINUTE APPEND
TO network_health.gold_anomaly_flags
AS
SELECT
    device_id,
    any(site_id) AS site_id,
    '' AS interface_name,
    window_start,
    addMinutes(window_start, 5) AS window_end,
    'CRITICAL_SYSLOG_BURST' AS anomaly_type,
    'SEVERITY_LT3_RATE' AS anomaly_subtype,
    toFloat32(count()) AS score,
    toFloat32(0.0) AS mean_util_in,
    toFloat32(0.0) AS std_util_in,
    max(_ingested_at) AS detected_at
FROM (
    SELECT
        device_id,
        site_id,
        _ingested_at,
        toStartOfInterval(device_ts, INTERVAL 5 MINUTE) AS window_start
    FROM network_health.silver_syslogs_src
    WHERE is_critical = 1
      AND length(ifNull(device_id, '')) > 0
      AND device_ts >= now() - INTERVAL 25 HOUR
)
GROUP BY device_id, window_start
HAVING count() >= 3;