-- Refreshable MV to compute weekly baseline versions directly in ClickHouse.
-- Uses APPEND into ReplacingMergeTree(device_baseline_params), where valid_from
-- acts as the version so latest baseline wins after merges.

CREATE MATERIALIZED VIEW IF NOT EXISTS network_health.mv_device_baseline_refresh
REFRESH EVERY 1 WEEK APPEND
TO network_health.device_baseline_params
AS
SELECT
    device_id,
    interface_name,
    toUInt8(toHour(device_ts)) AS hour_of_day,
    toUInt8((toDayOfWeek(device_ts) - 1) % 7) AS day_of_week,
    toFloat32(avg(effective_util_in)) AS baseline_mean,
    toFloat32(ifNull(stddevSamp(effective_util_in), 0.0)) AS baseline_std,
    toFloat32(1.5) AS iqr_k,
    toFloat32(0.65) AS isolation_score_threshold,
    concat(
        '{"source":"weekly_baseline_refresh_mv","lookback_days":30,"sample_count":',
        toString(count()),
        '}'
    ) AS distilled_rules,
    now() AS valid_from,
    toDateTime('2099-12-31 23:59:59') AS valid_until
FROM network_health.silver_interface_stats_src
WHERE
    device_ts >= now() - INTERVAL 30 DAY
    AND ifNull(ingest_anomaly, false) = false
    AND isNotNull(device_id)
    AND isNotNull(interface_name)
    AND isNotNull(effective_util_in)
GROUP BY device_id, interface_name, hour_of_day, day_of_week;
