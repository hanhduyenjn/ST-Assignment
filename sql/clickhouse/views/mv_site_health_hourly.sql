-- REFRESHABLE MATERIALIZED VIEW: Iceberg silver tables → gold_site_health_hourly
--
-- Runs every 1 minute (APPEND mode).  Each refresh recomputes the last 25 hours so
-- that the current incomplete hour and the just-closed hour are always up to date.
-- ReplacingMergeTree(refreshed_at) on the target table deduplicates on background merge;
-- use SELECT FINAL in Grafana queries to always see the latest version per window.
--
-- Health score formula (see IMPLEMENTATION_PLAN.md §14):
--   100 - (pct_saturated × 40) - (min(critical_syslogs, 4) × 10) - (pct_down × 20)
--   Clamped to [0, 100].  is_degraded = score < 40.
--
-- Prerequisites: ClickHouse v23.8+ (REFRESHABLE MV), silver_interface_stats_src and
-- silver_syslogs_src tables created by 00_silver_*.sql DDL files.

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
    -- any() is safe: syslog_agg is already 1 row per (site_id, hour) from the subquery
    least(toUInt32(any(coalesce(syslog_agg.critical_count, 0))), 4)   AS critical_syslog_count,
    count()                                                           AS total_interface_count,
    countIf(i.oper_status != 1) / count()                             AS pct_interfaces_down,
    now()                                                             AS refreshed_at
FROM network_health.silver_interface_stats_src AS i
LEFT JOIN (
    -- Pre-aggregate syslogs to one row per (site_id, hour) before the outer join
    SELECT
        site_id,
        toStartOfHour(device_ts)  AS syslog_hour,
        count()                   AS critical_count
    FROM network_health.silver_syslogs_src
    WHERE is_critical = 1
      AND device_ts >= now() - INTERVAL 25 HOUR
    GROUP BY site_id, syslog_hour
) AS syslog_agg
    ON  syslog_agg.site_id    = i.site_id
    AND syslog_agg.syslog_hour = toStartOfHour(i.device_ts)
WHERE i.device_ts >= now() - INTERVAL 25 HOUR
GROUP BY i.site_id, window_start, window_end;
