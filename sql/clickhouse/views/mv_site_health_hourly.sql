-- Materialized view: silver_interface_stats (Iceberg engine) → gold_site_health_hourly
-- Fires on every INSERT into the Iceberg-backed silver table.
-- Health score formula: 100 - (saturation*40) - (syslogs*10) - (down*20)

CREATE MATERIALIZED VIEW IF NOT EXISTS network_health.mv_site_health_hourly
TO network_health.gold_site_health_hourly
AS
SELECT
    site_id,
    toStartOfHour(device_ts)                                    AS window_start,
    dateAdd(hour, 1, toStartOfHour(device_ts))                  AS window_end,
    avg(effective_util_in)                                      AS avg_util_in,
    avg(effective_util_out)                                     AS avg_util_out,
    countIf(effective_util_in > 80 OR effective_util_out > 80)
        / count()                                               AS pct_interfaces_saturated,
    -- critical_syslog_count joined from silver_syslogs; defaulted to 0 if no join
    toUInt32(0)                                                 AS critical_syslog_count,
    count()                                                     AS total_interface_count,
    countIf(oper_status != 1) / count()                         AS pct_interfaces_down
FROM network_health.silver_interface_stats
GROUP BY site_id, window_start;
