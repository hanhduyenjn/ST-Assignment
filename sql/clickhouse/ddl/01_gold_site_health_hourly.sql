-- Gold layer: hourly site health aggregates.
-- Engine: ReplacingMergeTree(refreshed_at) — each 1-minute MV refresh re-inserts the
-- last 25 h of windows; ReplacingMergeTree keeps the newest version per
-- (site_id, window_start) during background merges.
-- Use SELECT FINAL for always-consistent reads in Grafana.

CREATE TABLE IF NOT EXISTS network_health.gold_site_health_hourly
(
    site_id                   String,
    window_start              DateTime,
    window_end                DateTime,
    avg_util_in               Float32,
    avg_util_out              Float32,
    pct_interfaces_saturated  Float32,   -- fraction of readings with effective_util > 80 %
    critical_syslog_count     UInt32,    -- severity < 3, capped at 4 per formula
    total_interface_count     UInt32,
    pct_interfaces_down       Float32,   -- fraction of readings with oper_status != 1
    refreshed_at              DateTime,  -- set to now() by the MV; version key for dedup
    -- Derived columns — computed on read, never stored
    health_score Float32 ALIAS toFloat32(
        greatest(0, least(100,
            100
            - (pct_interfaces_saturated * 40)
            - (least(toFloat32(critical_syslog_count), 4) * 10)
            - (pct_interfaces_down * 20)
        ))
    ),
    is_degraded UInt8 ALIAS toUInt8(health_score < 40)
)
ENGINE = ReplacingMergeTree(refreshed_at)
ORDER BY (site_id, window_start)
PARTITION BY toYYYYMMDD(window_start)
SETTINGS index_granularity = 8192;
