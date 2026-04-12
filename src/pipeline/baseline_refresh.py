from __future__ import annotations

import socket

from src.common.config import CLICKHOUSE_DB, CLICKHOUSE_HOST, CLICKHOUSE_PORT
from src.common.logging import log
from src.storage.clickhouse_writer import ClickHouseWriter


LOOKBACK_DAYS = 30


def _resolve_clickhouse_host(host: str) -> str:
    if host != "clickhouse":
        return host
    try:
        socket.gethostbyname(host)
        return host
    except OSError:
        # Host runs cannot resolve Docker service DNS names.
        return "localhost"


def _candidate_groups_sql() -> str:
    return f"""
    SELECT count() AS groups
    FROM (
        SELECT
            device_id,
            interface_name,
            toUInt8(toHour(device_ts)) AS hour_of_day,
            toUInt8((toDayOfWeek(device_ts) - 1) % 7) AS day_of_week
        FROM network_health.silver_interface_stats_src
        WHERE
            device_ts >= now() - INTERVAL {LOOKBACK_DAYS} DAY
            AND ifNull(ingest_anomaly, false) = false
            AND isNotNull(device_id)
            AND isNotNull(interface_name)
            AND isNotNull(effective_util_in)
        GROUP BY device_id, interface_name, hour_of_day, day_of_week
    )
    """


def _insert_baselines_sql() -> str:
    return f"""
    INSERT INTO network_health.device_baseline_params
    (
        device_id,
        interface_name,
        hour_of_day,
        day_of_week,
        baseline_mean,
        baseline_std,
        iqr_k,
        isolation_score_threshold,
        distilled_rules,
        valid_from,
        valid_until
    )
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
            '{{"source":"weekly_baseline_refresh","lookback_days":',
            toString({LOOKBACK_DAYS}),
            ',"sample_count":',
            toString(count()),
            '}}'
        ) AS distilled_rules,
        now() AS valid_from,
        toDateTime('2099-12-31 23:59:59') AS valid_until
    FROM network_health.silver_interface_stats_src
    WHERE
        device_ts >= now() - INTERVAL {LOOKBACK_DAYS} DAY
        AND ifNull(ingest_anomaly, false) = false
        AND isNotNull(device_id)
        AND isNotNull(interface_name)
        AND isNotNull(effective_util_in)
    GROUP BY device_id, interface_name, hour_of_day, day_of_week
    """


def run() -> None:
    host = _resolve_clickhouse_host(CLICKHOUSE_HOST)
    writer = ClickHouseWriter(host=host, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

    group_rows = writer.query(_candidate_groups_sql())
    groups = int(group_rows[0]["groups"]) if group_rows else 0
    if groups == 0:
        log.warning("baseline_refresh: no confirmed-normal rows found in silver_interface_stats_src")
        return

    writer.client.command(_insert_baselines_sql())
    log.info("baseline_refresh: wrote %s baseline groups to ClickHouse", groups)


if __name__ == "__main__":
    run()
