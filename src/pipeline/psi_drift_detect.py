from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.common.config import CLICKHOUSE_DB, CLICKHOUSE_HOST, CLICKHOUSE_PORT
from src.common.logging import log
from src.models.psi_drift_detector import PSIDriftDetector
from src.storage.clickhouse_writer import ClickHouseWriter


LOOKBACK_DAYS = 30
CURRENT_WINDOW_DAYS = 7


def _metrics_sql() -> str:
    return f"""
    SELECT
        device_id,
        interface_name,
        toUInt8(toHour(device_ts)) AS hour_of_day,
        toUInt8((toDayOfWeek(device_ts) - 1) % 7) AS day_of_week,
        toFloat64(effective_util_in) AS metric,
        if(device_ts >= now() - INTERVAL {CURRENT_WINDOW_DAYS} DAY, 'current', 'baseline') AS bucket
    FROM network_health.silver_interface_stats_src
    WHERE
        device_ts >= now() - INTERVAL {LOOKBACK_DAYS} DAY
        AND ifNull(ingest_anomaly, false) = false
        AND isNotNull(device_id)
        AND isNotNull(interface_name)
        AND isNotNull(effective_util_in)
    """


def _latest_baseline_sql() -> str:
    return """
    SELECT
        device_id,
        interface_name,
        hour_of_day,
        day_of_week,
        argMax(baseline_mean, valid_from) AS baseline_mean,
        argMax(baseline_std, valid_from) AS baseline_std,
        argMax(iqr_k, valid_from) AS iqr_k,
        argMax(isolation_score_threshold, valid_from) AS isolation_score_threshold
    FROM network_health.device_baseline_params
    GROUP BY device_id, interface_name, hour_of_day, day_of_week
    """


def run() -> None:
    writer = ClickHouseWriter(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

    metrics_rows = writer.query(_metrics_sql())
    if not metrics_rows:
        log.warning("psi_drift_detect: no metric rows found")
        return

    baseline_rows = writer.query(_latest_baseline_sql())
    baseline_map = {
        (
            str(r["device_id"]),
            str(r["interface_name"]),
            int(r["hour_of_day"]),
            int(r["day_of_week"]),
        ): r
        for r in baseline_rows
    }

    frame = pd.DataFrame(metrics_rows)
    detector = PSIDriftDetector(threshold=0.25, bins=10)

    updates: list[dict[str, object]] = []
    grouped = frame.groupby(["device_id", "interface_name", "hour_of_day", "day_of_week"])
    now = datetime.utcnow().replace(microsecond=0)

    for key, group in grouped:
        baseline_series = group.loc[group["bucket"] == "baseline", "metric"]
        current_series = group.loc[group["bucket"] == "current", "metric"]
        if len(baseline_series) < 10 or len(current_series) < 10:
            continue

        result = detector.compute_psi(baseline_series, current_series)
        if not result.is_significant:
            continue

        prev = baseline_map.get((str(key[0]), str(key[1]), int(key[2]), int(key[3])), {})
        updates.append(
            {
                "device_id": str(key[0]),
                "interface_name": str(key[1]),
                "hour_of_day": int(key[2]),
                "day_of_week": int(key[3]),
                "baseline_mean": float(current_series.mean()),
                "baseline_std": float(current_series.std(ddof=1) if len(current_series) > 1 else 0.0),
                "iqr_k": float(prev.get("iqr_k", 1.5)),
                "isolation_score_threshold": float(prev.get("isolation_score_threshold", 0.65)),
                "distilled_rules": f'{{"source":"psi_drift_detector","psi":{result.psi:.6f}}}',
                "valid_from": now,
                "valid_until": datetime(2099, 12, 31, 23, 59, 59),
            }
        )

    if not updates:
        log.info("psi_drift_detect: no significant drift groups detected")
        return

    writer.upsert_baseline_params(updates)
    log.info("psi_drift_detect: upserted %s baseline updates", len(updates))


if __name__ == "__main__":
    run()
