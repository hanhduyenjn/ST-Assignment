from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.common.config import CLICKHOUSE_DB, CLICKHOUSE_HOST, CLICKHOUSE_PORT
from src.common.logging import log
from src.models.isolation_forest import IsolationForestDetector
from src.storage.clickhouse_writer import ClickHouseWriter


LOOKBACK_DAYS = 30
ARTIFACT_DIR = Path("/tmp/network-health-models")
MODEL_ARTIFACT = ARTIFACT_DIR / "isolation_forest.joblib"
RULES_ARTIFACT = ARTIFACT_DIR / "isolation_forest_rules.json"


def _training_sql() -> str:
    return f"""
    SELECT
        device_id,
        interface_name,
        toStartOfHour(device_ts) AS window_start,
        toFloat64(avg(effective_util_in)) AS avg_in,
        toFloat64(avg(effective_util_out)) AS avg_out,
        toFloat64(ifNull(stddevSamp(effective_util_in), 0.0)) AS std_in,
        toFloat64(ifNull(stddevSamp(effective_util_out), 0.0)) AS std_out,
        toFloat64(max(effective_util_in)) AS max_in,
        toFloat64(min(effective_util_in)) AS min_in,
        toFloat64(max(effective_util_out)) AS max_out,
        toFloat64(min(effective_util_out)) AS min_out,
        toFloat64(countIf(oper_status != 1)) AS down_count,
        toFloat64(count()) AS sample_count
    FROM network_health.silver_interface_stats_src
    WHERE
        device_ts >= now() - INTERVAL {LOOKBACK_DAYS} DAY
        AND ifNull(ingest_anomaly, false) = false
        AND isNotNull(device_id)
        AND isNotNull(interface_name)
        AND isNotNull(effective_util_in)
        AND isNotNull(effective_util_out)
    GROUP BY device_id, interface_name, window_start
    """


def run() -> None:
    writer = ClickHouseWriter(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)
    rows = writer.query(_training_sql())
    if not rows:
        log.warning("isolation_forest_train: no training rows found")
        return

    frame = pd.DataFrame(rows)
    features = frame[
        [
            "avg_in",
            "avg_out",
            "std_in",
            "std_out",
            "max_in",
            "min_in",
            "max_out",
            "min_out",
            "down_count",
            "sample_count",
        ]
    ].fillna(0.0)

    detector = IsolationForestDetector(contamination=0.05)
    detector.train(features)
    rules = detector.distill_rules(features)

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    detector.save(MODEL_ARTIFACT)
    detector.save_rules(rules, RULES_ARTIFACT)

    # Sync distilled rules to ClickHouse for streaming pipeline to use
    now = datetime.utcnow().replace(microsecond=0)
    baseline_updates = []

    for device_id, interface_name in frame[["device_id", "interface_name"]].drop_duplicates().values:
        # Create baseline parameter entries for each device/interface combination
        # These get populated across all hour_of_day/day_of_week combinations
        for hour in range(24):
            for day in range(7):
                baseline_updates.append({
                    "device_id": str(device_id),
                    "interface_name": str(interface_name),
                    "hour_of_day": hour,
                    "day_of_week": day,
                    "baseline_mean": 0.0,
                    "baseline_std": 0.0,
                    "iqr_k": 1.5,
                    "isolation_score_threshold": 0.65,
                    "distilled_rules": f'{{"source":"isolation_forest","rules":{str(rules).replace(chr(39), chr(34))}}}',
                    "valid_from": now,
                    "valid_until": datetime(2099, 12, 31, 23, 59, 59),
                })

    if baseline_updates:
        writer.upsert_baseline_params(baseline_updates)
        log.info(
            "isolation_forest_train: synced %s baseline entries to ClickHouse",
            len(baseline_updates),
        )

    log.info(
        "isolation_forest_train: trained with %s windows, model=%s, rules=%s",
        len(features),
        MODEL_ARTIFACT,
        RULES_ARTIFACT,
    )


if __name__ == "__main__":
    run()
