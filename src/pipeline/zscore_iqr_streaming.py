from __future__ import annotations

import logging
import os
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import CLICKHOUSE_DB, CLICKHOUSE_HOST, CLICKHOUSE_PORT
from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.storage.clickhouse_writer import ClickHouseWriter

SILVER_INTERFACE_STATS = "rest.silver.interface_stats"
ZSCORE_IQR_CHECKPOINT = "s3a://lakehouse/checkpoints/streaming/zscore_iqr"

Z_THRESHOLD = float(os.environ.get("ZSCORE_IQR_Z_THRESHOLD", "2.0"))
IQR_THRESHOLD = float(os.environ.get("ZSCORE_IQR_IQR_THRESHOLD", "1.0"))
WATERMARK_DELAY = os.environ.get("ZSCORE_IQR_WATERMARK_DELAY", "5 minutes")
BACKFILL_HOURS = int(os.environ.get("ZSCORE_IQR_BACKFILL_HOURS", "168"))
MAX_ROWS_PER_INSERT = int(os.environ.get("ZSCORE_IQR_MAX_ROWS_PER_INSERT", "5000"))
BASELINE_TTL_SECONDS = int(os.environ.get("ZSCORE_IQR_BASELINE_TTL_SECONDS", "300"))
ENABLE_BACKFILL = os.environ.get("ZSCORE_IQR_ENABLE_BACKFILL", "false").lower() == "true"

_baseline_params: list[dict] = []
_baseline_loaded_at: float = 0.0


def _load_baseline_params(ch: ClickHouseWriter) -> None:
    global _baseline_loaded_at
    _baseline_params.clear()
    _baseline_params.extend(ch.fetch_baseline_params())
    _baseline_loaded_at = time.monotonic()
    log.info("zscore_iqr.streaming: loaded %s baseline rows", len(_baseline_params))


def _ensure_baseline_fresh(ch: ClickHouseWriter) -> None:
    age = time.monotonic() - _baseline_loaded_at
    if age >= BASELINE_TTL_SECONDS:
        log.info("zscore_iqr.streaming: baseline TTL expired (age=%.0fs), refreshing", age)
        _load_baseline_params(ch)


def _baseline_df(spark) -> DataFrame:
    if not _baseline_params:
        return spark.createDataFrame(
            [],
            "device_id string, interface_name string, hour_of_day int, day_of_week int,"
            " baseline_mean double, baseline_std double, iqr_k double",
        )
    return spark.createDataFrame(_baseline_params).select(
        "device_id",
        "interface_name",
        "hour_of_day",
        "day_of_week",
        "baseline_mean",
        "baseline_std",
        "iqr_k",
    )


def _score_records(batch_df: DataFrame, spark) -> DataFrame:
    bl = _baseline_df(spark).select(
        F.col("device_id").alias("_bl_device_id"),
        F.col("interface_name").alias("_bl_interface_name"),
        F.col("hour_of_day").alias("_bl_hour_of_day"),
        F.col("day_of_week").alias("_bl_day_of_week"),
        F.col("baseline_mean"),
        F.col("baseline_std"),
        F.col("iqr_k"),
    )

    keyed = (
        batch_df
        .filter(F.col("oper_status") == 1)
        .withColumn("_hour_of_day", F.hour("device_ts").cast("int"))
        # Spark dayofweek(): 1=Sun..7=Sat -> convert to 0=Mon..6=Sun
        .withColumn("_day_of_week", ((F.dayofweek("device_ts") + 5) % 7).cast("int"))
    )

    joined = keyed.join(
        bl,
        on=(
            (F.col("device_id") == F.col("_bl_device_id"))
            & (F.col("interface_name") == F.col("_bl_interface_name"))
            & (F.col("_hour_of_day") == F.col("_bl_hour_of_day"))
            & (F.col("_day_of_week") == F.col("_bl_day_of_week"))
        ),
        how="left",
    )

    eps = F.lit(1e-8)
    z_score = F.abs(F.col("effective_util_in") - F.col("baseline_mean")) / (F.col("baseline_std") + eps)
    iqr_score = F.abs(F.col("effective_util_in") - F.col("baseline_mean")) / (F.col("iqr_k") * F.col("baseline_std") + eps)

    return (
        joined
        .withColumn("z_score", F.coalesce(z_score, F.lit(0.0)))
        .withColumn("iqr_score", F.coalesce(iqr_score, F.lit(0.0)))
        .drop("_bl_device_id", "_bl_interface_name", "_bl_hour_of_day", "_bl_day_of_week", "_hour_of_day", "_day_of_week")
    )


def _build_anomaly_events(scored_df: DataFrame) -> DataFrame:
    z_events = scored_df.filter(F.col("z_score") > F.lit(Z_THRESHOLD)).select(
        F.col("device_id"),
        F.col("site_id"),
        F.col("interface_name"),
        F.col("device_ts").alias("window_start"),
        (F.col("device_ts") + F.expr("INTERVAL 1 MINUTE")).alias("window_end"),
        F.lit("HIGH_Z_SCORE").alias("anomaly_type"),
        F.col("z_score").cast("float").alias("variance"),
        F.col("effective_util_in").cast("float").alias("mean_util_in"),
        F.col("oper_status").cast("int").alias("oper_status"),
        F.current_timestamp().alias("detected_at"),
    )

    iqr_events = scored_df.filter(F.col("iqr_score") > F.lit(IQR_THRESHOLD)).select(
        F.col("device_id"),
        F.col("site_id"),
        F.col("interface_name"),
        F.col("device_ts").alias("window_start"),
        (F.col("device_ts") + F.expr("INTERVAL 1 MINUTE")).alias("window_end"),
        F.lit("IQR_OUTLIER").alias("anomaly_type"),
        F.col("iqr_score").cast("float").alias("variance"),
        F.col("effective_util_in").cast("float").alias("mean_util_in"),
        F.col("oper_status").cast("int").alias("oper_status"),
        F.current_timestamp().alias("detected_at"),
    )

    return z_events.unionByName(iqr_events).dropDuplicates([
        "device_id",
        "interface_name",
        "window_start",
        "anomaly_type",
    ])


def _collect_rows(df: DataFrame) -> list[dict]:
    rows = (
        df.select(
            "device_id",
            "site_id",
            "interface_name",
            "window_start",
            "window_end",
            "anomaly_type",
            "variance",
            "mean_util_in",
            "oper_status",
            "detected_at",
        )
        .limit(MAX_ROWS_PER_INSERT)
        .collect()
    )
    return [row.asDict(recursive=True) for row in rows]


def _write_events_batch(batch_df: DataFrame, _: int, spark, ch: ClickHouseWriter) -> None:
    _ensure_baseline_fresh(ch)

    scored = _score_records(batch_df, spark)
    events = _build_anomaly_events(scored)
    rows = _collect_rows(events)
    if not rows:
        return

    ch.insert_dicts("silver_anomaly_flags", rows)
    log.info("zscore_iqr.streaming: wrote %s anomaly events to silver_anomaly_flags", len(rows))


def _bootstrap_backfill(spark, ch: ClickHouseWriter) -> None:
    try:
        existing = ch.query(
            "SELECT count() AS c FROM silver_anomaly_flags "
            "WHERE anomaly_type IN ('HIGH_Z_SCORE', 'IQR_OUTLIER')"
        )
        existing_count = int(existing[0].get("c", 0)) if existing else 0
    except Exception:
        existing_count = 0

    if existing_count > 0:
        log.info("zscore_iqr.backfill: skipped (existing rows=%s)", existing_count)
        return

    base = spark.read.format("iceberg").load(SILVER_INTERFACE_STATS)
    scoped = base.filter(F.col("oper_status") == 1)
    if BACKFILL_HOURS > 0:
        scoped = scoped.filter(
            F.col("device_ts") >= (F.current_timestamp() - F.expr(f"INTERVAL {BACKFILL_HOURS} HOURS"))
        )

    scored = _score_records(scoped, spark)
    events = _build_anomaly_events(scored)
    rows = _collect_rows(events)
    if not rows:
        log.info("zscore_iqr.backfill: no rows produced")
        return

    ch.insert_dicts("silver_anomaly_flags", rows)
    log.info("zscore_iqr.backfill: wrote %s rows to silver_anomaly_flags", len(rows))


def run() -> None:
    if not log.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )

    spark = SparkSessionFactory.create(mode="streaming", app_name="zscore-iqr-streaming")
    ch = ClickHouseWriter(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)
    _load_baseline_params(ch)

    if ENABLE_BACKFILL:
        _bootstrap_backfill(spark, ch)
    else:
        log.info("zscore_iqr.streaming: startup backfill disabled")

    silver_stream = spark.readStream.format("iceberg").load(SILVER_INTERFACE_STATS)

    query = (
        silver_stream
        .withWatermark("device_ts", WATERMARK_DELAY)
        .writeStream
        .outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: _write_events_batch(batch_df, batch_id, spark, ch))
        .option("checkpointLocation", ZSCORE_IQR_CHECKPOINT)
        .start()
    )

    log.info(
        "zscore_iqr streaming started (source=%s, z>%s, iqr>%s, watermark=%s)",
        SILVER_INTERFACE_STATS,
        Z_THRESHOLD,
        IQR_THRESHOLD,
        WATERMARK_DELAY,
    )
    query.awaitTermination()


if __name__ == "__main__":
    run()
