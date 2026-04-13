"""Flatline Detection via Spark Structured Streaming (ACTIVE IMPLEMENTATION).

This is a standalone, always-on streaming job that continuously monitors for flatlines.
It uses Spark's distributed processing to detect when metrics become flat (zero variance).

Architecture:
  - Reads from: silver.interface_stats (Iceberg streaming source)
  - Window: 4-hour tumbling (non-overlapping) per device+interface
  - Detection: variance <= threshold (default 0.0)
  - Output: Writes anomaly flags to ClickHouse in real-time
  - State: Checkpoint stored in S3 for recovery

Alternative Implementation (NOT YET INTEGRATED):
  See src/models/flatline_detector.py which provides:
  - FlatlineDetector class with Welford online variance algorithm
  - O(1) streaming state per detector
  - Pluggable AnomalyDetector interface for future use

To run this job: python main.py --mode flatline-streaming
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import CLICKHOUSE_DB, CLICKHOUSE_HOST, CLICKHOUSE_PORT
from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.storage.clickhouse_writer import ClickHouseWriter

SILVER_INTERFACE_STATS = "rest.silver.interface_stats"
FLATLINE_CHECKPOINT    = "s3a://lakehouse/checkpoints/streaming/flatline"
FLATLINE_ANOMALY_TABLE = "silver_flatline_anomaly_flags"

WATERMARK_DELAY = os.environ.get("FLATLINE_WATERMARK_DELAY", "10 minutes")
WINDOW_DURATION = os.environ.get("FLATLINE_WINDOW_DURATION", "4 hours")
VARIANCE_THRESHOLD = float(os.environ.get("FLATLINE_VARIANCE_THRESHOLD", "0.0"))
BACKFILL_HOURS = int(os.environ.get("FLATLINE_BACKFILL_HOURS", "168"))
MAX_ROWS_PER_INSERT = int(os.environ.get("FLATLINE_MAX_ROWS_PER_INSERT", "50000"))


def _collect_flatline_rows(batch_df: DataFrame) -> list[dict]:
    return (
        batch_df
        .select(
            "device_id",
            "site_id",
            "interface_name",
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.lit("FLATLINE").alias("anomaly_type"),
            "variance",
            "mean_util_in",
            "oper_status",
            F.current_timestamp().alias("detected_at"),
        )
        .limit(MAX_ROWS_PER_INSERT)
        .toPandas()
        .to_dict(orient="records")
    )


def _write_flatline_batch(batch_df: DataFrame, _: int, ch: ClickHouseWriter) -> None:
    if batch_df.isEmpty():
        return

    rows = _collect_flatline_rows(batch_df)
    if not rows:
        return

    ch.insert_dicts(FLATLINE_ANOMALY_TABLE, rows)
    log.info("flatline.streaming: wrote %s flatline windows to %s", len(rows), FLATLINE_ANOMALY_TABLE)


def _bootstrap_backfill(spark, ch: ClickHouseWriter) -> None:
    try:
        existing = ch.query(
            f"SELECT count() AS c FROM {FLATLINE_ANOMALY_TABLE} WHERE anomaly_type = 'FLATLINE'"
        )
        existing_count = int(existing[0].get("c", 0)) if existing else 0
    except Exception:
        existing_count = 0

    if existing_count > 0:
        log.info("flatline.backfill: skipped (existing FLATLINE rows=%s)", existing_count)
        return

    base = spark.read.format("iceberg").load(SILVER_INTERFACE_STATS)
    scoped = base.filter(F.col("oper_status") == 1)
    if BACKFILL_HOURS > 0:
        scoped = scoped.filter(F.col("device_ts") >= (F.current_timestamp() - F.expr(f"INTERVAL {BACKFILL_HOURS} HOURS")))

    backfill_flatline = (
        scoped
        .groupBy(
            "device_id",
            "site_id",
            "interface_name",
            F.window("device_ts", WINDOW_DURATION).alias("w"),
        )
        .agg(
            F.coalesce(F.variance("effective_util_in"), F.lit(0.0)).alias("variance"),
            F.avg("effective_util_in").alias("mean_util_in"),
            F.first("oper_status").alias("oper_status"),
        )
        .filter(F.col("variance") <= F.lit(VARIANCE_THRESHOLD))
    )

    rows = _collect_flatline_rows(backfill_flatline)
    if not rows:
        log.info("flatline.backfill: no rows produced")
        return

    ch.insert_dicts(FLATLINE_ANOMALY_TABLE, rows)
    log.info("flatline.backfill: wrote %s rows to %s", len(rows), FLATLINE_ANOMALY_TABLE)


def run() -> None:
    if not log.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )

    spark = SparkSessionFactory.create(mode="streaming", app_name="flatline-streaming")
    ch = ClickHouseWriter(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)
    _bootstrap_backfill(spark, ch)

    silver_stream = spark.readStream.format("iceberg").load(SILVER_INTERFACE_STATS)

    flatline_stream = (
        silver_stream
        .filter(F.col("oper_status") == 1)
        .withWatermark("device_ts", WATERMARK_DELAY)
        .groupBy(
            "device_id",
            "site_id",
            "interface_name",
            F.window("device_ts", WINDOW_DURATION).alias("w"),
        )
        .agg(
            F.coalesce(F.variance("effective_util_in"), F.lit(0.0)).alias("variance"),
            F.avg("effective_util_in").alias("mean_util_in"),
            F.first("oper_status").alias("oper_status"),
        )
        .filter(F.col("variance") <= F.lit(VARIANCE_THRESHOLD))
    )

    query = (
        flatline_stream.writeStream
        .outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: _write_flatline_batch(batch_df, batch_id, ch))
        .option("checkpointLocation", FLATLINE_CHECKPOINT)
        .start()
    )

    log.info(
        "Flatline streaming started (window=%s, watermark=%s, variance<=%s)",
        WINDOW_DURATION,
        WATERMARK_DELAY,
        VARIANCE_THRESHOLD,
    )
    query.awaitTermination()


if __name__ == "__main__":
    run()
