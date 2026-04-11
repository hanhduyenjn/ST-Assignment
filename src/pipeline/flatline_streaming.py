"""Flatline detection as a long-lived Spark Structured Streaming job.

Reads silver.interface_stats as an Iceberg streaming source, applies a
4-hour tumbling window with watermark, and writes completed flatline windows
directly to ClickHouse silver_anomaly_flags.  The mv_anomaly_summary
materialized view then fires on each INSERT and populates gold_anomaly_flags.

Why a separate job (not inside the main streaming pipeline):
  The main pipeline's foreachBatch sees only ~30 s of micro-batch data —
  far too narrow for a 4-hour variance window.  Reading silver.interface_stats
  as a dedicated Iceberg stream gives Spark the full historical window needed
  to emit correct per-device flatline results once the window closes.

Watermark: 10 minutes — allows records up to 10 min late before a window is
sealed.  After the watermark advances past (window_end + 10 min), Spark emits
the final aggregation and it is written to ClickHouse.

Detection latency: window_end + watermark_delay ≈ up to 4h 10m after the
flatline starts.  Acceptable for operational alerting; far better than nightly.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import CLICKHOUSE_DB, CLICKHOUSE_HOST, CLICKHOUSE_PORT
from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.storage.clickhouse_writer import ClickHouseWriter

SILVER_INTERFACE_STATS = "rest.silver.interface_stats"
FLATLINE_CHECKPOINT    = "s3a://lakehouse/checkpoints/streaming/flatline"

WATERMARK_DELAY = "10 minutes"
WINDOW_DURATION = "4 hours"


def _write_flatline_batch(batch_df: DataFrame, _: int, ch: ClickHouseWriter) -> None:
    """foreachBatch handler: collect closed flatline windows and push to ClickHouse."""
    if batch_df.isEmpty():
        return

    rows = (
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
        .toPandas()
        .to_dict(orient="records")
    )

    # INSERT fires mv_anomaly_summary → gold_anomaly_flags in ClickHouse
    ch.insert_dicts("silver_anomaly_flags", rows)
    log.info("flatline.streaming: wrote %s flatline windows to silver_anomaly_flags", len(rows))


def run() -> None:
    if not log.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )

    spark = SparkSessionFactory.create(mode="streaming", app_name="flatline-streaming")
    ch = ClickHouseWriter(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

    # Iceberg streaming source — Spark tracks offsets via checkpoint, no manual
    # offset management needed.  New Iceberg snapshots committed by the main
    # streaming pipeline are picked up automatically.
    silver_stream = spark.readStream.format("iceberg").load(SILVER_INTERFACE_STATS)

    # Only UP interfaces (oper_status == 1) can flatline meaningfully.
    # withWatermark seals a window once event-time advances past window_end + delay.
    # outputMode("append") emits only sealed, finalized windows.
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
        .filter(F.col("variance") == 0.0)
    )

    query = (
        flatline_stream.writeStream
        .outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: _write_flatline_batch(batch_df, batch_id, ch))
        .option("checkpointLocation", FLATLINE_CHECKPOINT)
        .start()
    )

    log.info("Flatline streaming pipeline started (source: %s, window: %s)", SILVER_INTERFACE_STATS, WINDOW_DURATION)
    query.awaitTermination()


if __name__ == "__main__":
    run()
