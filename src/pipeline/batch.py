from __future__ import annotations

from pyspark.sql import functions as F

from src.common.config import CLICKHOUSE_DB, CLICKHOUSE_HOST, CLICKHOUSE_PORT, SAMPLE_FRACTION, SMOKE_MAX_ROWS
from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.pipeline.silver_transforms import apply_ingest_scores, build_silver_interface, empty_baseline_df
from src.storage.clickhouse_writer import ClickHouseWriter
from src.storage.iceberg_writer import IcebergWriter
from src.transforms.effective_util import effective_util_expr


def run() -> None:
    """Backfill job using SparkSQL window functions (Challenge 1)."""
    spark = SparkSessionFactory.create(mode="batch", app_name="backfill-job")
    writer = IcebergWriter(spark)

    raw = spark.table("rest.bronze.interface_stats")
    if SAMPLE_FRACTION < 1.0:
        log.info("batch: sampling bronze.interface_stats at fraction=%.2f", SAMPLE_FRACTION)
        raw = raw.sample(fraction=SAMPLE_FRACTION, seed=42).coalesce(4)
    if SMOKE_MAX_ROWS > 0:
        log.info("batch: capping bronze.interface_stats to %d rows", SMOKE_MAX_ROWS)
        raw = raw.limit(SMOKE_MAX_ROWS).coalesce(1)

    # Add derived columns using the same logic as streaming pipeline
    bronze_stats = (
        raw
        .withColumn("device_ts", F.to_timestamp("ts"))
        .withColumn("effective_util_in", effective_util_expr("util_in"))
        .withColumn("effective_util_out", effective_util_expr("util_out"))
        .withColumn("_partition_date", F.to_date("device_ts"))
    )

    inventory = spark.table("rest.bronze.inventory").select("device_id", "site_id", "vendor", "role")

    enriched = bronze_stats.join(inventory, on="device_id", how="left")

    # Load baseline params from ClickHouse so z-score / IQR flags are applied.
    # Falls back to an empty baseline (only THRESHOLD_SATURATED can fire) if
    # ClickHouse is unreachable or the table is empty.
    try:
        ch = ClickHouseWriter(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)
        baseline_rows = ch.fetch_baseline_params()
        if baseline_rows:
            baseline_df = spark.createDataFrame(baseline_rows)
            log.info("batch: loaded %d baseline rows from ClickHouse", len(baseline_rows))
        else:
            log.warning("batch: device_baseline_params is empty — only THRESHOLD_SATURATED will fire")
            baseline_df = empty_baseline_df(spark)
    except Exception as exc:
        log.warning("batch: could not load baseline params (%s) — only THRESHOLD_SATURATED will fire", exc)
        baseline_df = empty_baseline_df(spark)

    enriched = apply_ingest_scores(enriched, baseline_df)

    # Project to silver schema using the shared silver_transforms logic
    silver = build_silver_interface(enriched)

    writer.write_dataframe(silver, "rest.silver.interface_stats")

    log.info("Backfill complete: wrote silver.interface_stats")


if __name__ == "__main__":
    run()
