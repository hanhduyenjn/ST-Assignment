from __future__ import annotations

from pyspark.sql import functions as F

from src.common.config import SAMPLE_FRACTION, SMOKE_MAX_ROWS
from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.pipeline.silver_transforms import build_silver_interface
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

    # Join with inventory and add default scoring columns
    enriched = (
        bronze_stats.join(inventory, on="device_id", how="left")
        .withColumn("ingest_z_score", F.lit(0.0))
        .withColumn("ingest_iqr_score", F.lit(0.0))
        .withColumn("ingest_anomaly", F.lit(False))
        .withColumn("ingest_flags", F.array().cast("array<string>"))
    )

    # Project to silver schema using the shared silver_transforms logic
    silver = build_silver_interface(enriched)

    writer.write_dataframe(silver, "rest.silver.interface_stats")

    log.info("Backfill complete: wrote silver.interface_stats")


if __name__ == "__main__":
    run()
