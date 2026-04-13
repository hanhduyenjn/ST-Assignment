from __future__ import annotations

from pyspark.sql import functions as F

from src.common.config import SAMPLE_FRACTION, SMOKE_MAX_ROWS
from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.pipeline.silver_transforms import build_silver_interface
from src.storage.iceberg_writer import IcebergWriter
from src.transforms.effective_util import effective_util_expr

PENDING_TABLE = "rest.silver.pending_enrichment"
INVENTORY_TABLE = "rest.bronze.inventory"
TARGET_TABLE = "rest.silver.interface_stats"


def run() -> None:
    spark = SparkSessionFactory.create(mode="batch", app_name="pending-enrichment")
    writer = IcebergWriter(spark)

    pending = spark.table(PENDING_TABLE).filter(F.col("enriched") == F.lit(False))
    if SAMPLE_FRACTION < 1.0:
        log.info("pending_enrichment: sampling at fraction=%.2f", SAMPLE_FRACTION)
        pending = pending.sample(fraction=SAMPLE_FRACTION, seed=42).coalesce(4)
    if SMOKE_MAX_ROWS > 0:
        log.info("pending_enrichment: capping to %d rows", SMOKE_MAX_ROWS)
        pending = pending.limit(SMOKE_MAX_ROWS).coalesce(1)
    inventory = spark.table(INVENTORY_TABLE).select("device_id", "site_id", "vendor", "role")

    enriched = (
        pending.join(inventory, on="device_id", how="inner")
        .withColumn("enriched", F.lit(True))
        .withColumn("enriched_at", F.current_timestamp())
    )

    if enriched.isEmpty():
        log.info("No pending records to enrich")
        return

    parsed = (
        enriched
        .select(
            F.from_json(
                F.col("raw_payload"),
                "ts string, device_id string, interface_name string, util_in string, util_out string, admin_status string, oper_status string",
            ).alias("r"),
            "site_id",
            "vendor",
            "role",
            "parked_at",
        )
        .select("r.*", "site_id", "vendor", "role", "parked_at")
        .withColumn("util_in", F.col("util_in").cast("double"))
        .withColumn("util_out", F.col("util_out").cast("double"))
        .withColumn("admin_status", F.col("admin_status").cast("int"))
        .withColumn("oper_status", F.col("oper_status").cast("int"))
        .withColumn("device_ts", F.to_timestamp("ts"))
        .withColumn("effective_util_in", effective_util_expr("util_in"))
        .withColumn("effective_util_out", effective_util_expr("util_out"))
        .withColumn("ingest_z_score", F.lit(0.0))
        .withColumn("ingest_iqr_score", F.lit(0.0))
        .withColumn("ingest_anomaly", F.lit(False))
        .withColumn("ingest_flags", F.array().cast("array<string>"))
        .withColumn("_ingested_at", F.col("parked_at"))
        .withColumn("_partition_date", F.to_date("device_ts"))
    )

    # Project to silver schema using shared silver_transforms logic
    parsed = build_silver_interface(parsed)
    writer.write_dataframe(parsed, TARGET_TABLE)
    log.info("Promoted pending enrichment records to silver.interface_stats")


if __name__ == "__main__":
    run()
