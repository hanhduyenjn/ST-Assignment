from __future__ import annotations

from pyspark.sql import functions as F

from src.common.config import SAMPLE_FRACTION, SMOKE_MAX_ROWS
from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.pipeline.silver_transforms import build_silver_interface
from src.storage.iceberg_writer import IcebergWriter
from src.transforms.effective_util import effective_util_expr


def run() -> None:
    spark = SparkSessionFactory.create(mode="batch", app_name="dlq-reprocessor")
    writer = IcebergWriter(spark)

    dlq = spark.table("rest.silver.dlq_quarantine").filter(F.col("reprocessed") == F.lit(False))
    if SAMPLE_FRACTION < 1.0:
        log.info("dlq_reprocessor: sampling dlq_quarantine at fraction=%.2f", SAMPLE_FRACTION)
        dlq = dlq.sample(fraction=SAMPLE_FRACTION, seed=42).coalesce(4)
    if SMOKE_MAX_ROWS > 0:
        log.info("dlq_reprocessor: capping dlq_quarantine to %d rows", SMOKE_MAX_ROWS)
        dlq = dlq.limit(SMOKE_MAX_ROWS).coalesce(1)
    inventory = spark.table("rest.bronze.inventory").select("device_id")

    fixable = (
        dlq.withColumn("device_id", F.get_json_object("raw_payload", "$.device_id"))
        .join(inventory, on="device_id", how="inner")
        .withColumn("reprocessed", F.lit(True))
        .withColumn("reprocessed_at", F.current_timestamp())
        .withColumn("quarantine_resolution", F.lit("PROMOTED_TO_SILVER"))
    )

    permanent = (
        dlq.withColumn("device_id", F.get_json_object("raw_payload", "$.device_id"))
        .join(inventory, on="device_id", how="left_anti")
        .withColumn("reprocessed", F.lit(True))
        .withColumn("reprocessed_at", F.current_timestamp())
        .withColumn("quarantine_resolution", F.lit("PERMANENT"))
    )

    if not fixable.isEmpty():
        promoted = (
            fixable
            .select(
                F.from_json(
                    F.col("raw_payload"),
                    "ts string, device_id string, interface_name string, util_in string, util_out string, admin_status string, oper_status string",
                ).alias("r"),
                "quarantined_at",
            )
            .select("r.*", "quarantined_at")
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
            .withColumn("_partition_date", F.to_date("device_ts"))
            .withColumn("site_id", F.lit(None).cast("string"))
            .withColumn("vendor", F.lit(None).cast("string"))
            .withColumn("role", F.lit(None).cast("string"))
        )
        # Project to silver schema using shared silver_transforms logic
        promoted = build_silver_interface(promoted.withColumn("_ingested_at", F.col("quarantined_at")))
        writer.write_dataframe(promoted, "rest.silver.interface_stats")
    if not permanent.isEmpty():
        writer.write_dataframe(
            permanent.select(
                "raw_payload",
                "quarantine_reason",
                "source_topic",
                "quarantined_at",
                "reprocessed",
                "reprocessed_at",
                "quarantine_resolution",
                "_partition_date",
            ),
            "rest.silver.dlq_quarantine",
        )

    log.info("DLQ reprocessing complete")


if __name__ == "__main__":
    run()
