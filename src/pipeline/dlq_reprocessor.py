from __future__ import annotations

from pyspark.sql import functions as F

from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.storage.iceberg_writer import IcebergWriter


def run() -> None:
    spark = SparkSessionFactory.create(mode="batch", app_name="dlq-reprocessor")
    writer = IcebergWriter(spark)

    dlq = spark.table("rest.silver.dlq_quarantine").filter(F.col("reprocessed") == F.lit(False))
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
        writer.write_dataframe(fixable, "rest.silver.interface_stats")
    if not permanent.isEmpty():
        writer.write_dataframe(permanent, "rest.silver.dlq_quarantine")

    log.info("DLQ reprocessing complete")


if __name__ == "__main__":
    run()
