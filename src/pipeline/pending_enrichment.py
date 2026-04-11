from __future__ import annotations

from pyspark.sql import functions as F

from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.storage.iceberg_writer import IcebergWriter

PENDING_TABLE = "rest.silver.pending_enrichment"
INVENTORY_TABLE = "rest.bronze.inventory"
TARGET_TABLE = "rest.silver.interface_stats"


def run() -> None:
    spark = SparkSessionFactory.create(mode="batch", app_name="pending-enrichment")
    writer = IcebergWriter(spark)

    pending = spark.table(PENDING_TABLE).filter(F.col("enriched") == F.lit(False))
    inventory = spark.table(INVENTORY_TABLE).select("device_id", "site_id", "vendor", "role")

    enriched = (
        pending.join(inventory, on="device_id", how="inner")
        .withColumn("enriched", F.lit(True))
        .withColumn("enriched_at", F.current_timestamp())
    )

    if enriched.isEmpty():
        log.info("No pending records to enrich")
        return

    writer.write_dataframe(enriched, TARGET_TABLE)
    log.info("Promoted pending enrichment records to silver.interface_stats")


if __name__ == "__main__":
    run()
