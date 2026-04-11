from __future__ import annotations

from pyspark.sql import functions as F

from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.storage.iceberg_writer import IcebergWriter


def run() -> None:
    """Backfill job using SparkSQL window functions (Challenge 1)."""
    spark = SparkSessionFactory.create(mode="batch", app_name="backfill-job")
    writer = IcebergWriter(spark)

    bronze_stats = (
        spark.table("rest.bronze.interface_stats")
        .withColumn("device_ts", F.to_timestamp("ts"))
        .withColumn("effective_util_in", F.when(F.col("oper_status").isin(2, 3), 0.0).otherwise(F.col("util_in")))
        .withColumn("effective_util_out", F.when(F.col("oper_status").isin(2, 3), 0.0).otherwise(F.col("util_out")))
    )

    inventory = spark.table("rest.bronze.inventory").select("device_id", "site_id", "vendor", "role")

    silver = (
        bronze_stats.join(inventory, on="device_id", how="left")
        .withColumn("_partition_date", F.to_date("device_ts"))
    )

    writer.write_dataframe(silver, "rest.silver.interface_stats")

    hourly = silver.groupBy("site_id", F.window("device_ts", "1 hour").alias("w")).agg(
        F.avg("effective_util_in").alias("avg_util_in"),
        F.avg("effective_util_out").alias("avg_util_out"),
    )
    writer.write_dataframe(hourly, "rest.silver.site_health_hourly")

    log.info("Backfill complete")


if __name__ == "__main__":
    run()
