from __future__ import annotations

from pyspark.sql import functions as F

from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.storage.clickhouse_writer import ClickHouseWriter


def run() -> None:
    spark = SparkSessionFactory.create(mode="batch", app_name="gold-recompute")
    clickhouse = ClickHouseWriter(host="clickhouse", port=8123, database="network_health")

    interface_df = spark.table("rest.silver.interface_stats")
    syslogs_df = spark.table("rest.silver.syslogs")

    iface_hour = (
        interface_df.groupBy("site_id", F.window("device_ts", "1 hour").alias("w"))
        .agg(
            F.avg("effective_util_in").alias("avg_util_in"),
            F.avg("effective_util_out").alias("avg_util_out"),
            F.avg(F.when(F.col("effective_util_in") > 80, 1.0).otherwise(0.0)).alias("pct_interfaces_saturated"),
            F.count("*").alias("total_interface_count"),
            F.avg(F.when(F.col("oper_status") != 1, 1.0).otherwise(0.0)).alias("pct_interfaces_down"),
        )
    )

    sys_hour = (
        syslogs_df.groupBy("site_id", F.window("device_ts", "1 hour").alias("w"))
        .agg(F.sum(F.when(F.col("severity") < 3, 1).otherwise(0)).alias("critical_syslog_count"))
    )

    rows = (
        iface_hour.join(sys_hour, on=["site_id", "w"], how="left")
        .fillna({"critical_syslog_count": 0})
        .select(
            "site_id",
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "avg_util_in",
            "avg_util_out",
            "pct_interfaces_saturated",
            "critical_syslog_count",
            "total_interface_count",
            "pct_interfaces_down",
        )
        .toPandas()
        .to_dict(orient="records")
    )

    clickhouse.insert_dicts("gold_site_health_hourly", rows)
    log.info("Recomputed and upserted gold_site_health_hourly")


if __name__ == "__main__":
    run()
