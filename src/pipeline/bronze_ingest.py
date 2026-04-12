"""Bronze ingest — Spark Structured Streaming.

Reads raw Kafka topics and writes untransformed payloads to Iceberg bronze tables.
Runs as a separate always-on streaming application, independent of the main pipeline.

Replaces the Airflow collect_raw_data DAG. Offsets are checkpoint-managed — no
Airflow orchestration needed. Trigger is aligned with the main pipeline.
"""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_INTERFACE_STATS,
    KAFKA_TOPIC_INVENTORY,
    KAFKA_TOPIC_SYSLOGS,
)
from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.storage.iceberg_writer import IcebergWriter

# ---------------------------------------------------------------------------
# Iceberg table names
# ---------------------------------------------------------------------------
BRONZE_INTERFACE_STATS = "rest.bronze.interface_stats"
BRONZE_SYSLOGS = "rest.bronze.syslogs"
BRONZE_INVENTORY = "rest.bronze.inventory"

# Checkpoint root on MinIO
CHECKPOINT_BASE = "s3a://lakehouse/checkpoints/bronze"

# Trigger — aligned with main streaming pipeline
TRIGGER_INTERVAL = "30 seconds"

KAFKA_GROUP_INTERFACE = "bronze-ingest-interface-v1"
KAFKA_GROUP_SYSLOGS = "bronze-ingest-syslogs-v1"
KAFKA_GROUP_INVENTORY = "bronze-ingest-inventory-v1"

# ---------------------------------------------------------------------------
# Kafka source schemas (PERMISSIVE — extra columns land in _corrupt_record)
# ---------------------------------------------------------------------------
_INTERFACE_STATS_SCHEMA = StructType([
    StructField("ts",             StringType(),  True),
    StructField("device_id",      StringType(),  True),
    StructField("interface_name", StringType(),  True),
    StructField("util_in",        DoubleType(),  True),
    StructField("util_out",       DoubleType(),  True),
    StructField("admin_status",   IntegerType(), True),
    StructField("oper_status",    IntegerType(), True),
])

_SYSLOGS_SCHEMA = StructType([
    StructField("ts",        StringType(),  True),
    StructField("device_id", StringType(),  True),
    StructField("severity",  IntegerType(), True),
    StructField("message",   StringType(),  True),
])

_INVENTORY_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("site_id",   StringType(), True),
    StructField("vendor",    StringType(), True),
    StructField("role",      StringType(), True),
])

# Known columns per schema — everything else becomes _extra_cols
_INTERFACE_STATS_KNOWN = {"ts", "device_id", "interface_name", "util_in", "util_out",
                          "admin_status", "oper_status"}
_SYSLOGS_KNOWN = {"ts", "device_id", "severity", "message"}
_INVENTORY_KNOWN = {"device_id", "site_id", "vendor", "role"}


def _add_metadata(df, source_topic: str):
    """Add _ingested_at, _source_topic, _partition_date metadata columns."""
    return (
        df
        .withColumn("_ingested_at",    F.current_timestamp())
        .withColumn("_source_topic",   F.lit(source_topic))
        .withColumn("_partition_date", F.to_date(F.current_timestamp()))
    )


def _kafka_stream(spark: SparkSession, topic: str, group_id: str):
    """Read a Kafka topic as a streaming DataFrame; expose raw value as string."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("kafka.group.id", group_id)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS _raw_payload", "timestamp AS _kafka_ts")
    )


def _build_interface_stats_stream(spark: SparkSession):
    raw = _kafka_stream(spark, KAFKA_TOPIC_INTERFACE_STATS, KAFKA_GROUP_INTERFACE)

    # Parse known fields and extra fields from the same stream in one projection
    # to avoid unsupported stream-stream joins.
    parsed = (
        raw
        .withColumn("_parsed", F.from_json(F.col("_raw_payload"), _INTERFACE_STATS_SCHEMA))
        .withColumn("_all", F.from_json(F.col("_raw_payload"), MapType(StringType(), StringType())))
    )

    # Create known columns array once (outside lambda) for serialization in streaming context
    known_cols_array = F.array(*[F.lit(c) for c in _INTERFACE_STATS_KNOWN])

    df = parsed.select(
        F.col("_raw_payload"),
        F.col("_parsed.ts").alias("ts"),
        F.col("_parsed.device_id").alias("device_id"),
        F.col("_parsed.interface_name").alias("interface_name"),
        F.col("_parsed.util_in").alias("util_in"),
        F.col("_parsed.util_out").alias("util_out"),
        F.col("_parsed.admin_status").alias("admin_status"),
        F.col("_parsed.oper_status").alias("oper_status"),
        F.map_filter(
            F.col("_all"),
            lambda k, _: ~F.array_contains(known_cols_array, k),
        ).alias("_extra_cols"),
    )

    df = _add_metadata(df, KAFKA_TOPIC_INTERFACE_STATS)
    return df


def _build_syslogs_stream(spark: SparkSession):
    raw = _kafka_stream(spark, KAFKA_TOPIC_SYSLOGS, KAFKA_GROUP_SYSLOGS)
    parsed = (
        raw
        .withColumn("_parsed", F.from_json(F.col("_raw_payload"), _SYSLOGS_SCHEMA))
        .withColumn("_all", F.from_json(F.col("_raw_payload"), MapType(StringType(), StringType())))
    )

    # Create known columns array once (outside lambda) for serialization in streaming context
    known_cols_array = F.array(*[F.lit(c) for c in _SYSLOGS_KNOWN])

    df = (
        parsed
        .select(
            F.col("_raw_payload"),
            F.col("_parsed.ts").alias("ts"),
            F.col("_parsed.device_id").alias("device_id"),
            F.col("_parsed.severity").alias("severity"),
            F.col("_parsed.message").alias("message"),
            F.map_filter(
                F.col("_all"),
                lambda k, _: ~F.array_contains(known_cols_array, k),
            ).alias("_extra_cols"),
        )
    )
    df = _add_metadata(df, KAFKA_TOPIC_SYSLOGS)
    return df


def _build_inventory_stream(spark: SparkSession):
    raw = _kafka_stream(spark, KAFKA_TOPIC_INVENTORY, KAFKA_GROUP_INVENTORY)
    parsed = (
        raw
        .withColumn("_parsed", F.from_json(F.col("_raw_payload"), _INVENTORY_SCHEMA))
        .withColumn("_all", F.from_json(F.col("_raw_payload"), MapType(StringType(), StringType())))
    )

    # Create known columns array once (outside lambda) for serialization in streaming context
    known_cols_array = F.array(*[F.lit(c) for c in _INVENTORY_KNOWN])

    df = (
        parsed
        .select(
            F.col("_parsed.device_id").alias("device_id"),
            F.col("_parsed.site_id").alias("site_id"),
            F.col("_parsed.vendor").alias("vendor"),
            F.col("_parsed.role").alias("role"),
            F.map_filter(
                F.col("_all"),
                lambda k, _: ~F.array_contains(known_cols_array, k),
            ).alias("_extra_cols"),
        )
    )
    df = _add_metadata(df, KAFKA_TOPIC_INVENTORY)
    return df


def _align_to_table_schema(df, spark: SparkSession, target_table: str):
    """Align a micro-batch to the target Iceberg table columns.

    This makes ingestion resilient when consumer payload shape evolves before
    DDL changes are applied (extra columns are dropped; missing columns filled null).
    """
    target_cols = [field.name for field in spark.table(target_table).schema.fields]
    for col_name in target_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None))
    return df.select(*target_cols)


def _ensure_table_schema(df, spark: SparkSession, target_table: str) -> None:
    """Evolve Iceberg table schema for new incoming columns.

    If the incoming micro-batch has columns that are not in the target table,
    add them with inferred Spark SQL types before append.
    """
    target_cols = {field.name for field in spark.table(target_table).schema.fields}
    incoming_fields = {field.name: field.dataType.simpleString() for field in df.schema.fields}

    for col_name, sql_type in incoming_fields.items():
        if col_name not in target_cols:
            spark.sql(f"ALTER TABLE {target_table} ADD COLUMN {col_name} {sql_type}")
            log.info("bronze.schema: added column %s %s to %s", col_name, sql_type, target_table)


def _append_batch(batch_df, _: int, writer: IcebergWriter, target_table: str, stream_name: str) -> None:
    if batch_df.isEmpty():
        return
    _ensure_table_schema(batch_df, writer.spark, target_table)
    aligned = _align_to_table_schema(batch_df, writer.spark, target_table)
    row_count = int(aligned.count())
    writer.write_dataframe(aligned, target_table)
    log.info("bronze.%s: appended %s rows", stream_name, row_count)


def run():
    log.info("Starting bronze ingest streaming job")
    spark = SparkSessionFactory.create(mode="streaming", app_name="bronze-ingest")
    writer = IcebergWriter(spark)

    queries = []

    # --- interface_stats ---
    stats_df = _build_interface_stats_stream(spark)
    q_stats = (
        stats_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: _append_batch(
                batch_df,
                batch_id,
                writer,
                BRONZE_INTERFACE_STATS,
                "interface_stats",
            )
        )
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/interface_stats")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    queries.append(q_stats)
    log.info("bronze.interface_stats stream started")

    # --- syslogs ---
    syslogs_df = _build_syslogs_stream(spark)
    q_syslogs = (
        syslogs_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: _append_batch(
                batch_df,
                batch_id,
                writer,
                BRONZE_SYSLOGS,
                "syslogs",
            )
        )
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/syslogs")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    queries.append(q_syslogs)
    log.info("bronze.syslogs stream started")

    # --- inventory ---
    inventory_df = _build_inventory_stream(spark)
    q_inventory = (
        inventory_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: _append_batch(
                batch_df,
                batch_id,
                writer,
                BRONZE_INVENTORY,
                "inventory",
            )
        )
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/inventory")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    queries.append(q_inventory)
    log.info("bronze.inventory stream started")

    # Block until all queries terminate (or fail)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    run()
