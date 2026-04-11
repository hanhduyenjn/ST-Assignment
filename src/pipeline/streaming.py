from __future__ import annotations

import logging
import threading
import time
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import (
    CLICKHOUSE_DB,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_INTERFACE_STATS,
    KAFKA_TOPIC_INVENTORY,
    KAFKA_TOPIC_SYSLOGS,
)
from src.common.logging import log
from src.common.spark_session import SparkSessionFactory
from src.storage.clickhouse_writer import ClickHouseWriter
from src.storage.iceberg_writer import IcebergWriter
from src.transforms.effective_util import effective_util_expr
from src.validators.spark_expressions import interface_stats_validation_reason, syslogs_validation_reason

SILVER_INTERFACE_STATS = "rest.silver.interface_stats"
SILVER_SYSLOGS = "rest.silver.syslogs"
SILVER_DLQ = "rest.silver.dlq_quarantine"
SILVER_PENDING = "rest.silver.pending_enrichment"

KAFKA_GROUP_INTERFACE = "main-streaming-interface-v1"
KAFKA_GROUP_SYSLOGS = "main-streaming-syslogs-v1"
KAFKA_GROUP_INVENTORY = "main-streaming-inventory-v1"

# ---------------------------------------------------------------------------
# In-memory inventory cache — updated by q_inventory foreachBatch.
# Equivalent to Flink's broadcast state: one stream updates shared keyed state,
# the other streams look it up with zero latency.
# bronze.inventory remains a replay/backfill sink, not a hot-path lookup.
# ---------------------------------------------------------------------------
_inventory_lock = threading.Lock()
_inventory_cache: dict[str, dict[str, str | None]] = {}  # device_id → {site_id, vendor, role}

# ---------------------------------------------------------------------------
# Device baseline params cache — loaded from ClickHouse device_baseline_params.
# Refreshed every BASELINE_TTL_SECONDS (30 min) so weekly model retrain results
# are picked up without restarting the streaming job.
# Keyed by (device_id, interface_name, hour_of_day, day_of_week).
# ---------------------------------------------------------------------------
_baseline_lock = threading.Lock()
_baseline_params: list[dict] = []
_baseline_loaded_at: float = 0.0          # time.monotonic() timestamp of last load
BASELINE_TTL_SECONDS: int = 1800          # refresh every 30 minutes


def _load_baseline_params(ch: ClickHouseWriter) -> None:
    """Fetch device_baseline_params from ClickHouse and replace the driver-side cache."""
    global _baseline_loaded_at
    rows = ch.fetch_baseline_params()
    with _baseline_lock:
        _baseline_params.clear()
        _baseline_params.extend(rows)
        _baseline_loaded_at = time.monotonic()
    log.info("streaming.baseline: loaded %s device-baseline rows", len(rows))


def _ensure_baseline_fresh(ch: ClickHouseWriter) -> None:
    """Reload baseline params if the TTL has expired."""
    with _baseline_lock:
        age = time.monotonic() - _baseline_loaded_at
    if age >= BASELINE_TTL_SECONDS:
        log.info("streaming.baseline: TTL expired (age=%.0fs), refreshing from ClickHouse", age)
        _load_baseline_params(ch)


def _get_baseline_df(spark) -> DataFrame:
    """Snapshot the baseline cache as a Spark DataFrame for use in foreachBatch joins."""
    with _baseline_lock:
        snapshot = list(_baseline_params)
    if not snapshot:
        return spark.createDataFrame(
            [],
            "device_id string, interface_name string, hour_of_day int, day_of_week int,"
            " baseline_mean double, baseline_std double, iqr_k double,"
            " isolation_score_threshold double",
        )
    return spark.createDataFrame(snapshot)


def _apply_ingest_scores(enriched_df: DataFrame, spark) -> DataFrame:
    """Annotate enriched records with ingest-time anomaly scores.

    Three methods applied as native Spark column expressions (no UDF overhead):

    THRESHOLD_SATURATED
        effective_util_in > 80 OR effective_util_out > 80

    HIGH_Z_SCORE  (requires baseline params)
        z = |effective_util_in - baseline_mean| / (baseline_std + ε)
        flag when z > 3  (3-sigma rule)

    IQR_OUTLIER  (requires baseline params)
        iqr_score = |effective_util_in - baseline_mean| / (iqr_k × baseline_std + ε)
        flag when iqr_score > 1  (outside iqr_k-scaled fence)

    Records with no matching baseline params get ingest_z_score=0, ingest_iqr_score=0
    and only THRESHOLD_SATURATED can fire.  Scores will populate fully once the weekly
    IsolationForest train job writes device_baseline_params to ClickHouse.

    Note on day_of_week alignment: Spark dayofweek() returns 1=Sun..7=Sat; the PSI
    detector writes 0=Mon..6=Sun (pandas convention).  We use (dayofweek-1) % 7 as a
    best-effort mapping for the PoC.
    """
    baseline_df = _get_baseline_df(spark).select(
        F.col("device_id").alias("_bl_device_id"),
        F.col("interface_name").alias("_bl_iname"),
        F.col("hour_of_day").alias("_bl_hour"),
        F.col("day_of_week").alias("_bl_dow"),
        "baseline_mean",
        "baseline_std",
        "iqr_k",
    )

    with_keys = (
        enriched_df
        .withColumn("_hour_of_day", F.hour("device_ts").cast("int"))
        .withColumn("_day_of_week", ((F.dayofweek("device_ts") - 1) % 7).cast("int"))
    )

    joined = with_keys.join(
        baseline_df,
        on=(
            (F.col("device_id")    == F.col("_bl_device_id")) &
            (F.col("interface_name") == F.col("_bl_iname")) &
            (F.col("_hour_of_day")   == F.col("_bl_hour")) &
            (F.col("_day_of_week")   == F.col("_bl_dow"))
        ),
        how="left",
    ).drop("_bl_device_id", "_bl_iname", "_bl_hour", "_bl_dow",
           "_hour_of_day", "_day_of_week")

    eps = F.lit(1e-8)
    z_score   = F.abs(F.col("effective_util_in") - F.col("baseline_mean")) / (F.col("baseline_std") + eps)
    iqr_score = F.abs(F.col("effective_util_in") - F.col("baseline_mean")) / (F.col("iqr_k") * F.col("baseline_std") + eps)

    return (
        joined
        .withColumn("ingest_z_score",   F.coalesce(z_score,   F.lit(0.0)))
        .withColumn("ingest_iqr_score",  F.coalesce(iqr_score, F.lit(0.0)))
        .withColumn("_thr_flag",  (F.col("effective_util_in") > 80) | (F.col("effective_util_out") > 80))
        .withColumn("_z_flag",    F.col("ingest_z_score")   > F.lit(3.0))
        .withColumn("_iqr_flag",  F.col("ingest_iqr_score") > F.lit(1.0))
        .withColumn("ingest_anomaly", F.col("_thr_flag") | F.col("_z_flag") | F.col("_iqr_flag"))
        .withColumn(
            "ingest_flags",
            F.filter(
                F.array(
                    F.when(F.col("_thr_flag"),  F.lit("THRESHOLD_SATURATED")),
                    F.when(F.col("_z_flag"),    F.lit("HIGH_Z_SCORE")),
                    F.when(F.col("_iqr_flag"),  F.lit("IQR_OUTLIER")),
                ),
                lambda x: x.isNotNull(),
            ),
        )
        .drop("baseline_mean", "baseline_std", "iqr_k", "_thr_flag", "_z_flag", "_iqr_flag")
    )


def _debug(message: str) -> None:
    print(f"[streaming] {message}", flush=True)


def _count_rows(df: DataFrame) -> int:
    return int(df.count())


def _kafka_raw_stream(spark, topic: str, group_id: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("kafka.group.id", group_id)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) as payload", "timestamp as kafka_ts", "topic as source_topic")
    )


def _parse_interface_stats(df: DataFrame) -> DataFrame:
    return (
        df.select(
            "payload",
            "source_topic",
            F.from_json(
                F.col("payload"),
                "ts string, device_id string, interface_name string, util_in double, util_out double, admin_status int, oper_status int",
            ).alias("r")
        )
        .select("payload", "source_topic", "r.*")
        .withColumn("device_ts", F.to_timestamp("ts"))
        .withColumn("effective_util_in", effective_util_expr("util_in"))
        .withColumn("effective_util_out", effective_util_expr("util_out"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_partition_date", F.to_date(F.current_timestamp()))
    )


def _parse_syslogs(df: DataFrame) -> DataFrame:
    return (
        df.select(
            "payload",
            "source_topic",
            F.from_json(F.col("payload"), "ts string, device_id string, severity int, message string").alias("r")
        )
        .select("payload", "source_topic", "r.*")
        .withColumn("device_ts", F.to_timestamp("ts"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("is_critical", F.col("severity") < 3)
        .withColumn("_partition_date", F.to_date(F.current_timestamp()))
    )


def _parse_inventory(df: DataFrame) -> DataFrame:
    return (
        df.select(
            "payload",
            "source_topic",
            F.from_json(F.col("payload"), "device_id string, site_id string, vendor string, role string").alias(
                "r"
            )
        )
        .select("payload", "source_topic", "r.*")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_partition_date", F.to_date(F.current_timestamp()))
    )


def _update_inventory_cache(batch_df: DataFrame) -> None:
    """Merge new inventory records into the driver-side cache (latest write wins per device_id)."""
    rows = batch_df.select("device_id", "site_id", "vendor", "role").dropna(subset=["device_id"]).collect()
    with _inventory_lock:
        for row in rows:
            _inventory_cache[row.device_id] = {"site_id": row.site_id, "vendor": row.vendor, "role": row.role}
    log.info("streaming.inventory: cache updated total_devices=%s batch_rows=%s", len(_inventory_cache), len(rows))


def _get_inventory_df(spark) -> DataFrame:
    """Snapshot the in-memory cache as a DataFrame for use in foreachBatch joins."""
    with _inventory_lock:
        snapshot = [{"device_id": k, **v} for k, v in _inventory_cache.items()]
    if not snapshot:
        return spark.createDataFrame([], "device_id string, site_id string, vendor string, role string")
    return spark.createDataFrame(snapshot)


def _split_interface_records(batch_df: DataFrame, inventory_latest: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    staged = batch_df.join(inventory_latest, on="device_id", how="left")
    with_reason = staged.withColumn("validation_reason", interface_stats_validation_reason())
    invalid = with_reason.filter(F.col("validation_reason").isNotNull())
    valid = with_reason.filter(F.col("validation_reason").isNull())

    pending = valid.filter(F.col("site_id").isNull())
    enriched = valid.filter(F.col("site_id").isNotNull())
    return invalid, pending, enriched


def _write_dlq(invalid_df: DataFrame, writer: IcebergWriter, invalid_count: int | None = None) -> None:
    if invalid_count is None:
        invalid_count = _count_rows(invalid_df)
    if invalid_count == 0:
        return
    log.info("streaming.interface: writing %s invalid rows to DLQ", invalid_count)
    dlq = invalid_df.select(
        F.col("payload").alias("raw_payload"),
        F.col("validation_reason").alias("quarantine_reason"),
        F.col("source_topic"),
        F.current_timestamp().alias("quarantined_at"),
        F.lit(False).alias("reprocessed"),
        F.lit(None).cast("timestamp").alias("reprocessed_at"),
        F.lit(None).cast("string").alias("quarantine_resolution"),
        F.to_date(F.current_timestamp()).alias("_partition_date"),
    )
    writer.write_dataframe(dlq, SILVER_DLQ)


def _write_pending(pending_df: DataFrame, writer: IcebergWriter, pending_count: int | None = None) -> None:
    if pending_count is None:
        pending_count = _count_rows(pending_df)
    if pending_count == 0:
        return
    log.info("streaming.interface: writing %s rows to pending enrichment", pending_count)
    parked = pending_df.select(
        F.col("payload").alias("raw_payload"),
        F.col("source_topic"),
        F.current_timestamp().alias("parked_at"),
        F.col("device_id"),
        F.lit(False).alias("enriched"),
        F.lit(None).cast("timestamp").alias("enriched_at"),
        F.lit(0).cast("int").alias("retry_count"),
        F.to_date(F.current_timestamp()).alias("_partition_date"),
    )
    writer.write_dataframe(parked, SILVER_PENDING)


def _write_enriched_interface(
    enriched_df: DataFrame,
    writer: IcebergWriter,
    enriched_count: int | None = None,
) -> None:
    if enriched_count is None:
        enriched_count = _count_rows(enriched_df)
    if enriched_count == 0:
        return
    log.info("streaming.interface: writing %s enriched rows to silver interface table", enriched_count)
    silver = enriched_df.select(
        F.col("device_ts"),
        F.col("_ingested_at").alias("ingested_ts"),
        F.col("device_id"),
        F.col("site_id"),
        F.col("vendor"),
        F.col("role"),
        F.col("interface_name"),
        F.col("effective_util_in"),
        F.col("effective_util_out"),
        F.col("util_in").alias("raw_util_in"),
        F.col("util_out").alias("raw_util_out"),
        F.col("admin_status"),
        F.col("oper_status"),
        F.map_from_arrays(F.array().cast("array<string>"), F.array().cast("array<string>")).alias(
            "extra_cols"
        ),
        F.col("ingest_flags"),
        F.col("ingest_z_score"),
        F.col("ingest_iqr_score"),
        F.col("ingest_anomaly"),
        F.col("_partition_date"),
    )
    writer.write_dataframe(silver, SILVER_INTERFACE_STATS)



def _process_interface_batch(batch_df: DataFrame, _: int, spark, writer: IcebergWriter, ch: ClickHouseWriter) -> None:
    if batch_df.isEmpty():
        log.info("streaming.interface: empty micro-batch")
        _debug("interface micro-batch empty")
        return

    _ensure_baseline_fresh(ch)

    input_count = _count_rows(batch_df)
    log.info("streaming.interface: micro-batch received rows=%s", input_count)
    _debug(f"interface micro-batch rows={input_count}")
    inventory_latest = _get_inventory_df(spark)
    invalid, pending, enriched = _split_interface_records(batch_df, inventory_latest)
    enriched = _apply_ingest_scores(enriched, spark)
    invalid = invalid.persist()
    pending = pending.persist()
    enriched = enriched.persist()

    invalid_count = _count_rows(invalid)
    pending_count = _count_rows(pending)
    enriched_count = _count_rows(enriched)
    log.info(
        "streaming.interface: split invalid=%s pending=%s enriched=%s",
        invalid_count,
        pending_count,
        enriched_count,
    )
    _write_dlq(invalid, writer, invalid_count)
    _write_pending(pending, writer, pending_count)
    _write_enriched_interface(enriched, writer, enriched_count)
    # Flatline detection and health-score aggregation are NOT computed here.
    # A micro-batch covers only ~30 s of data — window aggregations over 1 h / 4 h
    # need historical context that doesn't exist in a single batch.
    # Both are handled by scheduled batch jobs reading from silver.interface_stats.

    invalid.unpersist()
    pending.unpersist()
    enriched.unpersist()


def _process_syslog_batch(batch_df: DataFrame, _: int, spark, writer: IcebergWriter) -> None:
    if batch_df.isEmpty():
        log.info("streaming.syslogs: empty micro-batch")
        _debug("syslog micro-batch empty")
        return
    log.info("streaming.syslogs: micro-batch received rows=%s", _count_rows(batch_df))
    _debug("syslog micro-batch received")
    inventory_latest = _get_inventory_df(spark)
    staged = batch_df.join(inventory_latest, on="device_id", how="left")
    with_reason = staged.withColumn("validation_reason", syslogs_validation_reason())
    invalid = with_reason.filter(F.col("validation_reason").isNotNull())
    invalid_count = _count_rows(invalid)
    _write_dlq(invalid, writer, invalid_count)

    clean = with_reason.filter(F.col("validation_reason").isNull() & F.col("site_id").isNotNull())
    clean_count = _count_rows(clean)
    if clean_count == 0:
        log.info("streaming.syslogs: no clean rows to write")
        return
    log.info("streaming.syslogs: writing clean rows=%s", clean_count)
    silver = clean.select(
        F.col("device_ts"),
        F.col("_ingested_at").alias("ingested_ts"),
        F.col("device_id"),
        F.col("site_id"),
        F.col("vendor"),
        F.col("role"),
        F.col("severity"),
        F.col("is_critical"),
        F.col("message"),
        F.col("_partition_date"),
    )
    writer.write_dataframe(silver, SILVER_SYSLOGS)


def _replay_pending_from_inventory(inventory_batch: DataFrame, _: int, spark, writer: IcebergWriter) -> None:
    if inventory_batch.isEmpty():
        log.info("streaming.inventory: empty micro-batch")
        return

    # 1. Update the in-memory cache so future stats/syslog batches enrich immediately.
    _update_inventory_cache(inventory_batch)

    # 2. Identify which device_ids just became known — only scan pending rows for those.
    new_device_ids = [
        row.device_id
        for row in inventory_batch.select("device_id").dropna(subset=["device_id"]).distinct().collect()
    ]
    if not new_device_ids:
        return

    # 3. Read only the relevant pending rows (targeted predicate, not full table scan).
    try:
        pending = (
            spark.read.format("iceberg").load(SILVER_PENDING)
            .filter((F.col("enriched") == F.lit(False)) & F.col("device_id").isin(new_device_ids))
        )
    except Exception:
        log.warning("streaming.inventory: silver.pending_enrichment not available, skipping replay")
        return

    if pending.isEmpty():
        log.info("streaming.inventory: no pending rows found for new device_ids=%s", new_device_ids)
        return

    # 4. Enrich: join with the inventory snapshot now in the cache.
    inventory_df = _get_inventory_df(spark).filter(F.col("device_id").isin(new_device_ids))
    ready = pending.join(inventory_df, on="device_id", how="inner")

    parsed = (
        ready.select(
            F.col("raw_payload"),
            F.from_json(
                F.col("raw_payload"),
                "ts string, device_id string, interface_name string, util_in double, util_out double, admin_status int, oper_status int",
            ).alias("r"),
            "site_id", "vendor", "role",
        )
        .select("raw_payload", "site_id", "vendor", "role", "r.*")
    )

    enriched = (
        parsed
        .withColumn("device_ts", F.to_timestamp("ts"))
        .withColumn("effective_util_in", effective_util_expr("util_in"))
        .withColumn("effective_util_out", effective_util_expr("util_out"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_partition_date", F.to_date(F.current_timestamp()))
    )

    # Replayed records were already validated at park time; re-score against current
    # baseline so late-enriched silver rows carry accurate ingest_* annotations.
    enriched = _apply_ingest_scores(enriched, spark)

    replayed_count = enriched.count()
    log.info("streaming.inventory: replaying %s pending rows for device_ids=%s", replayed_count, new_device_ids)
    _write_enriched_interface(enriched, writer, replayed_count)

    # 5. Mark resolved rows as enriched so the batch safety-net job skips them.
    enriched.select("raw_payload", "device_id").createOrReplaceTempView("_resolved_pending")
    spark.sql(f"""
        MERGE INTO {SILVER_PENDING} AS t
        USING _resolved_pending AS s
            ON  t.raw_payload = s.raw_payload
            AND t.device_id   = s.device_id
            AND t.enriched    = false
        WHEN MATCHED THEN DELETE 
    """)


def run() -> None:
    if not log.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )

    spark = SparkSessionFactory.create(mode="streaming", app_name="main-streaming")
    logging.getLogger().setLevel(logging.INFO)
    _debug("spark session created")
    writer = IcebergWriter(spark)
    ch = ClickHouseWriter(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)
    _load_baseline_params(ch)   # initial load; refreshed every BASELINE_TTL_SECONDS thereafter

    stats_raw = _kafka_raw_stream(spark, KAFKA_TOPIC_INTERFACE_STATS, KAFKA_GROUP_INTERFACE)
    sys_raw = _kafka_raw_stream(spark, KAFKA_TOPIC_SYSLOGS, KAFKA_GROUP_SYSLOGS)
    inventory_raw = _kafka_raw_stream(spark, KAFKA_TOPIC_INVENTORY, KAFKA_GROUP_INVENTORY)
    _debug("kafka source dataframes created")

    stats = _parse_interface_stats(stats_raw)
    syslogs = _parse_syslogs(sys_raw)
    inventory = _parse_inventory(inventory_raw)
    _debug("parsed streaming dataframes created")

    _debug("starting interface stream query")
    q_stats = (
        stats.writeStream.outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: _process_interface_batch(batch_df, batch_id, spark, writer, ch))
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/streaming/interface_stats")
        .start()
    )
    _debug("interface stream started")

    _debug("starting syslogs stream query")
    q_syslogs = (
        syslogs.writeStream.outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: _process_syslog_batch(batch_df, batch_id, spark, writer))
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/streaming/syslogs")
        .start()
    )
    _debug("syslogs stream started")

    _debug("starting inventory stream query")
    q_inventory = (
        inventory.writeStream.outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: _replay_pending_from_inventory(batch_df, batch_id, spark, writer))
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/streaming/inventory")
        .start()
    )
    _debug("inventory stream started")

    log.info("Main streaming pipeline started (topics: interface_stats, syslogs, inventory)")
    _debug("main streaming pipeline started")
    q_stats.awaitTermination()
    q_syslogs.awaitTermination()
    q_inventory.awaitTermination()


if __name__ == "__main__":
    run()
