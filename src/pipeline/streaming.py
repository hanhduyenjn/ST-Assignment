from __future__ import annotations

import logging
import os
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
from src.monitoring.exporter import run_metrics_exporter
from src.monitoring.metrics import (
    ANOMALIES_TOTAL,
    DLQ_RECORDS_TOTAL,
    INGEST_RECORDS_TOTAL,
    LATEST_HEALTH_SCORE,
    STREAM_BATCH_DURATION_SECONDS,
)
from src.pipeline.silver_transforms import build_silver_dlq, build_silver_interface, build_silver_pending, build_silver_syslogs
from src.storage.clickhouse_writer import ClickHouseWriter
from src.storage.iceberg_writer import IcebergWriter
from src.transforms.effective_util import effective_util_expr
from src.transforms.flatline_v2 import detect_flatline
from src.transforms.health_score import compute_health_score
from src.validators.spark_expressions import interface_stats_validation_reason, syslogs_validation_reason

SILVER_INTERFACE_STATS = "rest.silver.interface_stats"
SILVER_SYSLOGS = "rest.silver.syslogs"
SILVER_DLQ = "rest.silver.dlq_quarantine"
SILVER_PENDING = "rest.silver.pending_enrichment"

FLATLINE_VARIANCE_THRESHOLD = float(os.environ.get("FLATLINE_VARIANCE_THRESHOLD", "0.01"))
FLATLINE_MIN_POINTS = int(os.environ.get("FLATLINE_MIN_POINTS", "30"))

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

# Serialize Iceberg appends across concurrent foreachBatch callbacks to avoid
# commit-contention spikes and JVM instability under heavy micro-batches.
_iceberg_write_lock = threading.Lock()

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
        flag when z > 2

    IQR_OUTLIER  (requires baseline params)
        iqr_score = |effective_util_in - baseline_mean| / (iqr_k × baseline_std + ε)
        flag when iqr_score > 1  (outside iqr_k-scaled fence)

    Records with no matching baseline params get ingest_z_score=0, ingest_iqr_score=0
    and only THRESHOLD_SATURATED can fire.  Scores will populate fully once the weekly
    IsolationForest train job writes device_baseline_params to ClickHouse.

    Note on day_of_week alignment: Spark dayofweek() returns 1=Sun..7=Sat; baseline
    params use 0=Mon..6=Sun.  We map via (dayofweek+5) % 7.
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
        .withColumn("_day_of_week", ((F.dayofweek("device_ts") + 5) % 7).cast("int"))
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
        .withColumn("_z_flag",    F.col("ingest_z_score")   > F.lit(2.0))
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


def _kafka_raw_stream(spark, topic: str, group_id: str) -> DataFrame:
    max_offsets = int(os.environ.get("STREAM_MAX_OFFSETS_PER_TRIGGER", "1200"))
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("kafka.group.id", group_id)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", str(max_offsets))
        .load()
        .selectExpr("CAST(value AS STRING) as payload", "timestamp as kafka_ts", "topic as source_topic")
    )


def _parse_interface_stats(df: DataFrame) -> DataFrame:
    # Parse all fields as strings first so that legacy messages where the producer
    # serialised numeric values as JSON strings (e.g. "58.3" instead of 58.3) are
    # handled correctly.  Explicit casts below are equivalent to the original schema
    # but tolerate both representations.
    return (
        df.select(
            "payload",
            "source_topic",
            F.from_json(
                F.col("payload"),
                "ts string, device_id string, interface_name string, util_in string, util_out string, admin_status string, oper_status string",
            ).alias("r")
        )
        .select("payload", "source_topic", "r.*")
        .withColumn("util_in", F.col("util_in").cast("double"))
        .withColumn("util_out", F.col("util_out").cast("double"))
        .withColumn("admin_status", F.col("admin_status").cast("int"))
        .withColumn("oper_status", F.col("oper_status").cast("int"))
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


def _write_dlq(invalid_df: DataFrame, writer: IcebergWriter) -> None:
    row_count = invalid_df.count()
    log.info("streaming.interface: writing invalid rows to DLQ count=%s", row_count)
    if row_count == 0:
        return
    dlq = build_silver_dlq(invalid_df, raw_col="payload")
    try:
        with _iceberg_write_lock:
            writer.write_dataframe(dlq, SILVER_DLQ)
    except Exception as e:
        log.error("streaming.dlq: write failed (continuing): %s", str(e)[:150])


def _write_pending(pending_df: DataFrame, writer: IcebergWriter) -> None:
    row_count = pending_df.count()
    log.info("streaming.interface: writing rows to pending enrichment count=%s", row_count)
    if row_count == 0:
        return
    parked = build_silver_pending(pending_df, raw_col="payload")
    try:
        with _iceberg_write_lock:
            writer.write_dataframe(parked, SILVER_PENDING)
    except Exception as e:
        log.error("streaming.pending: write failed (continuing): %s", str(e)[:150])


def _write_enriched_interface(
    enriched_df: DataFrame,
    writer: IcebergWriter,
) -> None:
    row_count = enriched_df.count()
    log.info("streaming.interface: writing enriched rows to silver interface table count=%s", row_count)
    if row_count == 0:
        return
    # Use the shared silver schema projection from silver_transforms to ensure consistency
    silver = build_silver_interface(enriched_df)
    try:
        with _iceberg_write_lock:
            writer.write_dataframe(silver, SILVER_INTERFACE_STATS)
    except Exception as e:
        log.error("streaming.interface: write failed (continuing): %s", str(e)[:150])



def _get_flatline_keys(enriched_df: DataFrame) -> set[tuple[str, str]]:
    """Return (device_id, interface_name) pairs whose util_in is flat in this micro-batch.

    Collects enriched rows to the driver, groups by device+interface, and applies
    Welford online variance.  Only oper_status=1 rows count — a down interface is
    not a flatline.  Requires FLATLINE_MIN_POINTS samples before firing to avoid
    false positives on tiny micro-batches.
    """
    rows = (
        enriched_df
        .filter(F.col("oper_status") == 1)
        .select("device_id", "interface_name", "effective_util_in")
        .collect()
    )
    if not rows:
        return set()

    grouped: dict[tuple[str, str], list[float]] = {}
    for row in rows:
        key = (row.device_id, row.interface_name)
        grouped.setdefault(key, []).append(float(row.effective_util_in))

    flatline_keys: set[tuple[str, str]] = set()
    for key, values in grouped.items():
        is_flat, _var, _mean = detect_flatline(
            values,
            min_points=FLATLINE_MIN_POINTS,
            eps=FLATLINE_VARIANCE_THRESHOLD,
        )
        if is_flat:
            flatline_keys.add(key)

    return flatline_keys


def _apply_flatline_flags(enriched_df: DataFrame, flatline_keys: set[tuple[str, str]]) -> DataFrame:
    """Merge FLATLINE into ingest_flags / ingest_anomaly for matching device+interface rows.

    Mirrors the HIGH_Z_SCORE / IQR_OUTLIER / THRESHOLD_SATURATED pattern so all
    anomaly types live on the same per-record columns and flow through mv_anomaly_summary
    without any separate staging table.
    """
    if not flatline_keys:
        return enriched_df

    # Build a broadcast-friendly list of (device_id, interface_name) structs.
    flatline_list = [{"device_id": d, "interface_name": i} for d, i in flatline_keys]
    spark = enriched_df.sparkSession
    flatline_df = spark.createDataFrame(flatline_list).select(
        F.col("device_id").alias("_fl_did"),
        F.col("interface_name").alias("_fl_iname"),
        F.lit(True).alias("_is_flatline"),
    )

    joined = enriched_df.join(
        F.broadcast(flatline_df),
        on=(
            (F.col("device_id")      == F.col("_fl_did")) &
            (F.col("interface_name") == F.col("_fl_iname"))
        ),
        how="left",
    ).drop("_fl_did", "_fl_iname")

    is_flat = F.col("_is_flatline") == True  # noqa: E712
    return (
        joined
        .withColumn(
            "ingest_flags",
            F.when(
                is_flat,
                F.array_union(F.coalesce(F.col("ingest_flags"), F.array()), F.array(F.lit("FLATLINE"))),
            ).otherwise(F.col("ingest_flags")),
        )
        .withColumn(
            "ingest_anomaly",
            F.col("ingest_anomaly") | is_flat,
        )
        .drop("_is_flatline")
    )


def _process_interface_batch(batch_df: DataFrame, _: int, spark, writer: IcebergWriter, ch: ClickHouseWriter) -> None:
    if batch_df.isEmpty():
        log.info("streaming.interface: empty micro-batch")
        _debug("interface micro-batch empty")
        return

    _batch_start = time.perf_counter()
    batch_count = batch_df.count()
    log.info("streaming.interface: micro-batch received count=%s", batch_count)
    _debug("interface micro-batch received")

    _ensure_baseline_fresh(ch)

    inventory_latest = _get_inventory_df(spark)
    invalid, pending, enriched = _split_interface_records(batch_df, inventory_latest)

    invalid_count = invalid.count()
    pending_count = pending.count()
    enriched_count = enriched.count()
    log.info("streaming.interface: split invalid=%s pending=%s enriched=%s", invalid_count, pending_count, enriched_count)

    INGEST_RECORDS_TOTAL.labels(topic=KAFKA_TOPIC_INTERFACE_STATS, status="valid").inc(enriched_count + pending_count)
    INGEST_RECORDS_TOTAL.labels(topic=KAFKA_TOPIC_INTERFACE_STATS, status="invalid").inc(invalid_count)
    if invalid_count > 0:
        DLQ_RECORDS_TOTAL.labels(topic=KAFKA_TOPIC_INTERFACE_STATS, reason="validation").inc(invalid_count)

    enriched = _apply_ingest_scores(enriched, spark)
    flatline_keys = _get_flatline_keys(enriched)
    if flatline_keys:
        log.info("streaming.flatline: detected %d flatline device+interface pairs", len(flatline_keys))
        enriched = _apply_flatline_flags(enriched, flatline_keys)

    if enriched_count > 0:
        anomaly_rows = (
            enriched
            .filter(F.col("ingest_anomaly"))
            .select(F.explode(F.col("ingest_flags")).alias("anomaly_type"))
            .groupBy("anomaly_type")
            .count()
            .collect()
        )
        for row in anomaly_rows:
            ANOMALIES_TOTAL.labels(anomaly_type=row.anomaly_type).inc(row["count"])

        site_rows = (
            enriched
            .filter(F.col("site_id").isNotNull())
            .groupBy("site_id")
            .agg(
                F.avg(F.when(F.col("effective_util_in") > 80, 1.0).otherwise(0.0)).alias("pct_saturated"),
                F.avg(F.when(F.col("oper_status") != 1, 1.0).otherwise(0.0)).alias("pct_down"),
            )
            .collect()
        )
        for row in site_rows:
            if row.site_id:
                score = compute_health_score(row.pct_saturated, 0, row.pct_down)
                LATEST_HEALTH_SCORE.labels(site_id=row.site_id).set(score)

    _write_dlq(invalid, writer)
    _write_pending(pending, writer)
    _write_enriched_interface(enriched, writer)
    STREAM_BATCH_DURATION_SECONDS.labels(stage="interface_stats").observe(time.perf_counter() - _batch_start)

def _process_syslog_batch(batch_df: DataFrame, _: int, spark, writer: IcebergWriter) -> None:
    if batch_df.isEmpty():
        log.info("streaming.syslogs: empty micro-batch")
        _debug("syslog micro-batch empty")
        return
    _batch_start = time.perf_counter()
    batch_count = batch_df.count()
    log.info("streaming.syslogs: micro-batch received count=%s", batch_count)
    _debug("syslog micro-batch received")
    inventory_latest = _get_inventory_df(spark)
    staged = batch_df.join(inventory_latest, on="device_id", how="left")
    with_reason = staged.withColumn("validation_reason", syslogs_validation_reason())
    invalid = with_reason.filter(F.col("validation_reason").isNotNull())
    invalid_count = invalid.count()
    if invalid_count > 0:
        INGEST_RECORDS_TOTAL.labels(topic=KAFKA_TOPIC_SYSLOGS, status="invalid").inc(invalid_count)
        DLQ_RECORDS_TOTAL.labels(topic=KAFKA_TOPIC_SYSLOGS, reason="validation").inc(invalid_count)
    _write_dlq(invalid, writer)

    clean = with_reason.filter(F.col("validation_reason").isNull() & F.col("site_id").isNotNull())
    clean_count = clean.count()
    INGEST_RECORDS_TOTAL.labels(topic=KAFKA_TOPIC_SYSLOGS, status="valid").inc(clean_count)
    log.info("streaming.syslogs: writing clean rows count=%s", clean_count)
    if clean_count > 0:
        silver = build_silver_syslogs(clean)
        try:
            with _iceberg_write_lock:
                writer.write_dataframe(silver, SILVER_SYSLOGS)
        except Exception as e:
            log.error("streaming.syslogs: write failed (continuing): %s", str(e)[:150])
    STREAM_BATCH_DURATION_SECONDS.labels(stage="syslogs").observe(time.perf_counter() - _batch_start)


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
                "ts string, device_id string, interface_name string, util_in string, util_out string, admin_status string, oper_status string",
            ).alias("r"),
            "site_id", "vendor", "role",
        )
        .select("raw_payload", "site_id", "vendor", "role", "r.*")
        .withColumn("util_in", F.col("util_in").cast("double"))
        .withColumn("util_out", F.col("util_out").cast("double"))
        .withColumn("admin_status", F.col("admin_status").cast("int"))
        .withColumn("oper_status", F.col("oper_status").cast("int"))
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
    flatline_keys = _get_flatline_keys(enriched)
    if flatline_keys:
        log.info("streaming.flatline: detected %d flatline pairs (replay path)", len(flatline_keys))
        enriched = _apply_flatline_flags(enriched, flatline_keys)

    replay_count = enriched.count()
    log.info("streaming.inventory: replaying pending rows for device_ids=%s count=%s", new_device_ids, replay_count)
    _write_enriched_interface(enriched, writer)

    # 5. Mark resolved rows as enriched so the batch safety-net job skips them.
    # Use snapshot isolation so concurrent appends to pending_enrichment by the
    # interface stream don't conflict with this MERGE DELETE.
    enriched.select("raw_payload", "device_id").createOrReplaceTempView("_resolved_pending")
    try:
        spark.conf.set("spark.sql.iceberg.merge.isolation-level", "snapshot")
        spark.sql(f"""
            MERGE INTO {SILVER_PENDING} AS t
            USING _resolved_pending AS s
                ON  t.raw_payload = s.raw_payload
                AND t.device_id   = s.device_id
                AND t.enriched    = false
            WHEN MATCHED THEN DELETE
        """)
    except Exception as e:
        log.warning(
            "streaming.inventory: MERGE on pending_enrichment failed (concurrent write), "
            "rows will be cleaned by batch safety-net: %s", str(e)[:150]
        )


def run() -> None:
    if not log.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )

    _metrics_thread = threading.Thread(target=run_metrics_exporter, daemon=True, name="metrics-exporter")
    _metrics_thread.start()
    log.info("streaming: metrics exporter started on port 9108")

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
