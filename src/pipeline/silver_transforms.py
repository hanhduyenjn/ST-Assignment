"""Shared Spark DataFrame transforms for the silver layer.

Used by both the streaming pipeline (streaming.py) and all batch jobs
(batch.py, dlq_reprocessor.py, pending_enrichment.py) so the logic stays
in one place and backfill results are byte-for-byte identical to streaming.

Design rules:
- All functions are pure DataFrame → DataFrame: no I/O, no Spark session creation.
- Silver schema projections (build_silver_*) are the single source of truth for
  each table's column list and types.
- Scoring accepts a pre-built baseline_df so callers control how baselines are
  loaded (in-memory cache for streaming; direct ClickHouse fetch for batch).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transforms.effective_util import effective_util_expr
from src.validators.spark_expressions import interface_stats_validation_reason, syslogs_validation_reason

# Empty baseline schema used when no baseline params are available.
_BASELINE_SCHEMA = (
    "device_id string, interface_name string, hour_of_day int, day_of_week int,"
    " baseline_mean double, baseline_std double, iqr_k double, isolation_score_threshold double"
)


# ---------------------------------------------------------------------------
# Field derivation — parse raw columns into typed/derived ones
# ---------------------------------------------------------------------------

def add_interface_derived_cols(df: DataFrame) -> DataFrame:
    """Derive device_ts, effective_util_in/out from raw interface_stats columns.

    Expects: ts (string), util_in (double), util_out (double), oper_status (int).
    Sets _partition_date from device_ts.  Preserves an existing _ingested_at column
    (batch reads it from bronze); fills current_timestamp() when absent.
    """
    has_ingested = "_ingested_at" in df.columns
    df = (
        df
        .withColumn("device_ts",         F.to_timestamp("ts"))
        .withColumn("effective_util_in",  effective_util_expr("util_in"))
        .withColumn("effective_util_out", effective_util_expr("util_out"))
        .withColumn("_partition_date",    F.to_date("device_ts"))
    )
    if not has_ingested:
        df = df.withColumn("_ingested_at", F.current_timestamp())
    return df


def add_syslog_derived_cols(df: DataFrame) -> DataFrame:
    """Derive device_ts, is_critical from raw syslogs columns.

    Expects: ts (string), severity (int).
    """
    has_ingested = "_ingested_at" in df.columns
    df = (
        df
        .withColumn("device_ts",      F.to_timestamp("ts"))
        .withColumn("is_critical",    F.col("severity") < 3)
        .withColumn("_partition_date", F.to_date("device_ts"))
    )
    if not has_ingested:
        df = df.withColumn("_ingested_at", F.current_timestamp())
    return df


# ---------------------------------------------------------------------------
# Routing / splitting
# ---------------------------------------------------------------------------

def split_interface_records(
    stats_df: DataFrame,
    inventory_df: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Join stats with inventory, validate, and split into three routing buckets.

    Returns (invalid, pending, enriched):
      invalid  — rows that fail validation  → silver.dlq_quarantine
      pending  — valid rows with unknown device_id → silver.pending_enrichment
      enriched — valid rows with known device_id   → silver.interface_stats
    """
    staged = stats_df.join(inventory_df, on="device_id", how="left")
    with_reason = staged.withColumn("validation_reason", interface_stats_validation_reason())
    invalid  = with_reason.filter(F.col("validation_reason").isNotNull())
    valid    = with_reason.filter(F.col("validation_reason").isNull())
    pending  = valid.filter(F.col("site_id").isNull())
    enriched = valid.filter(F.col("site_id").isNotNull())
    return invalid, pending, enriched


def split_syslog_records(
    syslogs_df: DataFrame,
    inventory_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Join syslogs with inventory, validate, and split into (invalid, clean).

    invalid — rows that fail validation → silver.dlq_quarantine
    clean   — valid rows with known site → silver.syslogs
    """
    staged = syslogs_df.join(inventory_df, on="device_id", how="left")
    with_reason = staged.withColumn("validation_reason", syslogs_validation_reason())
    invalid = with_reason.filter(F.col("validation_reason").isNotNull())
    clean   = with_reason.filter(F.col("validation_reason").isNull() & F.col("site_id").isNotNull())
    return invalid, clean


# ---------------------------------------------------------------------------
# Ingest-time anomaly scoring
# ---------------------------------------------------------------------------

def apply_ingest_scores(enriched_df: DataFrame, baseline_df: DataFrame) -> DataFrame:
    """Annotate enriched interface_stats rows with z-score, IQR, and threshold flags.

    baseline_df columns: device_id, interface_name, hour_of_day (int),
                         day_of_week (int), baseline_mean, baseline_std, iqr_k.

    Records with no matching baseline row get score=0.0; only THRESHOLD_SATURATED
    can still fire.  Baseline params are joined by (device_id, interface_name,
    hour_of_day, day_of_week) using a left join so unmatched rows are preserved.

    day_of_week mapping: Spark dayofweek() returns 1=Sun..7=Sat; baseline params
    use 0=Mon..6=Sun (pandas/ISO convention).  We map via (dayofweek+5) % 7.
    """
    bl = baseline_df.select(
        F.col("device_id").alias("_bl_did"),
        F.col("interface_name").alias("_bl_iname"),
        F.col("hour_of_day").alias("_bl_hour"),
        F.col("day_of_week").alias("_bl_dow"),
        "baseline_mean",
        "baseline_std",
        "iqr_k",
    )

    with_keys = (
        enriched_df
        .withColumn("_hour", F.hour("device_ts").cast("int"))
        .withColumn("_dow",  ((F.dayofweek("device_ts") + 5) % 7).cast("int"))
    )

    joined = with_keys.join(
        bl,
        on=(
            (F.col("device_id")      == F.col("_bl_did"))   &
            (F.col("interface_name") == F.col("_bl_iname")) &
            (F.col("_hour")          == F.col("_bl_hour"))  &
            (F.col("_dow")           == F.col("_bl_dow"))
        ),
        how="left",
    ).drop("_bl_did", "_bl_iname", "_bl_hour", "_bl_dow", "_hour", "_dow")

    eps       = F.lit(1e-8)
    z_score   = F.abs(F.col("effective_util_in") - F.col("baseline_mean")) / (F.col("baseline_std") + eps)
    iqr_score = F.abs(F.col("effective_util_in") - F.col("baseline_mean")) / (F.col("iqr_k") * F.col("baseline_std") + eps)

    return (
        joined
        .withColumn("ingest_z_score",  F.coalesce(z_score,   F.lit(0.0)))
        .withColumn("ingest_iqr_score", F.coalesce(iqr_score, F.lit(0.0)))
        .withColumn("_thr",  (F.col("effective_util_in") > 80) | (F.col("effective_util_out") > 80))
        .withColumn("_z",    F.col("ingest_z_score")   > F.lit(2.0))
        .withColumn("_iqr",  F.col("ingest_iqr_score") > F.lit(1.0))
        .withColumn("ingest_anomaly", F.col("_thr") | F.col("_z") | F.col("_iqr"))
        .withColumn(
            "ingest_flags",
            F.filter(
                F.array(
                    F.when(F.col("_thr"), F.lit("THRESHOLD_SATURATED")),
                    F.when(F.col("_z"),   F.lit("HIGH_Z_SCORE")),
                    F.when(F.col("_iqr"), F.lit("IQR_OUTLIER")),
                ),
                lambda x: x.isNotNull(),
            ),
        )
        .drop("baseline_mean", "baseline_std", "iqr_k", "_thr", "_z", "_iqr")
    )


def empty_baseline_df(spark) -> DataFrame:
    """Return an empty baseline DataFrame with the correct schema.

    Used when no baseline params are available (e.g. before the first model train).
    Scoring will still fire THRESHOLD_SATURATED; z-score / IQR scores stay 0.
    """
    return spark.createDataFrame([], _BASELINE_SCHEMA)


# ---------------------------------------------------------------------------
# Silver projections — single source of truth for each table's column layout
# ---------------------------------------------------------------------------

def build_silver_interface(enriched_df: DataFrame) -> DataFrame:
    """Project an enriched+scored DataFrame to the silver.interface_stats schema."""
    return enriched_df.select(
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
        F.map_from_arrays(
            F.array().cast("array<string>"), F.array().cast("array<string>")
        ).alias("extra_cols"),
        F.col("ingest_flags"),
        F.col("ingest_z_score"),
        F.col("ingest_iqr_score"),
        F.col("ingest_anomaly"),
        F.col("_partition_date"),
    )


def build_silver_dlq(df: DataFrame, raw_col: str = "payload") -> DataFrame:
    """Project an invalid row to the silver.dlq_quarantine schema.

    raw_col: column holding the original raw JSON string.
      "payload"     → streaming path (Kafka value column)
      "raw_payload" → DLQ reprocessor (already stored in the DLQ table)
    """
    return df.select(
        F.col(raw_col).alias("raw_payload"),
        F.col("validation_reason").alias("quarantine_reason"),
        F.col("source_topic"),
        F.current_timestamp().alias("quarantined_at"),
        F.lit(False).alias("reprocessed"),
        F.lit(None).cast("timestamp").alias("reprocessed_at"),
        F.lit(None).cast("string").alias("quarantine_resolution"),
        F.to_date(F.current_timestamp()).alias("_partition_date"),
    )


def build_silver_pending(pending_df: DataFrame, raw_col: str = "payload") -> DataFrame:
    """Project a pending row to the silver.pending_enrichment schema.

    raw_col: column holding the raw JSON payload to store for later replay.
    """
    return pending_df.select(
        F.col(raw_col).alias("raw_payload"),
        F.col("source_topic"),
        F.current_timestamp().alias("parked_at"),
        F.col("device_id"),
        F.lit(False).alias("enriched"),
        F.lit(None).cast("timestamp").alias("enriched_at"),
        F.lit(0).cast("int").alias("retry_count"),
        F.to_date(F.current_timestamp()).alias("_partition_date"),
    )


def build_silver_syslogs(clean_df: DataFrame) -> DataFrame:
    """Project a clean syslogs row to the silver.syslogs schema."""
    return clean_df.select(
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
