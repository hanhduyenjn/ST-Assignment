"""Additional pipeline tests for ingest-time detection, aggregation, and bronze evolution."""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import src.pipeline.bronze_ingest as bronze_ingest
import src.pipeline.gold_recompute as gold_recompute
import src.pipeline.streaming as streaming_mod
from src.pipeline.batch import run as run_backfill
from src.transforms.health_score import compute_health_score


_INGEST_BASELINE_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("interface_name", StringType(), True),
    StructField("hour_of_day", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("baseline_mean", DoubleType(), True),
    StructField("baseline_std", DoubleType(), True),
    StructField("iqr_k", DoubleType(), True),
    StructField("isolation_score_threshold", DoubleType(), True),
])


_STREAMING_INPUT_SCHEMA = StructType([
    StructField("payload", StringType(), True),
    StructField("source_topic", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("interface_name", StringType(), True),
    StructField("util_in", DoubleType(), True),
    StructField("util_out", DoubleType(), True),
    StructField("admin_status", IntegerType(), True),
    StructField("oper_status", IntegerType(), True),
    StructField("device_ts", TimestampType(), True),
    StructField("effective_util_in", DoubleType(), True),
    StructField("effective_util_out", DoubleType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_partition_date", DateType(), True),
])


_BRONZE_RAW_SCHEMA = StructType([
    StructField("_raw_payload", StringType(), True),
    StructField("_kafka_ts", TimestampType(), True),
])


@pytest.fixture(autouse=True)
def reset_baseline_cache():
    """Keep ingest scoring tests independent."""
    streaming_mod._baseline_params.clear()
    streaming_mod._baseline_loaded_at = 0.0
    yield
    streaming_mod._baseline_params.clear()
    streaming_mod._baseline_loaded_at = 0.0


def _streaming_row(device_id: str, util_in: float, util_out: float, hour: int = 0) -> dict:
    device_ts = datetime(2024, 1, 1, hour, 5, tzinfo=timezone.utc)
    return {
        "payload": f'{{"device_id":"{device_id}"}}',
        "source_topic": "raw.interface.stats",
        "ts": "2024-01-01T00:05:00Z",
        "device_id": device_id,
        "interface_name": "Gi0/0",
        "util_in": util_in,
        "util_out": util_out,
        "admin_status": 1,
        "oper_status": 1,
        "device_ts": device_ts,
        "effective_util_in": util_in,
        "effective_util_out": util_out,
        "_ingested_at": device_ts,
        "_partition_date": date(2024, 1, 1),
    }


def test_apply_ingest_scores_handles_threshold_zscore_and_iqr(spark):
    """Main streaming ingest-time scoring should flag threshold, z-score, and IQR cases."""
    streaming_mod._baseline_params.extend([
        {
            "device_id": "d-1",
            "interface_name": "Gi0/0",
            "hour_of_day": 0,
            "day_of_week": 1,
            "baseline_mean": 50.0,
            "baseline_std": 10.0,
            "iqr_k": 1.5,
            "isolation_score_threshold": 0.0,
        }
    ])

    df = spark.createDataFrame(
        [
            _streaming_row("d-1", 85.0, 90.0),
            _streaming_row("d-2", 45.0, 40.0),
        ],
        schema=_STREAMING_INPUT_SCHEMA,
    )

    scored = streaming_mod._apply_ingest_scores(df, spark)
    rows = {row.device_id: row for row in scored.select(
        "device_id",
        "ingest_z_score",
        "ingest_iqr_score",
        "ingest_anomaly",
        "ingest_flags",
    ).collect()}

    flagged = rows["d-1"]
    assert flagged.ingest_anomaly is True
    assert flagged.ingest_z_score == pytest.approx(3.5, rel=1e-3)
    assert flagged.ingest_iqr_score > 1.0
    assert set(flagged.ingest_flags) == {
        "THRESHOLD_SATURATED",
        "HIGH_Z_SCORE",
        "IQR_OUTLIER",
    }

    clean = rows["d-2"]
    assert clean.ingest_anomaly is False
    assert clean.ingest_z_score == 0.0
    assert clean.ingest_iqr_score == 0.0
    assert clean.ingest_flags == []


def test_compute_health_score_clamps_and_weights():
    """ClickHouse-facing health score formula should apply weights and clamp to [0, 100]."""
    assert compute_health_score(0.0, 0, 0.0) == 100.0
    assert compute_health_score(0.5, 2, 0.25) == pytest.approx(70.0)
    assert compute_health_score(3.0, 10, 2.0) == 0.0


def test_gold_recompute_aggregates_hourly_site_metrics(spark):
    """Gold recompute should roll up site/hour metrics before ClickHouse insertion."""
    interface_df = spark.createDataFrame(
        [
            {
                "site_id": "SITE-A",
                "device_ts": datetime(2024, 1, 1, 0, 10, tzinfo=timezone.utc),
                "effective_util_in": 90.0,
                "effective_util_out": 30.0,
                "oper_status": 1,
            },
            {
                "site_id": "SITE-A",
                "device_ts": datetime(2024, 1, 1, 0, 20, tzinfo=timezone.utc),
                "effective_util_in": 70.0,
                "effective_util_out": 20.0,
                "oper_status": 2,
            },
            {
                "site_id": "SITE-B",
                "device_ts": datetime(2024, 1, 1, 1, 5, tzinfo=timezone.utc),
                "effective_util_in": 55.0,
                "effective_util_out": 40.0,
                "oper_status": 1,
            },
        ]
    )
    syslogs_df = spark.createDataFrame(
        [
            {
                "site_id": "SITE-A",
                "device_ts": datetime(2024, 1, 1, 0, 15, tzinfo=timezone.utc),
                "severity": 2,
            },
            {
                "site_id": "SITE-B",
                "device_ts": datetime(2024, 1, 1, 1, 15, tzinfo=timezone.utc),
                "severity": 5,
            },
            {
                "site_id": "SITE-B",
                "device_ts": datetime(2024, 1, 1, 1, 20, tzinfo=timezone.utc),
                "severity": 0,
            },
        ]
    )

    writer = MagicMock()
    writer.insert_dicts = MagicMock()

    spark_mock = MagicMock()
    spark_mock.table = MagicMock(side_effect=[interface_df, syslogs_df])

    with patch.object(gold_recompute.SparkSessionFactory, "create", return_value=spark_mock), \
         patch.object(gold_recompute, "ClickHouseWriter", return_value=writer):
        gold_recompute.run()

    assert writer.insert_dicts.called
    table_name, rows = writer.insert_dicts.call_args.args
    assert table_name == "gold_site_health_hourly"
    assert len(rows) == 2

    by_site = {row["site_id"]: row for row in rows}
    assert by_site["SITE-A"]["avg_util_in"] == pytest.approx(80.0)
    assert by_site["SITE-A"]["pct_interfaces_saturated"] == pytest.approx(0.5)
    assert by_site["SITE-A"]["critical_syslog_count"] == 1
    assert by_site["SITE-A"]["total_interface_count"] == 2
    assert by_site["SITE-A"]["pct_interfaces_down"] == pytest.approx(0.5)

    assert by_site["SITE-B"]["avg_util_in"] == pytest.approx(55.0)
    assert by_site["SITE-B"]["pct_interfaces_saturated"] == pytest.approx(0.0)
    assert by_site["SITE-B"]["critical_syslog_count"] == 1
    assert by_site["SITE-B"]["total_interface_count"] == 1
    assert by_site["SITE-B"]["pct_interfaces_down"] == pytest.approx(0.0)


def test_bronze_interface_stream_preserves_extra_fields(spark):
    """Bronze ingest should keep unexpected JSON fields in _extra_cols."""
    raw = spark.createDataFrame(
        [
            {
                "_raw_payload": (
                    '{"ts":"2024-01-01T00:00:00Z","device_id":"d-1","interface_name":"Gi0/0",'
                    '"util_in":12.5,"util_out":14.0,"admin_status":1,"oper_status":1,'
                    '"packet_loss":0.42,"note":"new-field"}'
                ),
                "_kafka_ts": datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            }
        ],
        schema=_BRONZE_RAW_SCHEMA,
    )

    with patch.object(bronze_ingest, "_kafka_stream", return_value=raw):
        df = bronze_ingest._build_interface_stats_stream(spark)

    row = df.select("device_id", "_source_topic", "_extra_cols").collect()[0]
    assert row.device_id == "d-1"
    assert row._source_topic == "raw.interface.stats"
    assert row._extra_cols["packet_loss"] == "0.42"
    assert row._extra_cols["note"] == "new-field"


def test_bronze_append_evolves_top_level_columns(spark):
    """Bronze ingest should ALTER the target table for new top-level columns before append."""
    batch_df = spark.createDataFrame(
        [
            {
                "_raw_payload": "{}",
                "ts": "2024-01-01T00:00:00Z",
                "device_id": "d-1",
                "interface_name": "Gi0/0",
                "util_in": 12.5,
                "util_out": 14.0,
                "admin_status": 1,
                "oper_status": 1,
                "_extra_cols": {},
                "_ingested_at": datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                "_source_topic": "raw.interface.stats",
                "_partition_date": date(2024, 1, 1),
                "packet_loss": 0.42,
            }
        ]
    )

    old_schema_df = spark.createDataFrame([], schema="_raw_payload string, ts string, device_id string, interface_name string, util_in double, util_out double, admin_status int, oper_status int, _extra_cols map<string,string>, _ingested_at timestamp, _source_topic string, _partition_date date")
    new_schema_df = spark.createDataFrame([], schema="_raw_payload string, ts string, device_id string, interface_name string, util_in double, util_out double, admin_status int, oper_status int, _extra_cols map<string,string>, _ingested_at timestamp, _source_topic string, _partition_date date, packet_loss double")

    writer = MagicMock()
    writer.write_dataframe = MagicMock()
    writer.spark = spark

    with patch.object(spark, "table", side_effect=[old_schema_df, new_schema_df]), \
         patch.object(spark, "sql") as sql_mock:
        bronze_ingest._append_batch(batch_df, 0, writer, bronze_ingest.BRONZE_INTERFACE_STATS, "interface_stats")

    assert sql_mock.called
    assert any("packet_loss" in str(call.args[0]) for call in sql_mock.call_args_list)

    written_df = writer.write_dataframe.call_args.args[0]
    assert "packet_loss" in written_df.columns
    assert written_df.select("packet_loss").collect()[0][0] == pytest.approx(0.42)
