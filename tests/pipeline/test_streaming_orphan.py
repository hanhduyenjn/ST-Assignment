"""Tests for orphaned-record handling in the streaming pipeline.

Verifies the full lifecycle:
  1. Stats record with unknown device_id → parked in silver.pending_enrichment
  2. Inventory arrives → in-memory cache updated + pending record replayed → silver.interface_stats
  3. Replayed record marked enriched=true via MERGE INTO (so batch safety-net skips it)

No Kafka or Iceberg infra required — local SparkSession + mocked IcebergWriter/SQL.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import src.pipeline.streaming as streaming_mod
from src.pipeline.streaming import (
    SILVER_DLQ,
    SILVER_INTERFACE_STATS,
    SILVER_PENDING,
    _process_interface_batch,
    _replay_pending_from_inventory,
)

# ---------------------------------------------------------------------------
# Schemas matching the outputs of _parse_interface_stats / _parse_inventory
# and the silver.pending_enrichment table layout.
# ---------------------------------------------------------------------------

_STATS_SCHEMA = StructType([
    StructField("payload",            StringType(),    True),
    StructField("source_topic",       StringType(),    True),
    StructField("ts",                 StringType(),    True),
    StructField("device_id",          StringType(),    True),
    StructField("interface_name",     StringType(),    True),
    StructField("util_in",            DoubleType(),    True),
    StructField("util_out",           DoubleType(),    True),
    StructField("admin_status",       IntegerType(),   True),
    StructField("oper_status",        IntegerType(),   True),
    StructField("device_ts",          TimestampType(), True),
    StructField("effective_util_in",  DoubleType(),    True),
    StructField("effective_util_out", DoubleType(),    True),
    StructField("_ingested_at",       TimestampType(), True),
    StructField("_partition_date",    DateType(),      True),
])

_INVENTORY_SCHEMA = StructType([
    StructField("payload",         StringType(),    True),
    StructField("source_topic",    StringType(),    True),
    StructField("device_id",       StringType(),    True),
    StructField("site_id",         StringType(),    True),
    StructField("vendor",          StringType(),    True),
    StructField("role",            StringType(),    True),
    StructField("_ingested_at",    TimestampType(), True),
    StructField("_partition_date", DateType(),      True),
])

_PENDING_SCHEMA = StructType([
    StructField("raw_payload",     StringType(),    True),
    StructField("source_topic",    StringType(),    True),
    StructField("parked_at",       TimestampType(), True),
    StructField("device_id",       StringType(),    True),
    StructField("enriched",        BooleanType(),   True),
    StructField("enriched_at",     TimestampType(), True),
    StructField("retry_count",     IntegerType(),   True),
    StructField("_partition_date", DateType(),      True),
])

# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------

_TS = "2024-01-01T00:00:00"
_NOW = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
_TODAY = date(2024, 1, 1)


def _stats_row(device_id: str, oper_status: int = 1, util_in: float = 50.0) -> dict:
    payload = json.dumps({
        "ts": _TS, "device_id": device_id, "interface_name": "Gi0/0",
        "util_in": util_in, "util_out": 40.0, "admin_status": 1, "oper_status": oper_status,
    })
    return {
        "payload": payload, "source_topic": "interface_stats", "ts": _TS,
        "device_id": device_id, "interface_name": "Gi0/0",
        "util_in": util_in, "util_out": 40.0, "admin_status": 1, "oper_status": oper_status,
        "device_ts": _NOW,
        "effective_util_in": 0.0 if oper_status in (2, 3) else util_in,
        "effective_util_out": 0.0 if oper_status in (2, 3) else 40.0,
        "_ingested_at": _NOW, "_partition_date": _TODAY,
    }


def _inventory_row(device_id: str, site_id: str = "SITE-A") -> dict:
    return {
        "payload": json.dumps({"device_id": device_id, "site_id": site_id, "vendor": "Cisco", "role": "core"}),
        "source_topic": "inventory",
        "device_id": device_id, "site_id": site_id, "vendor": "Cisco", "role": "core",
        "_ingested_at": _NOW, "_partition_date": _TODAY,
    }


def _pending_row(device_id: str) -> dict:
    payload = json.dumps({
        "ts": _TS, "device_id": device_id, "interface_name": "Gi0/0",
        "util_in": 50.0, "util_out": 40.0, "admin_status": 1, "oper_status": 1,
    })
    return {
        "raw_payload": payload, "source_topic": "interface_stats",
        "parked_at": _NOW, "device_id": device_id,
        "enriched": False, "enriched_at": None, "retry_count": 0, "_partition_date": _TODAY,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_cache():
    """Isolate each test — clear the module-level inventory cache."""
    streaming_mod._inventory_cache.clear()
    yield
    streaming_mod._inventory_cache.clear()


@pytest.fixture
def writer():
    w = MagicMock()
    w.write_dataframe = MagicMock()
    return w


@pytest.fixture
def ch():
    """Mock ClickHouseWriter — baseline cache stays empty so scores default to 0."""
    c = MagicMock()
    c.fetch_baseline_params = MagicMock(return_value=[])
    return c



# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _written_targets(writer_mock) -> list[str]:
    return [c.args[1] for c in writer_mock.write_dataframe.call_args_list]


def _mock_pending_read(spark, pending_df):
    """Context manager: patches spark.read (a property) so .format("iceberg").load(...).filter(...) returns pending_df."""
    mock_reader = MagicMock()
    mock_reader.format.return_value.load.return_value.filter.return_value = pending_df
    return patch.object(type(spark), "read", new_callable=PropertyMock, return_value=mock_reader)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestOrphanLifecycle:  # noqa: D101

    def test_unknown_device_goes_to_pending(self, spark, writer, ch):
        """Stats record with no inventory → parked in pending_enrichment, not silver."""
        batch = spark.createDataFrame([_stats_row("d-unknown")], schema=_STATS_SCHEMA)

        _process_interface_batch(batch, 0, spark, writer, ch)

        targets = _written_targets(writer)
        assert SILVER_PENDING in targets, "orphaned record must be parked"
        assert SILVER_INTERFACE_STATS not in targets, "orphaned record must not reach silver yet"

    def test_known_device_enriches_directly(self, spark, writer, ch):
        """Stats record with device_id already in cache → enriched immediately, nothing parked."""
        streaming_mod._inventory_cache["d-known"] = {"site_id": "SITE-A", "vendor": "Cisco", "role": "core"}
        batch = spark.createDataFrame([_stats_row("d-known")], schema=_STATS_SCHEMA)

        _process_interface_batch(batch, 0, spark, writer, ch)

        targets = _written_targets(writer)
        assert SILVER_INTERFACE_STATS in targets, "known device must reach silver"
        assert SILVER_PENDING not in targets, "known device must not be parked"

    def test_invalid_row_goes_to_dlq_not_pending(self, spark, writer, ch):
        """Stats row that fails validation (null device_id) → DLQ only."""
        row = _stats_row("d-valid")
        row["device_id"] = None
        batch = spark.createDataFrame([row], schema=_STATS_SCHEMA)

        _process_interface_batch(batch, 0, spark, writer, ch)

        targets = _written_targets(writer)
        assert SILVER_DLQ in targets
        assert SILVER_PENDING not in targets
        assert SILVER_INTERFACE_STATS not in targets

    def test_inventory_arrival_populates_cache(self, spark, writer):
        """Inventory Kafka batch → device_id lands in _inventory_cache."""
        assert "d-new" not in streaming_mod._inventory_cache

        inv_batch = spark.createDataFrame([_inventory_row("d-new", "SITE-B")], schema=_INVENTORY_SCHEMA)
        empty_pending = spark.createDataFrame([], schema=_PENDING_SCHEMA)

        with _mock_pending_read(spark, empty_pending), \
             patch.object(spark, "sql", return_value=MagicMock()):
            _replay_pending_from_inventory(inv_batch, 0, spark, writer)

        assert streaming_mod._inventory_cache.get("d-new") == {
            "site_id": "SITE-B", "vendor": "Cisco", "role": "core",
        }

    def test_replay_drains_pending_on_inventory_arrival(self, spark, writer):
        """Core orphan-resolution: parked record is enriched and MERGE INTO fires when inventory arrives."""
        device_id = "d-orphan"
        pending = spark.createDataFrame([_pending_row(device_id)], schema=_PENDING_SCHEMA)
        inv_batch = spark.createDataFrame([_inventory_row(device_id, "SITE-C")], schema=_INVENTORY_SCHEMA)

        sql_calls: list[str] = []

        with _mock_pending_read(spark, pending), \
             patch.object(spark, "sql", side_effect=lambda q: sql_calls.append(q) or MagicMock()):
            _replay_pending_from_inventory(inv_batch, 0, spark, writer)

        # Enriched record must land in silver
        targets = _written_targets(writer)
        assert SILVER_INTERFACE_STATS in targets, "replayed record must reach silver.interface_stats"

        # MERGE INTO must mark the record as enriched
        merge_calls = [q for q in sql_calls if "MERGE INTO" in q]
        assert merge_calls, "MERGE INTO must be issued to mark pending record as enriched"
        assert any("enriched" in q for q in merge_calls)

    def test_replay_carries_correct_site_id(self, spark, writer):
        """Enriched record written to silver must carry the site_id from the inventory batch."""
        device_id = "d-site-check"
        pending = spark.createDataFrame([_pending_row(device_id)], schema=_PENDING_SCHEMA)
        inv_batch = spark.createDataFrame([_inventory_row(device_id, "SITE-XYZ")], schema=_INVENTORY_SCHEMA)

        with _mock_pending_read(spark, pending), \
             patch.object(spark, "sql", return_value=MagicMock()):
            _replay_pending_from_inventory(inv_batch, 0, spark, writer)

        written_df = writer.write_dataframe.call_args_list[0].args[0]
        site_ids = [r.site_id for r in written_df.select("site_id").collect()]
        assert site_ids == ["SITE-XYZ"]

    def test_replay_targets_only_arriving_device_ids(self, spark, writer):
        """Only device_ids present in the inventory batch are replayed; others stay parked."""
        # pending_df for d-orphan-1 only (d-orphan-2 would be in a different Iceberg predicate scan)
        pending_d1 = spark.createDataFrame([_pending_row("d-orphan-1")], schema=_PENDING_SCHEMA)
        inv_batch = spark.createDataFrame(
            [_inventory_row("d-orphan-1")],  # only d-orphan-1 arrives
            schema=_INVENTORY_SCHEMA,
        )

        with _mock_pending_read(spark, pending_d1), \
             patch.object(spark, "sql", return_value=MagicMock()):
            _replay_pending_from_inventory(inv_batch, 0, spark, writer)

        written_df = writer.write_dataframe.call_args_list[0].args[0]
        device_ids = {r.device_id for r in written_df.select("device_id").collect()}
        assert "d-orphan-1" in device_ids
        assert "d-orphan-2" not in device_ids

    def test_empty_inventory_batch_is_noop(self, spark, writer):
        """Empty inventory micro-batch must not touch cache or writer."""
        inv_batch = spark.createDataFrame([], schema=_INVENTORY_SCHEMA)

        _replay_pending_from_inventory(inv_batch, 0, spark, writer)

        assert streaming_mod._inventory_cache == {}
        writer.write_dataframe.assert_not_called()

    def test_no_pending_rows_skips_write(self, spark, writer):
        """Inventory arrives but nothing is parked for that device → writer not called."""
        inv_batch = spark.createDataFrame([_inventory_row("d-fresh")], schema=_INVENTORY_SCHEMA)
        empty_pending = spark.createDataFrame([], schema=_PENDING_SCHEMA)

        with _mock_pending_read(spark, empty_pending), \
             patch.object(spark, "sql", return_value=MagicMock()):
            _replay_pending_from_inventory(inv_batch, 0, spark, writer)

        writer.write_dataframe.assert_not_called()

    def test_down_interface_uses_zero_effective_util(self, spark, writer, ch):
        """Stats row with oper_status=2 (DOWN) → effective_util_in/out are 0.0 in silver."""
        streaming_mod._inventory_cache["d-down"] = {"site_id": "SITE-A", "vendor": "Cisco", "role": "core"}
        batch = spark.createDataFrame([_stats_row("d-down", oper_status=2, util_in=75.0)], schema=_STATS_SCHEMA)

        _process_interface_batch(batch, 0, spark, writer, ch)

        written_df = writer.write_dataframe.call_args_list[0].args[0]
        row = written_df.select("effective_util_in", "effective_util_out").collect()[0]
        assert row.effective_util_in == 0.0
        assert row.effective_util_out == 0.0
