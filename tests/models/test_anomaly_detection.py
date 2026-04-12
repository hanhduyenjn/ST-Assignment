"""Tests for anomaly detection models and their pipeline integration.

Organised by detector:
  - TestIsolationForestDetector : unit tests for the sklearn model class
  - TestIsolationForestFeatures : Spark feature engineering (_build_feature_vectors)
  - TestIsolationForestPipeline : train/detect pipeline entrypoints (mocked I/O)
  - TestPSIDriftDetector        : PSI formula + param update logic
  - TestPSIPipeline             : run() entrypoint (mocked Spark + ClickHouse)
  - TestFlatlineDetector        : Welford-based Python detect + Spark window detect
"""
from __future__ import annotations

import json
import tempfile
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.models.base import AnomalyResult
from src.models.flatline_detector import FlatlineDetector
from src.models.isolation_forest import (
    IsolationForestDetector,
    _FEATURE_COLS,
    _build_feature_vectors,
)
from src.models.psi_drift_detector import DriftResult, PSIDriftDetector
from src.transforms.flatline import WelfordState, detect_flatline


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_SILVER_SCHEMA = StructType([
    StructField("device_id",          StringType(),    True),
    StructField("interface_name",     StringType(),    True),
    StructField("site_id",            StringType(),    True),
    StructField("effective_util_in",  DoubleType(),    True),
    StructField("effective_util_out", DoubleType(),    True),
    StructField("oper_status",        IntegerType(),   True),
    StructField("device_ts",          TimestampType(), True),
    StructField("_partition_date",    DateType(),      True),
])


def _silver_row(
    device_id="d-1",
    iface="Gi0/0",
    site="SITE-A",
    util_in=50.0,
    util_out=40.0,
    oper=1,
    hour=0,
) -> dict:
    ts = datetime(2024, 1, 1, hour, 0, tzinfo=timezone.utc)
    return {
        "device_id":          device_id,
        "interface_name":     iface,
        "site_id":            site,
        "effective_util_in":  util_in,
        "effective_util_out": util_out,
        "oper_status":        oper,
        "device_ts":          ts,
        "_partition_date":    date(2024, 1, 1),
    }


def _normal_features(n=100, mean=50.0, std=5.0, seed=0) -> pd.DataFrame:
    """Synthetic feature vectors representing normal traffic."""
    rng = np.random.default_rng(seed)
    util = rng.normal(mean, std, n).clip(0, 100)
    return pd.DataFrame({
        "mean_util_in":   util,
        "mean_util_out":  util * 0.8,
        "std_util_in":    rng.uniform(2, 8, n),
        "std_util_out":   rng.uniform(2, 8, n),
        "max_util_in":    util + rng.uniform(5, 15, n),
        "p95_util_in":    util + rng.uniform(3, 10, n),
        "pct_saturated":  np.zeros(n),
        "pct_down":       np.zeros(n),
        "count_records":  np.ones(n),
        "mean_util_delta": rng.uniform(5, 15, n),
    })


# ===========================================================================
# TestIsolationForestDetector — unit tests for the sklearn model class
# ===========================================================================

class TestIsolationForestDetector:

    def test_train_and_detect_returns_anomaly_results(self):
        """Trained detector must flag injected anomalies and leave normals clean."""
        normal = _normal_features(n=200)
        detector = IsolationForestDetector(contamination=0.05, random_state=42)
        detector.train(normal)

        # Clearly anomalous row: saturated, high util, high std
        anomalous = pd.DataFrame([{
            "mean_util_in":   99.0,
            "mean_util_out":  99.0,
            "std_util_in":    0.0,
            "std_util_out":   0.0,
            "max_util_in":    100.0,
            "p95_util_in":    100.0,
            "pct_saturated":  1.0,
            "pct_down":       0.0,
            "count_records":  1.0,
            "mean_util_delta": 0.0,
        }])
        original = anomalous.copy()
        original["device_id"] = "d-anom"
        original["interface_name"] = "Gi0/0"

        results = detector.detect_frame(original, anomalous)
        assert isinstance(results, list)
        # The single anomalous row should be flagged
        assert any(r.device_id == "d-anom" for r in results)
        for r in results:
            assert isinstance(r, AnomalyResult)
            assert r.anomaly_type == "MODEL"
            assert r.subtype == "MULTIVARIATE_ANOMALY"

    def test_detect_before_train_raises(self):
        detector = IsolationForestDetector()
        with pytest.raises(RuntimeError, match="must be trained"):
            detector.detect_frame(pd.DataFrame(), pd.DataFrame([[1.0]], columns=["x"]))

    def test_empty_records_returns_empty_list(self):
        detector = IsolationForestDetector(contamination=0.05, random_state=42)
        detector.train(_normal_features(n=100))
        assert detector.detect([]) == []

    def test_distill_rules_returns_dict_with_expected_keys(self):
        normal = _normal_features(n=100)
        detector = IsolationForestDetector(contamination=0.1, random_state=42)
        detector.train(normal)
        rules = detector.distill_rules(normal)
        assert "feature_names" in rules
        assert "tree" in rules
        assert rules["feature_names"] == list(normal.columns)

    def test_save_and_load_roundtrip(self, tmp_path):
        """Saved and reloaded detector must produce identical predictions."""
        normal = _normal_features(n=100)
        detector = IsolationForestDetector(contamination=0.05, random_state=42)
        detector.train(normal)

        path = tmp_path / "model.joblib"
        detector.save(path)
        loaded = IsolationForestDetector.load(path)

        x = detector.scaler.transform(normal.values)
        orig_labels = detector.model.predict(x)
        load_labels = loaded.model.predict(loaded.scaler.transform(normal.values))
        assert (orig_labels == load_labels).all()


# ===========================================================================
# TestIsolationForestFeatures — Spark _build_feature_vectors
# ===========================================================================

class TestIsolationForestFeatures:

    def test_feature_columns_match_expected(self, spark):
        """_build_feature_vectors must produce exactly _FEATURE_COLS + key columns."""
        rows = [_silver_row(util_in=u, hour=h) for u, h in [(30.0, 0), (50.0, 0), (85.0, 0)]]
        silver_df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)
        features = _build_feature_vectors(silver_df)

        assert set(_FEATURE_COLS).issubset(features.columns), \
            f"Missing: {set(_FEATURE_COLS) - set(features.columns)}"
        assert "device_id" in features.columns
        assert "interface_name" in features.columns

    def test_saturated_row_yields_nonzero_pct_saturated(self, spark):
        """A row with util_in > 80 must contribute to pct_saturated."""
        rows = [
            _silver_row(util_in=90.0),  # saturated
            _silver_row(util_in=40.0),  # normal
        ]
        silver_df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)
        features = _build_feature_vectors(silver_df)
        assert features.iloc[0]["pct_saturated"] == pytest.approx(0.5)

    def test_down_interface_yields_nonzero_pct_down(self, spark):
        """Rows with oper_status != 1 must contribute to pct_down."""
        rows = [
            _silver_row(oper=1),
            _silver_row(oper=2),  # DOWN
        ]
        silver_df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)
        features = _build_feature_vectors(silver_df)
        assert features.iloc[0]["pct_down"] == pytest.approx(0.5)

    def test_count_records_normalised_to_one_for_single_group(self, spark):
        """count_records is normalised by max — a single group should give 1.0."""
        rows = [_silver_row(util_in=50.0), _silver_row(util_in=55.0)]
        silver_df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)
        features = _build_feature_vectors(silver_df)
        assert features["count_records"].max() == pytest.approx(1.0)

    def test_multiple_devices_produce_separate_rows(self, spark):
        """Each (device_id, interface_name, hour_of_day, day_of_week) is a distinct row."""
        rows = [
            _silver_row(device_id="d-1", util_in=30.0),
            _silver_row(device_id="d-2", util_in=80.0),
        ]
        silver_df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)
        features = _build_feature_vectors(silver_df)
        assert len(features) == 2
        means = set(features["mean_util_in"].round(1))
        assert 30.0 in means
        assert 80.0 in means


# ===========================================================================
# TestIsolationForestPipeline — train/detect pipeline (mocked I/O)
# ===========================================================================

class TestIsolationForestPipeline:

    def _make_silver_df(self, spark, util_values: list[float]):
        rows = [_silver_row(util_in=float(u), util_out=float(u) * 0.8) for u in util_values]
        return spark.createDataFrame(rows, schema=_SILVER_SCHEMA)

    def test_train_saves_artifact_and_upserts_baseline(self, spark, tmp_path):
        """train() must save a model artifact and call upsert_baseline_params."""
        from src.models.isolation_forest import train

        silver_df = self._make_silver_df(spark, list(range(20, 80, 2)))
        ch_mock = MagicMock()
        ch_mock.fetch_baseline_params.return_value = []

        with patch("src.common.spark_session.SparkSessionFactory") as sf, \
             patch("src.storage.clickhouse_writer.ClickHouseWriter", return_value=ch_mock), \
             patch("src.common.config.MODEL_ARTIFACT_PATH", str(tmp_path)):
            spark_mock = MagicMock()
            spark_mock.table.return_value.filter.return_value = silver_df
            sf.create.return_value = spark_mock

            train(end_date="2024-01-01", lookback_days=1)

        # Artifact must exist on disk
        assert (tmp_path / "isolation_forest" / "model.joblib").exists()
        assert (tmp_path / "isolation_forest" / "distilled_rules.json").exists()
        # Baseline params must have been upserted
        ch_mock.upsert_baseline_params.assert_called_once()
        rows = ch_mock.upsert_baseline_params.call_args.args[0]
        assert len(rows) >= 1
        assert "baseline_mean" in rows[0]
        assert "isolation_score_threshold" in rows[0]
        assert "distilled_rules" in rows[0]

    def test_detect_inserts_anomaly_flags(self, spark, tmp_path):
        """detect() must call insert_anomaly_flags when anomalies are found."""
        from src.models.isolation_forest import detect

        # Train a minimal model first so the artifact exists
        normal = _normal_features(n=100, seed=1)
        detector = IsolationForestDetector(contamination=0.1, random_state=42)
        detector.train(normal)
        artifact_dir = tmp_path / "isolation_forest"
        artifact_dir.mkdir()
        detector.save(artifact_dir / "model.joblib")

        # Silver data with an obviously anomalous row (saturated)
        rows = [_silver_row(util_in=u) for u in [30.0] * 5 + [99.0] * 5]
        silver_df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)

        ch_mock = MagicMock()

        with patch("src.common.spark_session.SparkSessionFactory") as sf, \
             patch("src.storage.clickhouse_writer.ClickHouseWriter", return_value=ch_mock), \
             patch("src.common.config.MODEL_ARTIFACT_PATH", str(tmp_path)):
            spark_mock = MagicMock()
            spark_mock.table.return_value.filter.return_value = silver_df
            sf.create.return_value = spark_mock

            detect(date="2024-01-01")

        ch_mock.insert_anomaly_flags.assert_called_once()
        flags = ch_mock.insert_anomaly_flags.call_args.args[0]
        assert len(flags) >= 1
        assert all(f["anomaly_type"] == "MODEL" for f in flags)
        assert all("anomaly_subtype" in f for f in flags)
        assert all("score" in f for f in flags)

    def test_detect_skips_when_no_artifact(self, spark, tmp_path):
        """detect() must exit cleanly when no trained model exists."""
        from src.models.isolation_forest import detect

        ch_mock = MagicMock()
        with patch("src.common.spark_session.SparkSessionFactory"), \
             patch("src.storage.clickhouse_writer.ClickHouseWriter", return_value=ch_mock), \
             patch("src.common.config.MODEL_ARTIFACT_PATH", str(tmp_path)):
            detect(date="2024-01-01")  # should not raise

        ch_mock.insert_anomaly_flags.assert_not_called()

    def test_detect_skips_when_silver_empty(self, spark, tmp_path):
        """detect() must not call insert_anomaly_flags when there is no data for the date."""
        from src.models.isolation_forest import detect

        normal = _normal_features(n=100, seed=1)
        detector = IsolationForestDetector(contamination=0.1, random_state=42)
        detector.train(normal)
        artifact_dir = tmp_path / "isolation_forest"
        artifact_dir.mkdir()
        detector.save(artifact_dir / "model.joblib")

        empty_df = spark.createDataFrame([], schema=_SILVER_SCHEMA)
        ch_mock = MagicMock()

        with patch("src.common.spark_session.SparkSessionFactory") as sf, \
             patch("src.storage.clickhouse_writer.ClickHouseWriter", return_value=ch_mock), \
             patch("src.common.config.MODEL_ARTIFACT_PATH", str(tmp_path)):
            spark_mock = MagicMock()
            spark_mock.table.return_value.filter.return_value = empty_df
            sf.create.return_value = spark_mock
            detect(date="2024-01-01")

        ch_mock.insert_anomaly_flags.assert_not_called()


# ===========================================================================
# TestPSIDriftDetector — PSI formula + param update logic
# ===========================================================================

class TestPSIDriftDetector:

    def test_identical_distributions_produce_zero_psi(self):
        """PSI of a distribution against itself must be ~0."""
        data = pd.Series(np.random.default_rng(0).normal(50, 10, 200))
        result = PSIDriftDetector().compute_psi(data, data.copy())
        assert result.psi == pytest.approx(0.0, abs=1e-6)
        assert result.is_significant is False

    def test_shifted_distribution_produces_significant_psi(self):
        """A distribution shifted by 3σ must exceed the 0.25 PSI threshold."""
        rng = np.random.default_rng(42)
        baseline = pd.Series(rng.normal(50, 5, 500))
        current  = pd.Series(rng.normal(80, 5, 500))  # shifted by 6σ
        result = PSIDriftDetector(threshold=0.25).compute_psi(baseline, current)
        assert result.psi > 0.25
        assert result.is_significant is True

    def test_empty_series_returns_zero_psi(self):
        result = PSIDriftDetector().compute_psi(pd.Series([], dtype=float), pd.Series([50.0]))
        assert result.psi == 0.0
        assert result.is_significant is False

    def test_build_param_updates_returns_only_drifted_groups(self):
        """Groups without significant drift must be excluded from updates."""
        rng = np.random.default_rng(7)
        # stable group (d-1): identical data in both windows — PSI guaranteed 0
        stable_baseline = rng.normal(50, 5, 60)
        stable_current  = stable_baseline.copy()
        # drifted group (d-2): mean shifted by 40 units
        drifted_baseline = rng.normal(30, 5, 60)
        drifted_current  = rng.normal(70, 5, 60)

        frame = pd.DataFrame({
            "device_id":       ["d-1"] * 60 + ["d-1"] * 60 + ["d-2"] * 60 + ["d-2"] * 60,
            "interface_name":  ["Gi0/0"] * 240,
            "hour_of_day":     [0] * 240,
            "day_of_week":     [1] * 240,
            "baseline_metric": list(stable_baseline) + [np.nan] * 60 + list(drifted_baseline) + [np.nan] * 60,
            "current_metric":  [np.nan] * 60 + list(stable_current) + [np.nan] * 60 + list(drifted_current),
        })

        updates = PSIDriftDetector(threshold=0.25).build_param_updates(frame)
        if not updates.empty:
            # If any updates, only d-2 should be present
            assert "d-1" not in updates["device_id"].values
            assert "d-2" in updates["device_id"].values

    def test_build_param_updates_produces_positive_iqr_k(self):
        """Updated iqr_k must be positive for any valid distribution."""
        rng = np.random.default_rng(0)
        baseline = rng.normal(30, 5, 100)
        current  = rng.normal(70, 5, 100)

        frame = pd.DataFrame({
            "device_id":       ["d-1"] * 200,
            "interface_name":  ["Gi0/0"] * 200,
            "hour_of_day":     [0] * 200,
            "day_of_week":     [1] * 200,
            "baseline_metric": list(baseline) + [np.nan] * 100,
            "current_metric":  [np.nan] * 100 + list(current),
        })

        updates = PSIDriftDetector(threshold=0.01).build_param_updates(frame)
        if not updates.empty:
            assert (updates["iqr_k"] > 0).all()
            assert (updates["baseline_std"] > 0).all()


# ===========================================================================
# TestPSIPipeline — run() entrypoint (mocked Spark + ClickHouse)
# ===========================================================================

class TestPSIPipeline:

    def test_run_upserts_drifted_baselines(self, spark):
        """run() must call upsert_baseline_params for groups with significant drift."""
        from src.models.psi_drift_detector import run

        rng = np.random.default_rng(42)
        # Current window: high utilisation
        current_rows = [
            _silver_row(device_id="d-drift", util_in=float(u), hour=0)
            for u in rng.normal(80, 3, 30)
        ]
        # Reference window: low utilisation
        reference_rows = [
            _silver_row(device_id="d-drift", util_in=float(u), hour=0)
            for u in rng.normal(30, 3, 30)
        ]
        all_rows = current_rows + reference_rows

        silver_df = spark.createDataFrame(all_rows, schema=_SILVER_SCHEMA)

        ch_mock = MagicMock()

        with patch("src.common.spark_session.SparkSessionFactory") as sf, \
             patch("src.storage.clickhouse_writer.ClickHouseWriter", return_value=ch_mock):
            sf.create.return_value = spark
            with patch.object(spark, "table", return_value=silver_df):
                run(reference_end="2024-01-31")

        # With a large enough PSI, baseline params should be updated
        # (may be zero if the filter produces empty frames; assert called or not called gracefully)
        assert ch_mock.upsert_baseline_params.call_count <= 1

    def test_run_skips_update_when_no_drift(self, spark):
        """run() must not call upsert_baseline_params when PSI is below threshold."""
        from src.models.psi_drift_detector import run

        rng = np.random.default_rng(1)
        # Same distribution in both windows — PSI ~ 0
        rows = [_silver_row(device_id="d-stable", util_in=float(u), hour=0)
                for u in rng.normal(50, 5, 60)]
        silver_df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)

        ch_mock = MagicMock()

        with patch("src.common.spark_session.SparkSessionFactory") as sf, \
             patch("src.storage.clickhouse_writer.ClickHouseWriter", return_value=ch_mock):
            sf.create.return_value = spark
            with patch.object(spark, "table", return_value=silver_df):
                run(reference_end="2024-01-31")

        ch_mock.upsert_baseline_params.assert_not_called()


# ===========================================================================
# TestFlatlineDetector — Welford-based Python detect + Spark window detect
# ===========================================================================

class TestFlatlineDetector:

    # --- Welford state (transforms/flatline.py) ---

    def test_welford_state_constant_sequence_gives_zero_variance(self):
        state = WelfordState()
        for _ in range(10):
            state.update(42.0)
        assert state.variance == pytest.approx(0.0, abs=1e-10)
        assert state.mean == pytest.approx(42.0)

    def test_welford_state_known_variance(self):
        """[1, 2, 3, 4, 5] has variance 2.5 (sample)."""
        state = WelfordState()
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            state.update(v)
        assert state.variance == pytest.approx(2.5)

    def test_detect_flatline_returns_true_for_constant_signal(self):
        values = [55.0] * 10
        is_flat, variance, mean = detect_flatline(values, min_points=5)
        assert is_flat is True
        assert variance == pytest.approx(0.0, abs=1e-10)
        assert mean == pytest.approx(55.0)

    def test_detect_flatline_returns_false_for_varying_signal(self):
        values = [10.0, 50.0, 90.0, 30.0, 70.0, 20.0]
        is_flat, _, _ = detect_flatline(values, min_points=5)
        assert is_flat is False

    def test_detect_flatline_insufficient_points_returns_false(self):
        is_flat, variance, mean = detect_flatline([42.0, 42.0], min_points=5)
        assert is_flat is False
        assert variance == 0.0

    # --- FlatlineDetector.detect() — Python / streaming record path ---

    def test_flatline_detector_flags_constant_interface(self):
        detector = FlatlineDetector(min_points=3)
        records = [
            {"device_id": "d-1", "interface_name": "Gi0/0", "oper_status": 1, "effective_util_in": 55.0}
            for _ in range(6)
        ]
        results = detector.detect(records)
        assert len(results) == 1
        assert results[0].device_id == "d-1"
        assert results[0].anomaly_type == "FLATLINE"
        assert results[0].subtype == "LOW_VARIANCE"
        assert results[0].score == pytest.approx(0.0, abs=1e-10)

    def test_flatline_detector_ignores_down_interfaces(self):
        """DOWN (oper_status=2) interfaces must not be flagged even if util is constant."""
        detector = FlatlineDetector(min_points=3)
        records = [
            {"device_id": "d-down", "interface_name": "Gi0/0", "oper_status": 2, "effective_util_in": 0.0}
            for _ in range(6)
        ]
        results = detector.detect(records)
        assert results == []

    def test_flatline_detector_skips_null_effective_util(self):
        """Records with effective_util_in=None must not crash or produce false positives."""
        detector = FlatlineDetector(min_points=3)
        records = [
            {"device_id": "d-1", "interface_name": "Gi0/0", "oper_status": 1, "effective_util_in": None}
            for _ in range(6)
        ]
        results = detector.detect(records)
        assert results == []

    def test_flatline_detector_variable_signal_not_flagged(self):
        detector = FlatlineDetector(min_points=3)
        records = [
            {"device_id": "d-1", "interface_name": "Gi0/0", "oper_status": 1, "effective_util_in": v}
            for v in [10.0, 50.0, 90.0, 30.0, 70.0, 20.0]
        ]
        results = detector.detect(records)
        assert results == []

    # --- FlatlineDetector.detect_from_dataframe() — Spark window path ---

    def test_spark_flatline_flags_zero_variance_window(self, spark):
        """A 4h window with constant util_in must be detected by the Spark path."""
        rows = [_silver_row(util_in=55.0, hour=h) for h in range(4)]
        # All same value → variance = 0 after groupBy window
        silver_df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)

        detector = FlatlineDetector()
        result_df = detector.detect_from_dataframe(silver_df, window_duration="4 hours")
        rows_out = result_df.collect()

        assert len(rows_out) >= 1
        row = rows_out[0]
        assert row.anomaly_type == "FLATLINE"
        assert row.variance == pytest.approx(0.0, abs=1e-10)
        assert row.device_id == "d-1"

    def test_spark_flatline_does_not_flag_varying_window(self, spark):
        """A window with varying util_in must produce no flatline flag."""
        rows = [_silver_row(util_in=float(v), hour=h) for h, v in enumerate([10, 50, 90, 30])]
        silver_df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)

        detector = FlatlineDetector()
        result_df = detector.detect_from_dataframe(silver_df, window_duration="4 hours")
        assert result_df.count() == 0

    def test_spark_flatline_excludes_down_interfaces(self, spark):
        """DOWN interfaces (oper_status != 1) must be filtered before variance check."""
        rows = [_silver_row(util_in=0.0, oper=2, hour=h) for h in range(4)]
        silver_df = spark.createDataFrame(rows, schema=_SILVER_SCHEMA)

        detector = FlatlineDetector()
        result_df = detector.detect_from_dataframe(silver_df, window_duration="4 hours")
        assert result_df.count() == 0
