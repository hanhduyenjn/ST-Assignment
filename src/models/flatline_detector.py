"""Flatline Detection via Welford Online Variance Algorithm (ALTERNATIVE IMPLEMENTATION).

This module provides a lightweight, streaming-friendly flatline detector using Welford's
algorithm for online variance calculation. It's designed as a future drop-in replacement
for the current Spark Structured Streaming approach in src/pipeline/flatline_streaming.py

Current Status: NOT YET INTEGRATED into the main pipeline.

When to Use This Over flatline_streaming.py:
  - Processing micro-batches instead of 4-hour windows
  - Lightweight per-record processing with minimal state
  - Integrating with the pluggable AnomalyDetector interface
  - Swapping variance calculation algorithms

Trade-offs:
  ✓ Lower memory footprint (O(1) state per detector)
  ✓ Single-pass variance calculation
  ✓ No distributed Spark needed for basic operation
  ✗ Cannot partition across multiple workers
  ✗ Needs explicit windowing logic (Welford just accumulates)

Integration Path (future):
  1. Refactor main streaming pipeline to collect micro-batches per device+interface
  2. Feed batches to FlatlineDetector.detect() method
  3. Emit AnomalyResult objects to ClickHouse writer
"""
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.models.base import AnomalyDetector, AnomalyResult
from transforms.flatline_v2 import detect_flatline


class FlatlineDetector(AnomalyDetector):
    """Welford online variance detector for flatline anomalies.

    Detects when a time series becomes flat (low variance) using Welford's
    numerically-stable online variance algorithm. Requires a minimum number
    of observations before making a detection.
    """

    def __init__(
        self,
        *,
        variance_threshold: float = 1e-12,
        min_points: int = 5,
    ) -> None:
        self.variance_threshold = variance_threshold
        self.min_points = min_points

    def detect(self, records: list[dict[str, Any]]) -> list[AnomalyResult]:
        grouped: dict[tuple[str, str], list[float]] = {}
        for record in records:
            key = (str(record.get("device_id", "")), str(record.get("interface_name", "")))
            if record.get("oper_status") != 1:
                continue
            util = record.get("effective_util_in")
            if util is None:
                continue
            grouped.setdefault(key, []).append(float(util))

        results: list[AnomalyResult] = []
        for (device_id, interface_name), values in grouped.items():
            is_flat, variance, mean_util = detect_flatline(values, min_points=self.min_points, eps=self.variance_threshold)
            if is_flat:
                results.append(
                    AnomalyResult(
                        device_id=device_id,
                        interface_name=interface_name,
                        anomaly_type="FLATLINE",
                        score=variance,
                        subtype="LOW_VARIANCE",
                        metadata={
                            "mean_util_in": mean_util,
                            "variance": variance,
                        },
                    )
                )
        return results

    def detect_from_dataframe(self, df: DataFrame, window_duration: str = "4 hours") -> DataFrame:
        """DataFrame method for batch processing (for testing/validation only).

        NOTE: This uses Spark's F.variance(), not Welford. Use the detect() method
        for actual Welford-based processing.
        """
        return (
            df.filter(F.col("oper_status") == 1)
            .groupBy(
                "device_id",
                "site_id",
                "interface_name",
                F.window("device_ts", window_duration).alias("w"),
            )
            .agg(
                F.coalesce(F.variance("effective_util_in"), F.lit(0.0)).alias("variance"),
                F.avg("effective_util_in").alias("mean_util_in"),
                F.first("oper_status").alias("oper_status"),
            )
            .filter(F.col("variance") <= F.lit(self.variance_threshold))
            .select(
                "device_id",
                "site_id",
                "interface_name",
                F.col("w.start").alias("window_start"),
                F.col("w.end").alias("window_end"),
                F.lit("FLATLINE").alias("anomaly_type"),
                F.col("variance"),
                F.col("mean_util_in"),
                "oper_status",
                F.current_timestamp().alias("detected_at"),
            )
        )


def detect_flatline_batch(records: list[dict], variance_threshold: float = 1e-12, min_points: int = 5) -> list[AnomalyResult]:
    """Convenience function to detect flatlines in a batch of records using Welford algorithm.

    Useful for integrating FlatlineDetector into a streaming pipeline's foreachBatch callback.

    Example usage in streaming pipeline:
        detector = FlatlineDetector(variance_threshold=1e-12, min_points=5)

        def process_batch(batch_df: DataFrame, batch_id: int, ch: ClickHouseWriter) -> None:
            records = batch_df.select("device_id", "interface_name", "effective_util_in", "oper_status").toPandas().to_dict(orient="records")
            anomalies = detect_flatline_batch(records)
            if anomalies:
                ch.insert_anomalies(anomalies)

    Args:
        records: List of dictionaries with keys: device_id, interface_name, effective_util_in, oper_status
        variance_threshold: Variance below this triggers flatline detection
        min_points: Minimum samples required before making a detection

    Returns:
        List of AnomalyResult objects
    """
    detector = FlatlineDetector(variance_threshold=variance_threshold, min_points=min_points)
    return detector.detect(records)
