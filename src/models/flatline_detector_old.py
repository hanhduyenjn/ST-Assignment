from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.models.base import AnomalyDetector, AnomalyResult
from transforms.flatline_v1 import detect_flatline


class FlatlineDetector(AnomalyDetector):
    """Streaming-friendly detector based on zero variance over a moving window."""

    def __init__(self, min_points: int = 5) -> None:
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
            is_flat, variance, mean_util = detect_flatline(values, min_points=self.min_points)
            if is_flat:
                results.append(
                    AnomalyResult(
                        device_id=device_id,
                        interface_name=interface_name,
                        anomaly_type="FLATLINE",
                        score=variance,
                        subtype="LOW_VARIANCE",
                        metadata={"mean_util_in": mean_util, "variance": variance},
                    )
                )
        return results

    def detect_from_dataframe(self, df: DataFrame, window_duration: str = "4 hours") -> DataFrame:
        """Spark implementation used by the streaming pipeline stage."""
        agg = (
            df.filter(F.col("oper_status") == 1)
            .groupBy(
                "device_id",
                "site_id",
                "interface_name",
                F.window("device_ts", window_duration).alias("w"),
            )
            .agg(
                F.variance("effective_util_in").alias("variance"),
                F.avg("effective_util_in").alias("mean_util_in"),
                F.first("oper_status").alias("oper_status"),
            )
            .withColumn("variance", F.coalesce(F.col("variance"), F.lit(0.0)))
            .filter(F.col("variance") == 0.0)
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
                F.to_date(F.current_timestamp()).alias("_partition_date"),
            )
        )
        return agg
