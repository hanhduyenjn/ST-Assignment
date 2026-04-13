from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.storage.base import StorageWriter


class IcebergWriter(StorageWriter):
    """Utility writer for Spark DataFrames and row payloads."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def write_rows(self, rows: list[dict[str, Any]], target: str) -> None:
        if not rows:
            return
        df = self.spark.createDataFrame(rows)
        self.write_dataframe(df, target)

    def _align_to_target_schema(self, df: DataFrame, target: str) -> DataFrame:
        """Align incoming DataFrame columns to the existing Iceberg table schema.

        This protects append writes from historical naming drift across pipeline
        versions (for example, ingested_ts vs _ingested_at).
        """
        target_schema = self.spark.table(target).schema

        # Map legacy/current aliases used across pipeline revisions.
        alias_candidates: dict[str, list[str]] = {
            "_ingested_at": ["ingested_ts"],
            "ingested_ts": ["_ingested_at"],
            "util_in": ["raw_util_in"],
            "util_out": ["raw_util_out"],
            "raw_util_in": ["util_in"],
            "raw_util_out": ["util_out"],
            "_extra_cols": ["extra_cols"],
            "extra_cols": ["_extra_cols"],
        }

        for field in target_schema:
            if field.name in df.columns:
                continue
            for candidate in alias_candidates.get(field.name, []):
                if candidate in df.columns:
                    df = df.withColumn(field.name, F.col(candidate))
                    break

        # Fill any still-missing target fields as typed nulls.
        for field in target_schema:
            if field.name not in df.columns:
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))

        # Write only the target schema columns in the target order.
        return df.select(*[F.col(field.name) for field in target_schema])

    def write_dataframe(self, df: DataFrame, target: str, mode: str = "append") -> None:
        aligned = self._align_to_target_schema(df, target)
        writer = aligned.writeTo(target)
        if mode == "append":
            writer.append()
        elif mode == "overwrite":
            writer.overwritePartitions()
        else:
            raise ValueError(f"Unsupported write mode: {mode}")
