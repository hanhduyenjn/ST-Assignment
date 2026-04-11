from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

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

    def write_dataframe(self, df: DataFrame, target: str, mode: str = "append") -> None:
        if mode == "append":
            df.writeTo(target).append()
        elif mode == "overwrite":
            df.writeTo(target).overwritePartitions()
        else:
            raise ValueError(f"Unsupported write mode: {mode}")
