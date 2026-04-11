from __future__ import annotations

from pyspark.sql import SparkSession

from src.common.config import (
    ICEBERG_REST_URL,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)

_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
_ICEBERG_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"


def _base_builder(app_name: str) -> SparkSession.Builder:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", _ICEBERG_EXTENSIONS)
        .config("spark.sql.catalog.lakehouse", _ICEBERG_CATALOG_IMPL)
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", ICEBERG_REST_URL)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        # S3A configuration for hadoop/iceberg file I/O
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        # Alternative s3 scheme support (maps to s3a)
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3.secret.key", MINIO_SECRET_KEY)
    )


class SparkSessionFactory:
    @staticmethod
    def create(mode: str = "streaming", app_name: str | None = None) -> SparkSession:
        """Create a SparkSession configured for the given mode.

        Parameters
        ----------
        mode:
            "streaming" — includes Kafka + Iceberg jars, used by streaming jobs.
            "batch"     — includes Iceberg jars only, used by batch jobs.
        app_name:
            Override the default app name derived from mode.
        """
        name = app_name or f"network-health-{mode}"
        builder = _base_builder(name)

        if mode == "streaming":
            builder = builder.config(
                "spark.jars.packages",
                ",".join([
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
                    "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                ]),
            )
        elif mode == "batch":
            builder = builder.config(
                "spark.jars.packages",
                ",".join([
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
                    "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                ]),
            )
        else:
            raise ValueError(f"Unknown SparkSession mode: {mode!r}. Use 'streaming' or 'batch'.")

        return builder.getOrCreate()
