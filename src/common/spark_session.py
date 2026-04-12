from __future__ import annotations

import os

from pyspark.sql import SparkSession

from src.common.config import (
    ICEBERG_REST_URL,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)

_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
_ICEBERG_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"
# Jars are preloaded into the image by docker/Dockerfile. Glob is expanded by
# Spark's classpath loader, so the catalog is registered before any SQL runs.
_SPARK_JARS_DIR = os.environ.get("SPARK_JARS_DIR", "/opt/spark-jars")
_SPARK_JARS_GLOB = f"{_SPARK_JARS_DIR}/*"


def _base_builder(app_name: str) -> SparkSession.Builder:
    jvm_compat_opts = os.environ.get(
        "SPARK_JAVA_COMPAT_OPTS",
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    )
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.master", os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.driver.extraClassPath", _SPARK_JARS_GLOB)
        .config("spark.executor.extraClassPath", _SPARK_JARS_GLOB)
        .config("spark.driver.extraJavaOptions", jvm_compat_opts)
        .config("spark.executor.extraJavaOptions", jvm_compat_opts)
        .config("spark.sql.extensions", _ICEBERG_EXTENSIONS)
        .config("spark.sql.defaultCatalog", "rest")
        .config("spark.sql.catalog.rest", _ICEBERG_CATALOG_IMPL)
        .config("spark.sql.catalog.rest.type", "rest")
        .config("spark.sql.catalog.rest.uri", ICEBERG_REST_URL)
        .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.rest.s3.access-key-id", MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.rest.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.rest.s3.path-style-access", "true")
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
        if mode not in ("streaming", "batch"):
            raise ValueError(f"Unknown SparkSession mode: {mode!r}. Use 'streaming' or 'batch'.")
        name = app_name or f"network-health-{mode}"
        builder = _base_builder(name)

        if mode == "batch":
            shuffle_partitions = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4")
            builder = (
                builder
                .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1500m"))
                .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1500m"))
                .config("spark.sql.shuffle.partitions", shuffle_partitions)
                .config("spark.default.parallelism", shuffle_partitions)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
                .config("spark.network.timeout", os.environ.get("SPARK_NETWORK_TIMEOUT", "600s"))
                .config("spark.executor.heartbeatInterval", os.environ.get("SPARK_HEARTBEAT_INTERVAL", "60s"))
            )

        if mode == "streaming":
            shuffle_partitions = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")
            builder = (
                builder
                .config("spark.sql.shuffle.partitions", shuffle_partitions)
                .config("spark.default.parallelism", shuffle_partitions)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", shuffle_partitions)
                .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1500m"))
                .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1500m"))
                .config("spark.driver.maxResultSize", os.environ.get("SPARK_DRIVER_MAX_RESULT_SIZE", "512m"))
                .config("spark.network.timeout", os.environ.get("SPARK_NETWORK_TIMEOUT", "300s"))
                .config("spark.executor.heartbeatInterval", os.environ.get("SPARK_HEARTBEAT_INTERVAL", "30s"))
                .config("spark.sql.session.timeZone", "UTC")
            )

        return builder.getOrCreate()
