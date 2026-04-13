-- ClickHouse read-only view of Iceberg silver.syslogs via the Iceberg table engine.
-- No data is stored in ClickHouse — reads go directly to MinIO S3 at query time.
-- Joined by mv_site_health_hourly to compute critical_syslog_count per site per hour.
--
-- Prerequisites: ClickHouse v24.1+ with s3 + Iceberg engine enabled.

CREATE TABLE IF NOT EXISTS network_health.silver_syslogs_src
ENGINE = Iceberg(
    'http://minio:9000/lakehouse/silver/syslogs/',
    'minioadmin',
    'minioadmin123'
);
