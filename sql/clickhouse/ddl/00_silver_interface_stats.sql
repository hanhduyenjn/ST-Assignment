-- ClickHouse read-only view of Iceberg silver.interface_stats via the Iceberg table engine.
-- No data is stored in ClickHouse — reads go directly to MinIO S3 at query time.
-- Source for mv_site_health_hourly (REFRESHABLE MATERIALIZED VIEW).
--
-- Prerequisites: ClickHouse v24.1+ with s3 + Iceberg engine enabled.
-- Credentials should be stored in a ClickHouse Named Collection in production;
-- inline here for PoC convenience.
--
-- S3 path: http://<minio>/<bucket>/warehouse/<namespace>/<table>/
-- Adjust host, bucket, and credentials to match your docker-compose.yml.

CREATE TABLE IF NOT EXISTS network_health.silver_interface_stats_src
ENGINE = Iceberg(
    'http://minio:9000/lakehouse/warehouse/silver/interface_stats/',
    'minioadmin',
    'minioadmin123'
);
