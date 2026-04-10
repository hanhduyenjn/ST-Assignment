-- Silver: validated + enriched interface stats.
-- Partitioned by site_id (identity) + date for optimal site+range queries.
-- Sorted by device_id, interface_name, device_ts within each partition file.

CREATE TABLE IF NOT EXISTS silver.interface_stats (
    device_id           string,
    site_id             string,
    interface_name      string,
    vendor              string,
    role                string,
    device_ts           timestamp   COMMENT 'parsed from raw ts field',
    util_in             double      COMMENT 'raw utilisation in',
    util_out            double      COMMENT 'raw utilisation out',
    effective_util_in   double      COMMENT 'zeroed when oper_status IN (2,3)',
    effective_util_out  double      COMMENT 'zeroed when oper_status IN (2,3)',
    admin_status        int,
    oper_status         int,
    ingest_z_score      double      COMMENT 'z-score vs device baseline_mean/std',
    ingest_iqr_score    double      COMMENT 'IQR fence score vs rolling 4h window',
    ingest_anomaly      boolean     COMMENT 'true if ingest-time anomaly rule fired',
    _extra_cols         map<string, string>  COMMENT 'schema-evolved extra columns',
    _ingested_at        timestamp,
    _partition_date     date
)
USING iceberg
PARTITIONED BY (identity(site_id), days(_partition_date))
-- Sort order within partition for column statistics skip
ORDER BY (device_id, interface_name, device_ts)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.metadata.metrics.default' = 'truncate(36)',
    -- schema evolution: new columns added automatically on write
    'write.spark.accept-any-schema' = 'true'
);
