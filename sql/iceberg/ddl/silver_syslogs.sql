-- Silver: validated + enriched syslog events.

CREATE TABLE IF NOT EXISTS silver.syslogs (
    device_id       string,
    site_id         string,
    vendor          string,
    role            string,
    device_ts       timestamp,
    severity        int,
    is_critical     boolean     COMMENT 'severity < 3 (EMERGENCY, ALERT, CRITICAL)',
    message         string,
    _ingested_at    timestamp,
    _partition_date date
)
USING iceberg
PARTITIONED BY (identity(site_id), days(_partition_date))
ORDER BY (device_id, device_ts)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);
