-- Silver: streaming flatline detection output (Stage 3b).
-- Written by FlatlineDetector via IcebergWriter.

CREATE TABLE IF NOT EXISTS silver.anomaly_flags (
    device_id       string,
    site_id         string,
    interface_name  string,
    window_start    timestamp,
    window_end      timestamp,
    anomaly_type    string      COMMENT 'FLATLINE',
    variance        double      COMMENT 'should be 0.0 for true flatline',
    mean_util_in    double,
    oper_status     int         COMMENT 'must be 1 (UP) — flatline on DOWN is not anomalous',
    detected_at     timestamp,
    _partition_date date
)
USING iceberg
PARTITIONED BY (identity(site_id), days(_partition_date))
ORDER BY (device_id, interface_name, window_start)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);
