-- Bronze: raw syslog events — append-only, no transformations, permanent retention.

CREATE TABLE IF NOT EXISTS bronze.syslogs (
    _raw_payload    string       COMMENT 'original JSON string',
    ts              string       COMMENT 'as-received, unparsed',
    device_id       string,
    severity        int,
    message         string,
    _ingested_at    timestamp,
    _source_topic   string,      -- 'raw.syslogs'
    _partition_date date
)
USING iceberg
PARTITIONED BY (days(_partition_date))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);
