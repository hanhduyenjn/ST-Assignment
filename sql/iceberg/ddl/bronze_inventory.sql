-- Bronze: device inventory snapshots — append-only, full history of all snapshots.
-- Compacted Kafka topic means latest device state is always present.

CREATE TABLE IF NOT EXISTS bronze.inventory (
    device_id       string,
    site_id         string,
    vendor          string,
    role            string,
    _ingested_at    timestamp,
    _source_topic   string,      -- 'raw.inventory'
    _partition_date date
)
USING iceberg
PARTITIONED BY (days(_partition_date))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);
