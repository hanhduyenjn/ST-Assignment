-- Bronze: raw interface stats — append-only, no transformations, permanent retention.
-- Schema evolution: _extra_cols captures any unknown columns (e.g. packet_loss)
-- so the pipeline never hard-fails on new fields.

CREATE TABLE IF NOT EXISTS bronze.interface_stats (
    _raw_payload    string       COMMENT 'original JSON string, never mutated',
    ts              string       COMMENT 'as-received, unparsed',
    device_id       string,
    interface_name  string,
    util_in         double       COMMENT 'raw — may be negative or >100',
    util_out        double       COMMENT 'raw',
    admin_status    int,
    oper_status     int,
    _extra_cols     map<string, string>  COMMENT 'any unknown columns (schema evolution)',
    _ingested_at    timestamp    COMMENT 'broker receipt time',
    _source_topic   string,      -- 'raw.interface-stats'
    _partition_date date         COMMENT 'partition key'
)
USING iceberg
PARTITIONED BY (_source_topic, days(_partition_date))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);
