-- Silver: pending enrichment backlog.
-- Records that passed Stage 1 validation but lack inventory (site_id).
-- Stored here waiting for inventory updates to enable Stage 2 enrichment.
-- Reprocessed by DLQReprocessor when new inventory arrives.

CREATE TABLE IF NOT EXISTS silver.pending_enrichment (
    raw_payload     string      COMMENT 'original message from bronze',
    source_topic    string,
    parked_at       timestamp   COMMENT 'when record arrived lacking enrichment',
    device_id       string,
    enriched        boolean     COMMENT 'default false; set true after enrichment',
    enriched_at     timestamp,  -- nullable
    retry_count     int         COMMENT 'incremented by reprocessor',
    _partition_date date
)
USING iceberg
PARTITIONED BY (days(_partition_date))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);
