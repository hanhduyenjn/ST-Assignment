-- Silver: DLQ quarantine table.
-- Written by Stage 1 validator for every record that fails ValidatorChain.
-- Read by nightly DLQReprocessor to classify and promote fixable records.

CREATE TABLE IF NOT EXISTS silver.dlq_quarantine (
    raw_payload             string      COMMENT 'original message — never mutated',
    quarantine_reason       string      COMMENT 'NULL_FIELD | NEGATIVE_UTIL | EXCEEDS_MAX | ORPHANED_DEVICE | INVALID_TIMESTAMP | STALE_TIMESTAMP | FUTURE_TIMESTAMP',
    source_topic            string,
    quarantined_at          timestamp,
    reprocessed             boolean     COMMENT 'default false; set true after reprocessor runs',
    reprocessed_at          timestamp,  -- nullable
    quarantine_resolution   string,     -- 'PROMOTED_TO_SILVER' | 'PERMANENT' | null
    _partition_date         date
)
USING iceberg
PARTITIONED BY (days(_partition_date))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);
