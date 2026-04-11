# Iceberg DDL Files

This directory contains all table definitions (DDL) for the ST-Assignment data warehouse.

## Files

**Bronze (raw/validated):**
- `bronze_interface_stats.sql` - Raw interface statistics from network devices
- `bronze_syslogs.sql` - Raw syslog messages from network devices  
- `bronze_inventory.sql` - Device inventory snapshots

**Silver (enriched/validated):**
- `silver_interface_stats.sql` - Validated + enriched interface statistics with anomaly scores
- `silver_syslogs.sql` - Validated + enriched syslog events
- `silver_dlq_quarantine.sql` - Dead letter queue for failed records
- `silver_anomaly_flags.sql` - Flatline detection anomaly records

## Quick Start

### Option 1: Run init script (Recommended)

```bash
cd ST-Assignment
ICEBERG_REST_CATALOG_URL=http://localhost:8181 \
MINIO_ENDPOINT=http://localhost:9000 \
MINIO_ROOT_USER=minioadmin \
MINIO_ROOT_PASSWORD=minioadmin123 \
python sql/iceberg/ddl/init.py
```

This script:
- Reads all `.sql` files in this directory
- Creates `rest.bronze` and `rest.silver` namespaces
- Handles Spark version compatibility (removes ORDER BY clauses)
- Qualifies table names with the `rest.` catalog prefix
- Creates `pending_enrichment` table (inline, not in DDL files)
- Shows verification output

### Option 2: Manual execution

Using `spark-sql`:
```bash
spark-sql --jars ... -i bronze_interface_stats.sql -i silver_interface_stats.sql ...
```

### Option 3: From Python

```python
from src.admin.init_iceberg_tables import init_iceberg_tables
init_iceberg_tables()
```

## Schema Design

All tables use:
- **Catalog:** `rest` (Iceberg REST Catalog with MinIO backend)
- **Partitioning:** `days(_partition_date)` for time-based pruning
- **Format:** Parquet with Snappy compression
- **Schema Evolution:** Enabled for silver tables

## Notes

- `ORDER BY` clauses in SQL files are removed during init (Spark 3.5 limitation)
- Table names are auto-qualified during init (silver. → rest.silver.)
- The `pending_enrichment` table is created inline since it's not in DDL files
- Run init script whenever environment is reset
