# Network Health Platform — PoC Implementation Plan

> Internal engineering plan. Not the ADR. This document captures every technical
> decision, design pattern, data model, and file-level responsibility agreed upon
> before a single line of code is written.

---

## Table of Contents

1. [Assignment Recap](#1-assignment-recap)
2. [Architecture Overview](#2-architecture-overview)
3. [Technology Stack](#3-technology-stack)
4. [Data Flow](#4-data-flow)
5. [Data Model](#5-data-model)
6. [Repository Structure](#6-repository-structure)
7. [Design Patterns](#7-design-patterns)
8. [Component Specifications](#8-component-specifications)
   - 8.1 [Source Consumers](#81-source-consumers)
   - 8.2 [Validators](#82-validators)
   - 8.3 [Spark Streaming Pipeline](#83-spark-streaming-pipeline)
   - 8.4 [Spark Batch Jobs](#84-spark-batch-jobs)
   - 8.5 [Storage Layer](#85-storage-layer)
   - 8.6 [Query & Analytics Layer](#86-query--analytics-layer)
   - 8.7 [Anomaly Detection](#87-anomaly-detection)
   - 8.8 [Monitoring & Observability](#88-monitoring--observability)
9. [Anomaly Detection Strategy](#9-anomaly-detection-strategy)
   - 9.1 [Ingest-Time Detection](#91-ingest-time-detection)
   - 9.2 [Post-Processing Detection](#92-post-processing-detection)
   - 9.3 [Feedback Loop](#93-feedback-loop)
10. [Schema Evolution Strategy](#10-schema-evolution-strategy)
11. [Kafka Topic Design](#11-kafka-topic-design)
12. [Iceberg Catalog & Table Design](#12-iceberg-catalog--table-design)
13. [ClickHouse Table & Materialized View Design](#13-clickhouse-table--materialized-view-design)
14. [Health Score Formula](#14-health-score-formula)
15. [DLQ Reprocessing Logic](#15-dlq-reprocessing-logic)
16. [Infrastructure — Docker Compose Services](#16-infrastructure--docker-compose-services)
17. [Production Upgrade Path](#17-production-upgrade-path)
18. [File-Level Responsibility Map](#18-file-level-responsibility-map)
19. [Build Order](#19-build-order)

---

## 1. Assignment Recap

**Context:** Network Management Platform migrating from poll-and-store to a modern
on-premises data platform. No internet connectivity. Open-source only.

**Timebox:** 6–8 hours.

**Three input sources:**
- `device_inventory.csv` — static device registry (`device_id, site_id, vendor, role`)
- `interface_stats.csv` — high-velocity telemetry (`ts, device_id, interface_name, util_in, util_out, admin_status, oper_status`)
- `syslogs.jsonl` — event stream (`ts, device_id, severity 0–7, message`)

**Core requirements:**
1. Defensive ingestion with automatic DLQ quarantine
2. Effective utilization override when `oper_status` is DOWN (2) or TESTING (3)
3. Site health score aggregated by `site_id` and hour — formula self-defined
4. Schema evolution — handle new columns (e.g. `packet_loss`) without hard-failing

**Challenge 1 — Migration Architect:**
- Describe 50GB/day Spark + Lakehouse architecture
- Partitioning strategy for `site_id + time_range` queries
- Code bonus: SparkSQL window functions for aggregation

**Challenge 2 — AI & Real-Time:**
- Describe anomaly detection model placement: streaming vs batch, with justification
- Code bonus: Python flatline detection (variance = 0 over moving window)

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  SOURCES                                                         │
│  CsvConsumer · JsonlConsumer · MqttConsumer(stub)               │
│  SourceConsumer ABC + registry                                   │
└──────────────────────┬──────────────────────────────────────────┘
                       │ produce
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  KAFKA  (3 raw input topics · 1 alert output topic)             │
│  raw.interface-stats (7d) · raw.syslogs (7d)                    │
│  raw.inventory (compacted) · output.anomaly-alerts (7d)         │
└──────┬─────────────────────────────────────┬────────────────────┘
       │ Spark reads                          │ Spark reads
       ▼                                      ▼
┌──────────────────────┐          ┌──────────────────────────────┐
│  SPARK BATCH         │          │  SPARK STRUCTURED STREAMING  │
│  Bronze ingest job   │          │  Single app · single DAG     │
│  raw → bronze/       │          │  Stage 1: validate + DLQ     │
│  (Iceberg)           │          │  Stage 2: enrich             │
└──────────────────────┘          │  Stage 3a: health score      │
                                  │  Stage 3b: flatline detect   │
                                  │  → writes silver/ (Iceberg)  │
                                  └──────────────┬───────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  ICEBERG LAKEHOUSE  (REST Catalog · MinIO object store)         │
│  bronze/ · silver/ · dlq_quarantine                             │
│  Partitioned by site_id / date                                  │
└──────┬──────────────────────────────┬───────────────────────────┘
       │ Iceberg table engine          │ DuckDB reads
       ▼                              ▼
┌──────────────────────┐    ┌─────────────────────┐
│  CLICKHOUSE          │    │  DUCKDB             │
│  Reads silver via    │    │  Ad-hoc exploration │
│  Iceberg engine      │    │  FastAPI bridge     │
│  MV → Gold tables    │    │  → Grafana JSON DS  │
│  gold_site_health    │    └─────────────────────┘
│  gold_anomaly_flags  │
│  device_baseline_    │
│  params              │
└──────┬───────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────┐
│  GRAFANA  (queries ClickHouse · sub-second latency)             │
│  Pipeline dashboard · Data quality dashboard · Anomaly dashboard│
└─────────────────────────────────────────────────────────────────┘

SPARK BATCH (nightly scheduled)
  · DLQ reprocessor      reads dlq_quarantine → re-validates → silver
  · Isolation Forest     reads silver (30d) → gold_anomaly_flags (ClickHouse)
  · Tree distillation    trains shallow decision tree → streaming rules
  · Backfill job         bronze → silver (Challenge 1 SparkSQL window fns)
  · PSI drift job        weekly · updates device_baseline_params
```

---

## 3. Technology Stack

| Layer | Technology | Justification |
|---|---|---|
| Message bus | Apache Kafka | Decouples producers/consumers, replay buffer, compacted topics for inventory |
| Stream processing | Spark Structured Streaming | Explicitly requested in Challenge 1; same codebase as batch |
| Batch processing | Apache Spark | Challenge 1 requirement; window functions, backfill |
| Lakehouse format | Apache Iceberg (REST Catalog) | Schema evolution built-in; time travel; ACID; partition evolution |
| Object store | MinIO | On-premises S3-compatible; Iceberg backend |
| Iceberg catalog | REST Catalog (Polaris / simple REST) | Decouples catalog from compute; any engine can connect |
| Gold layer / analytics | ClickHouse | Sub-second query latency; native Iceberg table engine; materialized views; Grafana connector |
| Ad-hoc queries | DuckDB | Zero infra; reads Iceberg directly; Python API |
| Dashboard | Grafana | Standard on-prem; ClickHouse + JSON datasource plugins |
| Alerting | Prometheus + Alertmanager | Standard on-prem; scrapes custom metrics exporter |
| ML — batch | scikit-learn IsolationForest | Unsupervised; no labels needed; joblib/ONNX export |
| ML — distillation | scikit-learn DecisionTreeClassifier | Distills forest into streaming rules |
| ML — retraining gate | Held-out validation window | Candidate model only promoted if it outperforms current; prevents contaminated retraining |
| Feature store (future) | ScyllaDB | Device behavioral embeddings; low-latency streaming lookup |
| Container | Docker Compose | On-premises, no Kubernetes required for PoC |

---

## 4. Data Flow

### 4.1 Streaming Path (operational, low-latency)

```
CsvConsumer / JsonlConsumer
    │ produce raw JSON records
    ▼
Kafka raw topics
    │ Spark reads via readStream
    ▼
Stage 1 — Ingest + Validate
    │ ValidatorChain: NullCheck → RangeCheck → OrphanCheck
    │ Extra columns kept as extra_cols map (schema evolution)
    ├── bad records → dlq_quarantine (Iceberg)
    └── clean records → in-memory DataFrame
    │
    ▼
Stage 2 — Enrich (in-memory, no Kafka hop)
    │ Stream-static join: stats + syslogs ⟕ bronze/inventory on device_id
    │ Attach site_id, vendor, role
    │ oper_status IN (2,3) → effective_util_in = 0.0, effective_util_out = 0.0
    │ Enrich with device_baseline_params from ClickHouse (broadcast join)
    └── enriched DataFrame in memory
    │
    ├──▶ Stage 3a — Health Score
    │        Tumbling 1h window per site_id
    │        Join enriched interface-stats + syslogs on site_id + window
    │        Compute health_score, is_degraded
    │        → IcebergWriter → silver/site_health_hourly (triggers CH MV)
    │
    └──▶ Stage 3b — Flatline Detection
             Sliding 4h window, slide 5min, per device_id + interface_name
             Welford online variance, oper_status == UP
             variance == 0 → AnomalyResult(type=FLATLINE)
             → IcebergWriter → silver/anomaly_flags
             → Kafka output.anomaly-alerts (real-time Prometheus consumption)
```

### 4.2 Bronze Ingest Path (Spark batch, replaces Kafka Connect)

```
Kafka raw topics
    │ Spark reads via read (batch, offset-based)
    │ Scheduled every 15 minutes (or triggered)
    ▼
BronzeIngestJob
    │ No transformation — raw payload preserved exactly
    │ Add _ingested_at, _source_topic metadata columns
    │ Write to bronze/ partitioned by _source_topic / date
    ▼
Iceberg bronze/ tables (append-only, permanent retention)
```

### 4.3 Batch Path (nightly, post-processing)

```
silver/interface_stats (Iceberg, last 30 days)
    │
    ├──▶ IsolationForestDetector.train()  [weekly]
    │        compute feature vectors (10 features per device+interface window)
    │        fit IsolationForest(n_estimators=100, contamination=0.05)
    │        distill → DecisionTreeClassifier(max_depth=4) → streaming rules
    │        persist model + scaler + rules to MinIO
    │        update device_baseline_params in ClickHouse
    │
    ├──▶ IsolationForestDetector.detect()  [nightly]
    │        load model from MinIO
    │        compute feature vectors on last 24h
    │        score + classify subtype (FLAPPING, LOW_VARIANCE, RAPID_DRIFT, ...)
    │        → ClickHouse gold_anomaly_flags (anomaly_type=MODEL)
    │
    ├──▶ PSIDriftDetector  [weekly]
    │        compute PSI per device per metric vs 30-day baseline
    │        PSI > 0.25 → significant drift
    │        update device_baseline_params (new q1, q3, iqr_k)
    │        streaming job auto-adapts on next micro-batch
    │
    ├──▶ DLQReprocessor  [nightly]
    │        read dlq_quarantine where reprocessed=false
    │        classify: fixable (orphaned device now in inventory) vs permanent
    │        fixable → re-run ValidatorChain → promote to silver
    │        permanent → set reprocessed=true, quarantine_resolution=PERMANENT
    │
    └──▶ BackfillJob  [on-demand, Challenge 1]
             read bronze/interface_stats
             SparkSQL window functions for health score aggregation
             partitioned writes to silver + ClickHouse Gold tables
             idempotent — safe to rerun
```

---

## 5. Data Model

### 5.1 Iceberg Tables (REST Catalog)

#### `bronze.interface_stats`
Raw, append-only. No transforms. Permanent retention.

| Column | Type | Notes |
|---|---|---|
| `_raw_payload` | string | original JSON string |
| `ts` | string | as-received, unparsed |
| `device_id` | string | |
| `interface_name` | string | |
| `util_in` | double | raw, may be negative or >100 |
| `util_out` | double | raw |
| `admin_status` | int | |
| `oper_status` | int | |
| `_extra_cols` | map<string,string> | any unknown columns |
| `_ingested_at` | timestamp | broker receipt time |
| `_source_topic` | string | `raw.interface-stats` |
| `_partition_date` | date | partition key |

Partition spec: `_source_topic`, `days(_partition_date)`

---

#### `bronze.syslogs`
Raw, append-only.

| Column | Type | Notes |
|---|---|---|
| `_raw_payload` | string | |
| `ts` | string | |
| `device_id` | string | |
| `severity` | int | |
| `message` | string | |
| `_ingested_at` | timestamp | |
| `_source_topic` | string | |
| `_partition_date` | date | |

Partition spec: `days(_partition_date)`

---

#### `bronze.inventory`
Raw, append-only. Full history of all inventory snapshots.

| Column | Type | Notes |
|---|---|---|
| `device_id` | string | |
| `site_id` | string | |
| `vendor` | string | |
| `role` | string | |
| `_ingested_at` | timestamp | |
| `_partition_date` | date | |

---

#### `silver.interface_stats`
Validated, enriched. Primary analytical table. Schema evolution safe.

| Column | Type | Notes |
|---|---|---|
| `device_ts` | timestamp | parsed from source |
| `ingested_ts` | timestamp | broker receipt time |
| `device_id` | string | |
| `site_id` | string | joined from inventory |
| `vendor` | string | |
| `role` | string | |
| `interface_name` | string | |
| `effective_util_in` | double | 0.0 if oper_status IN (2,3) |
| `effective_util_out` | double | 0.0 if oper_status IN (2,3) |
| `raw_util_in` | double | original value preserved |
| `raw_util_out` | double | |
| `admin_status` | int | |
| `oper_status` | int | |
| `extra_cols` | map<string,string> | packet_loss etc. preserved here |
| `ingest_flags` | array<string> | flags set by ValidatorChain |
| `ingest_z_score` | double | streaming z-score at ingest |
| `ingest_iqr_score` | double | IQR-derived outlier score at ingest |
| `ingest_anomaly` | boolean | true if any ingest-time anomaly |
| `_partition_date` | date | |

Partition spec: `identity(site_id)`, `days(_partition_date)`
Sort order: `device_id`, `interface_name`, `device_ts`

---

#### `silver.syslogs`
Validated, enriched.

| Column | Type | Notes |
|---|---|---|
| `device_ts` | timestamp | |
| `ingested_ts` | timestamp | |
| `device_id` | string | |
| `site_id` | string | |
| `vendor` | string | |
| `role` | string | |
| `severity` | int | |
| `is_critical` | boolean | severity < 3 |
| `message` | string | |
| `_partition_date` | date | |

Partition spec: `identity(site_id)`, `days(_partition_date)`

---

#### `silver.anomaly_flags`
Written by streaming flatline detection (Stage 3b).

| Column | Type | Notes |
|---|---|---|
| `device_id` | string | |
| `site_id` | string | |
| `interface_name` | string | |
| `window_start` | timestamp | |
| `window_end` | timestamp | |
| `anomaly_type` | string | `FLATLINE` |
| `variance` | double | should be 0.0 |
| `mean_util_in` | double | |
| `oper_status` | int | must be 1 (UP) |
| `detected_at` | timestamp | |
| `_partition_date` | date | |

Partition spec: `identity(site_id)`, `days(_partition_date)`

---

#### `silver.dlq_quarantine`
Written by Stage 1 validator. Read by nightly DLQ reprocessor.

| Column | Type | Notes |
|---|---|---|
| `raw_payload` | string | original message, never mutated |
| `quarantine_reason` | string | `NULL_FIELD`, `NEGATIVE_UTIL`, `EXCEEDS_MAX`, `ORPHANED_DEVICE`, `INVALID_TIMESTAMP` |
| `source_topic` | string | |
| `quarantined_at` | timestamp | |
| `reprocessed` | boolean | default false |
| `reprocessed_at` | timestamp | nullable |
| `quarantine_resolution` | string | `PROMOTED_TO_SILVER`, `PERMANENT`, null |
| `_partition_date` | date | |

Partition spec: `days(_partition_date)`

---

### 5.2 ClickHouse Tables (Gold Layer + Parameters)

#### `gold_site_health_hourly`
Produced by ClickHouse materialized view reading `silver.interface_stats`.

| Column | Type | Engine notes |
|---|---|---|
| `site_id` | String | ORDER BY key |
| `window_start` | DateTime | ORDER BY key |
| `window_end` | DateTime | |
| `avg_util_in` | Float32 | |
| `avg_util_out` | Float32 | |
| `pct_interfaces_saturated` | Float32 | util > 80% |
| `critical_syslog_count` | UInt32 | severity < 3 |
| `total_interface_count` | UInt32 | |
| `health_score` | Float32 | 0–100, see formula |
| `is_degraded` | UInt8 | score < 40 |

Engine: `SummingMergeTree()` ORDER BY `(site_id, window_start)`
Partition: `toYYYYMMDD(window_start)`

---

#### `gold_anomaly_flags`
Appended by both streaming (FLATLINE) and batch (MODEL).

| Column | Type | Notes |
|---|---|---|
| `device_id` | String | |
| `site_id` | String | |
| `interface_name` | String | |
| `window_start` | DateTime | |
| `window_end` | DateTime | |
| `anomaly_type` | String | `FLATLINE` or `MODEL` |
| `anomaly_subtype` | String | `LOW_VARIANCE`, `FLAPPING`, `RAPID_DRIFT`, `MULTIVARIATE_ANOMALY` |
| `score` | Float32 | isolation score — lower = more anomalous |
| `mean_util_in` | Float32 | |
| `std_util_in` | Float32 | |
| `detected_at` | DateTime | |

Engine: `ReplacingMergeTree(detected_at)` ORDER BY `(device_id, interface_name, window_start)`
Partition: `toYYYYMMDD(window_start)`

---

#### `device_baseline_params`
Written by nightly batch jobs. Read by streaming job as broadcast join.

| Column | Type | Notes |
|---|---|---|
| `device_id` | String | |
| `interface_name` | String | |
| `hour_of_day` | UInt8 | 0–23, enables seasonal baseline |
| `day_of_week` | UInt8 | 0–6 |
| `baseline_mean` | Float32 | 30-day rolling mean |
| `baseline_std` | Float32 | 30-day rolling std |
| `iqr_k` | Float32 | IQR fence multiplier (typically 1.5) |
| `ewma_alpha` | Float32 | fit from autocorrelation |
| `isolation_score_threshold` | Float32 | 95th pct of normal scores |
| `distilled_rules` | String | JSON-encoded decision tree rules |
| `valid_from` | DateTime | |
| `valid_until` | DateTime | |

Engine: `ReplacingMergeTree(valid_from)` ORDER BY `(device_id, interface_name, hour_of_day, day_of_week)`

---

#### `anomaly_validation`
Written by nightly validation job comparing ingest-time vs batch decisions.

| Column | Type | Notes |
|---|---|---|
| `device_id` | String | |
| `interface_name` | String | |
| `window_start` | DateTime | |
| `ingest_flagged` | UInt8 | |
| `batch_flagged` | UInt8 | |
| `agreement` | String | `CONFIRMED`, `FALSE_POSITIVE`, `MISSED`, `BOTH_CLEAN` |
| `validated_at` | DateTime | |

Engine: `MergeTree()` ORDER BY `(device_id, interface_name, window_start)`

---

## 6. Repository Structure

```
network-health-poc/
│
├── README.md                          # ADR — architecture decision record
├── IMPLEMENTATION_PLAN.md             # this file
├── docker-compose.yml                 # all services
├── requirements.txt                   # Python dependencies
├── pyproject.toml                     # project metadata, linting config
├── main.py                            # entrypoint: --mode streaming|batch|...
│
├── src/
│   ├── __init__.py
│   │
│   ├── consumers/                     # pluggable source layer
│   │   ├── __init__.py
│   │   ├── base.py                    # SourceConsumer ABC
│   │   ├── csv_consumer.py            # polls CSV → Kafka
│   │   ├── jsonl_consumer.py          # polls JSONL → Kafka
│   │   ├── mqtt_consumer.py           # stub — MQTT swap-in
│   │   └── registry.py               # config-driven: "csv" → CsvConsumer
│   │
│   ├── validators/                    # validator chain
│   │   ├── __init__.py
│   │   ├── base.py                    # Validator ABC + ValidationResult
│   │   ├── rules.py                   # NullCheck, RangeCheck, OrphanCheck,
│   │   │                              # TimestampCheck, StatusCheck
│   │   └── chain.py                   # ValidatorChain — composes rules
│   │
│   ├── transforms/                    # pure functions, no Spark dependency
│   │   ├── __init__.py
│   │   ├── effective_util.py          # oper_status override
│   │   ├── health_score.py            # score formula
│   │   └── flatline.py                # Welford variance, flatline flag
│   │
│   ├── models/                        # anomaly detection
│   │   ├── __init__.py
│   │   ├── base.py                    # AnomalyDetector ABC + AnomalyResult
│   │   ├── flatline_detector.py       # streaming — Welford, sliding window
│   │   ├── isolation_forest.py        # batch — train, detect, distill
│   │   └── psi_drift_detector.py      # weekly — PSI drift, param update
│   │
│   ├── pipeline/                      # Spark jobs
│   │   ├── __init__.py
│   │   ├── streaming.py               # single Spark Structured Streaming app
│   │   │                              # stage1 → stage2 → stage3a + stage3b
│   │   ├── bronze_ingest.py           # Spark batch: Kafka → bronze Iceberg
│   │   ├── batch.py                   # SparkSQL window fns — Challenge 1
│   │   │                              # backfill bronze → silver
│   │   └── dlq_reprocessor.py         # reads DLQ → re-validates → silver
│   │
│   ├── storage/                       # storage abstraction
│   │   ├── __init__.py
│   │   ├── base.py                    # StorageWriter ABC
│   │   ├── iceberg_writer.py          # writes all Iceberg tables
│   │   └── clickhouse_writer.py       # writes anomaly flags + params
│   │
│   ├── catalog/                       # Iceberg REST catalog config
│   │   ├── __init__.py
│   │   └── rest_catalog.py            # catalog connection + table registry
│   │
│   ├── query/                         # query layer
│   │   ├── __init__.py
│   │   ├── duckdb_engine.py           # ad-hoc Iceberg queries
│   │   └── fastapi_bridge.py          # Grafana JSON datasource endpoint
│   │
│   ├── monitoring/                    # observability
│   │   ├── __init__.py
│   │   ├── metrics.py                 # Prometheus metric definitions
│   │   └── exporter.py                # HTTP /metrics endpoint
│   │
│   └── common/                        # shared utilities
│       ├── __init__.py
│       ├── config.py                  # centralised config (env vars, pydantic)
│       ├── logging.py                 # structured logging setup
│       └── spark_session.py           # SparkSession factory
│
├── sql/
│   ├── clickhouse/
│   │   ├── ddl/
│   │   │   ├── gold_site_health_hourly.sql
│   │   │   ├── gold_anomaly_flags.sql
│   │   │   ├── device_baseline_params.sql
│   │   │   └── anomaly_validation.sql
│   │   └── views/
│   │       ├── mv_site_health_hourly.sql    # materialized view Silver → Gold
│   │       └── mv_anomaly_summary.sql
│   │
│   └── iceberg/
│       └── ddl/
│           ├── bronze_interface_stats.sql
│           ├── bronze_syslogs.sql
│           ├── bronze_inventory.sql
│           ├── silver_interface_stats.sql
│           ├── silver_syslogs.sql
│           ├── silver_anomaly_flags.sql
│           └── silver_dlq_quarantine.sql
│
├── config/
│   ├── prometheus.yml
│   ├── alerting_rules.yml
│   └── grafana/
│       ├── datasources/
│       │   ├── clickhouse.yml
│       │   └── prometheus.yml
│       └── dashboards/
│           ├── pipeline.json          # Kafka lag, throughput, DLQ rate
│           ├── data_quality.json      # quarantine reasons, false positive rate
│           └── anomaly.json           # flatline events, model flags per site
│
└── data/
    ├── device_inventory.csv           # provided sample + dirty additions
    ├── interface_stats.csv            # provided sample + dirty additions
    └── syslogs.jsonl                  # provided sample + dirty additions
```

---

## 7. Design Patterns

### 7.1 Strategy Pattern — Source Consumers

Every data source implements `SourceConsumer` ABC. The pipeline never knows what
the source is — it only calls `poll()` and receives `RawRecord` objects.

```
SourceConsumer (ABC)
    ├── CsvConsumer        poll() tails CSV file rows
    ├── JsonlConsumer      poll() tails JSONL file lines
    └── MqttConsumer       poll() subscribes to MQTT broker topic (stub)

SourceRegistry             maps config string → consumer class
                           "csv"  → CsvConsumer
                           "jsonl" → JsonlConsumer
                           "mqtt" → MqttConsumer
```

Swap production: change one config line. Zero pipeline change.

---

### 7.2 Chain of Responsibility — Validators

Each `Validator` handles one concern and passes to the next. The chain is composable
and testable in isolation.

```
ValidatorChain
    ├── NullCheck          required fields must not be null
    ├── TimestampCheck     ts must parse as ISO8601, within ±24h of ingested_ts
    ├── RangeCheck         util_in, util_out must be in [0, 100]
    ├── StatusCheck        admin_status in (1,2), oper_status in (1,2,3)
    └── OrphanCheck        device_id must exist in bronze/inventory
```

Each validator returns `ValidationResult(ok: bool, reason: str | None)`.
Chain stops on first failure and routes to DLQ with the reason.
Validators have no side effects — pure functions on a record dict.

---

### 7.3 Abstract Base Class — Storage Writers

Storage backend is swappable at configuration time.

```
StorageWriter (ABC)
    ├── IcebergWriter      writes to Iceberg via REST Catalog + MinIO
    └── (future) DeltaWriter / S3ParquetWriter
```

Same write interface regardless of backend. ClickHouse writes are separate
(`ClickHouseWriter`) because they are always ClickHouse — no abstraction needed
for the PoC.

---

### 7.4 Abstract Base Class — Anomaly Detectors

```
AnomalyDetector (ABC)
    ├── FlatlineDetector         streaming · Welford · O(1) state
    └── IsolationForestDetector  batch · train() + detect() + distill()
    └── PSIDriftDetector         weekly · distribution comparison
```

All detectors produce `AnomalyResult` dataclass. All write to the same
`gold_anomaly_flags` ClickHouse table with different `anomaly_type` values.

---

### 7.5 Factory Pattern — SparkSession

One factory creates SparkSession with the correct config for each mode.
No SparkSession construction scattered across files.

```python
SparkSessionFactory.create(mode="streaming")  # includes Iceberg + Kafka jars
SparkSessionFactory.create(mode="batch")      # includes Iceberg jars only
```

---

### 7.6 Configuration — Pydantic Settings

All configuration via environment variables. No hardcoded values anywhere.
`src/common/config.py` defines a `Settings` Pydantic model — validated at startup.

```
KAFKA_BOOTSTRAP_SERVERS
ICEBERG_REST_CATALOG_URL
MINIO_ENDPOINT
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
CLICKHOUSE_HOST
CLICKHOUSE_PORT
PROMETHEUS_PORT
```

---

## 8. Component Specifications

### 8.1 Source Consumers

**`SourceConsumer` ABC:**
- `poll(batch_size: int) -> Iterator[RawRecord]`
- `source_type() -> str`
- `topic() -> str` — Kafka topic to produce to

**`RawRecord` dataclass:**
```python
@dataclass
class RawRecord:
    payload: dict[str, Any]     # raw parsed record
    source_type: str            # "csv", "jsonl", "mqtt"
    received_at: datetime       # consumer receipt time
    raw_string: str             # original string for DLQ preservation
```

**`CsvConsumer`:**
- Reads CSV with `pandas.read_csv` using `chunksize` for memory efficiency
- Required columns validated on first read — extra columns passed through
- Tracks last read offset (line number) — stateful across poll() calls
- Produces to `raw.interface-stats` or `raw.inventory` based on config

**`JsonlConsumer`:**
- Reads JSONL line by line using file seek position
- Each line parsed as JSON — parse error → skip + log, never crash
- Produces to `raw.syslogs`

**`MqttConsumer` (stub):**
- Implements ABC with `NotImplementedError` on `poll()`
- Documents the MQTT topic subscription pattern for production
- Zero changes to pipeline when implemented

---

### 8.2 Validators

**`NullCheck`:**
- Required fields: `ts`, `device_id` for all sources
- Additional for interface_stats: `interface_name`, `util_in`, `util_out`, `oper_status`
- Failure reason: `NULL_REQUIRED_FIELD:{field_name}`

**`TimestampCheck`:**
- `ts` must parse as ISO8601
- `|parsed_ts - ingested_ts| < 24h` — catches stale replays
- Failure reasons: `INVALID_TIMESTAMP`, `STALE_TIMESTAMP`, `FUTURE_TIMESTAMP`

**`RangeCheck`:**
- `util_in < 0` → `NEGATIVE_UTILIZATION`
- `util_out < 0` → `NEGATIVE_UTILIZATION`
- `util_in > 100` → `EXCEEDS_MAX_UTILIZATION` (firmware bug per spec)
- `util_out > 100` → `EXCEEDS_MAX_UTILIZATION`
- `severity not in range(0, 8)` → `INVALID_SEVERITY`
- Note: `util > 100` goes to DLQ per spec — not silently clamped

**`StatusCheck`:**
- `admin_status not in (1, 2)` → `INVALID_ADMIN_STATUS`
- `oper_status not in (1, 2, 3)` → `INVALID_OPER_STATUS`

**`OrphanCheck`:**
- `device_id` must exist in latest bronze/inventory snapshot
- Snapshot loaded at pipeline startup as broadcast variable — refreshed every 10 minutes
- Failure reason: `ORPHANED_DEVICE:{device_id}`
- Note: orphaned records are the primary fixable DLQ case — device may appear in inventory later

---

### 8.3 Spark Streaming Pipeline

**Single app entry: `pipeline/streaming.py`**

Runs as one long-lived Spark Structured Streaming application.
All stages share one `SparkSession`.

**Stage 1 — Ingest + Validate:**
```
readStream from Kafka (raw.interface-stats, raw.syslogs)
    → parse JSON payload
    → apply ValidatorChain
    → split: clean stream | dlq stream
    → dlq stream → writeStream to silver.dlq_quarantine (Iceberg)
    → clean stream → enrich with baseline params (ClickHouse broadcast)
```

Schema evolution: `from_json` with `PERMISSIVE` mode — unknown columns
land in `_corrupt_record`, then extracted into `extra_cols` map.

**Stage 2 — Enrich:**
```
clean stream
    → stream-static join with latest bronze/inventory on device_id
    → attach site_id, vendor, role
    → apply effective_util transform
    → attach ingest_z_score, ingest_iqr_score from baseline params
    → writeStream to silver.interface_stats (Iceberg)
    → writeStream to silver.syslogs (Iceberg)
```

Trigger: `processingTime("30 seconds")` — micro-batch.
Checkpoint: MinIO `s3a://lakehouse/checkpoints/streaming/`

**Stage 3a — Health Score:**
```
enriched interface-stats stream + enriched syslogs stream
    → watermark: device_ts 30 seconds
    → tumbling window: 1 hour per site_id
    → join on site_id + window
    → compute health_score (see formula section)
    → writeStream trigger once per window close
    → silver.site_health_hourly (Iceberg) → triggers ClickHouse MV
```

**Stage 3b — Flatline Detection:**
```
enriched interface-stats stream
    → filter: oper_status == 1 (UP only)
    → watermark: device_ts 10 minutes
    → sliding window: 4 hours, slide 5 minutes, per device_id + interface_name
    → FlatlineDetector.detect_spark(window_df)
    → variance == 0 → AnomalyResult(type=FLATLINE)
    → writeStream to silver.anomaly_flags (Iceberg)
    → produce to output.anomaly-alerts (Kafka) for Prometheus
```

---

### 8.4 Spark Batch Jobs

**`pipeline/bronze_ingest.py` — BronzeIngestJob:**
- Reads Kafka raw topics using `spark.read.format("kafka")` with offset tracking
- Writes raw payloads to bronze/ Iceberg tables — no transformation
- Adds `_ingested_at`, `_source_topic` metadata columns
- Runs on schedule: every 15 minutes
- Idempotent: Iceberg ACID prevents duplicate writes on rerun

**`pipeline/batch.py` — BackfillJob (Challenge 1):**
- Reads `bronze.interface_stats` with `site_id` + date range filter
- Applies full silver transformation pipeline
- SparkSQL window functions for health score aggregation:

```sql
SELECT
    site_id,
    window_start,
    avg(effective_util_in)                                     AS avg_util_in,
    count(*) FILTER (WHERE effective_util_in > 80)
        / count(*)                                             AS pct_saturated,
    sum(critical_syslog_count)                                 AS critical_syslogs,
    100
      - (pct_saturated * 40)
      - (LEAST(critical_syslogs, 4) * 10)
      - (pct_interfaces_down * 20)                            AS health_score
FROM (
    SELECT
        site_id,
        date_trunc('hour', device_ts)                         AS window_start,
        effective_util_in,
        oper_status,
        COUNT(*) OVER (
            PARTITION BY site_id, date_trunc('hour', device_ts)
        )                                                      AS total_ifaces
    FROM silver_interface_stats
    WHERE site_id = :site_id
      AND _partition_date BETWEEN :start_date AND :end_date
) sub
GROUP BY site_id, window_start
```

Partition pruning: `site_id` identity partition + date range on `_partition_date`
ensures Iceberg scans only relevant files — no full table scan.

**`pipeline/dlq_reprocessor.py` — DLQReprocessor:**
- Reads `silver.dlq_quarantine` where `reprocessed = false`
- Classifies each record:
  - `ORPHANED_DEVICE` → check if device_id now exists in bronze/inventory → fixable
  - `NEGATIVE_UTIL`, `EXCEEDS_MAX`, `NULL_FIELD` → permanent
- Fixable records: re-run ValidatorChain → write to silver → mark `reprocessed=true`, `resolution=PROMOTED_TO_SILVER`
- Permanent records: mark `reprocessed=true`, `resolution=PERMANENT`

---

### 8.5 Storage Layer

**`IcebergWriter`:**
- Connects to REST Catalog (URL from config)
- Namespace: `bronze`, `silver`
- Uses `pyiceberg` for schema registration and table creation
- Spark writes via `df.writeTo("catalog.namespace.table").append()`
- Schema evolution: `mergeSchema=true` — new columns automatically added to table schema

**REST Catalog connection:**
```python
catalog = load_catalog(
    "rest",
    **{
        "uri": settings.ICEBERG_REST_CATALOG_URL,
        "s3.endpoint": settings.MINIO_ENDPOINT,
        "s3.access-key-id": settings.MINIO_ACCESS_KEY,
        "s3.secret-access-key": settings.MINIO_SECRET_KEY,
    }
)
```

**`ClickHouseWriter`:**
- Uses `clickhouse-connect` Python client
- Writes `gold_anomaly_flags` rows via `client.insert()`
- Writes `device_baseline_params` updates via `client.command()` (ReplacingMergeTree upsert)
- Connection pooled — one client instance per batch job run

---

### 8.6 Query & Analytics Layer

**ClickHouse materialized views (Silver → Gold):**

`mv_site_health_hourly.sql`:
```sql
CREATE MATERIALIZED VIEW mv_site_health_hourly
TO gold_site_health_hourly
AS SELECT
    site_id,
    toStartOfHour(device_ts)             AS window_start,
    dateAdd(hour, 1, window_start)       AS window_end,
    avg(effective_util_in)               AS avg_util_in,
    avg(effective_util_out)              AS avg_util_out,
    countIf(effective_util_in > 80)
        / count()                        AS pct_interfaces_saturated,
    countIf(severity < 3)               AS critical_syslog_count,
    count()                              AS total_interface_count
FROM silver_interface_stats  -- Iceberg table engine
GROUP BY site_id, window_start
```

Health score and `is_degraded` computed as ClickHouse calculated columns.

**DuckDB engine:**
```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL iceberg; LOAD iceberg;")
conn.execute(f"""
    CREATE OR REPLACE VIEW silver_interface_stats AS
    SELECT * FROM iceberg_scan('{catalog_path}/silver/interface_stats')
""")
```

**FastAPI bridge:**
- Single endpoint: `POST /query` — accepts SQL, returns JSON
- Grafana JSON datasource plugin queries this endpoint
- Used for ad-hoc exploration and custom dashboard queries not covered by ClickHouse

---

### 8.7 Anomaly Detection

See [Section 9](#9-anomaly-detection-strategy) for full detail.

**`FlatlineDetector` (streaming):**
- Welford's online algorithm for running mean and variance
- Implemented as a Spark pandas UDF applied over sliding window group
- `variance == 0.0 AND oper_status == 1` → FLATLINE
- `variance < epsilon AND oper_status == 1` → NEAR_FLATLINE

**`IsolationForestDetector` (batch):**
- 10-feature vector per device+interface 4h window
- `get_training_data()`: returns only **confirmed-normal** Silver records —
  `ingest_anomaly = false` AND not present in previous batch anomaly flags.
  The model never sees anomalous records during training. Contaminated data
  cannot degrade the model.
- `train()`: fits on 30 days of confirmed-normal Silver, saves model + scaler to MinIO
- `validate()`: scores candidate model on a held-out validation window, computes
  precision and recall vs current production model
- `promote()`: candidate replaces production only if precision ≥ current AND
  recall ≥ current — otherwise current model is retained and an alert is raised
- `detect()`: loads production model, scores last 24h of Silver, classifies subtype
- `distill()`: fits `DecisionTreeClassifier(max_depth=4)` on forest predictions
  → exports rules as JSON → writes to `device_baseline_params.distilled_rules`
- Streaming job loads distilled rules and applies them as additional ingest-time checks

**Why online learning was rejected:**
Online learning approaches (Half-Space Trees, RRCF) were considered and explicitly
rejected. The core failure: online models consume each incoming record as a training
update. In infrastructure monitoring, anomalous records — flatlines, spikes, DoS
traffic — are consumed and the model drifts toward treating failures as normal.
The worse the network gets, the blinder the detector becomes. This is concept
contamination, not concept drift. Without labels, online learning cannot distinguish
between the two. Production systems at Cloudflare, Netflix, and Datadog use
controlled periodic retraining on validated-normal data for exactly this reason.

**`PSIDriftDetector` (weekly):**
- Computes PSI per device, per metric vs 30-day baseline
- PSI > 0.25 → recompute baseline params
- Updates `device_baseline_params` in ClickHouse
- Streaming job auto-adapts on next micro-batch startup

---

### 8.8 Monitoring & Observability

**Prometheus metrics (exported from `monitoring/exporter.py`):**

```
records_ingested_total{source, topic}          Counter
records_validated_total{result}                Counter  # result: clean|dlq
dlq_records_total{reason}                      Counter
kafka_consumer_lag{topic, partition}           Gauge
flatline_events_total{site_id}                 Counter
model_anomaly_flags_total{subtype}             Counter
model_retrain_promoted_total                   Counter
model_retrain_rejected_total                   Counter  # candidate underperformed
pipeline_latency_seconds{stage}                Histogram
site_health_score{site_id}                     Gauge    # latest score
```

**Grafana dashboards:**

1. **Pipeline dashboard** — Kafka consumer lag, records/sec, DLQ rate over time, pipeline latency per stage
2. **Data quality dashboard** — DLQ breakdown by reason, orphan rate, false positive rate (ingest vs batch agreement), DLQ reprocessing success rate
3. **Anomaly dashboard** — flatline events per site, model anomaly flags per subtype, site health score heatmap, anomaly validation agreement rate

**Alertmanager rules:**
- DLQ rate > 5% of total ingested → `WARNING`
- Site health score < 40 → `WARNING` per site
- Flatline event detected → `CRITICAL` (operational emergency)
- Kafka consumer lag > 10,000 records → `WARNING`
- No records ingested in 5 minutes from any topic → `CRITICAL`

---

## 9. Anomaly Detection Strategy

### 9.1 Ingest-Time Detection

Applied in Stage 1 and Stage 2 of the streaming pipeline.

| Method | What it catches | Implementation |
|---|---|---|
| Deterministic rules | Invalid data (negative util, bad status codes) | ValidatorChain → DLQ |
| Threshold alerting | Operational breaches (util > 80%, severity < 3) | DataFrame filter |
| Flatline via Welford | Variance = 0 over 4h, oper_status UP | FlatlineDetector, sliding window |
| Z-score | Sudden spike/drop vs device baseline | Computed using baseline_mean + baseline_std from ClickHouse |
| IQR | Point outliers vs device's own recent distribution  | approx_percentile over rolling 4h window. |
| Distilled decision tree rules | Multivariate patterns learned by batch model | Rules loaded from device_baseline_params.distilled_rules |

All ingest-time anomaly flags written as annotations into silver records
(`ingest_flags`, `ingest_z_score`, `ingest_iqr_score`, `ingest_anomaly`).

### 9.2 Post-Processing Detection

Applied in nightly and weekly batch jobs.

| Method | What it catches | Frequency |
|---|---|---|
| Isolation Forest | Correlated failures, flapping, multivariate anomalies | Nightly inference; weekly retrain on confirmed-normal data only |
| STL decomposition | Seasonal anomalies (3am spike, off-pattern traffic) | Nightly |
| PSI drift detection | Sensor calibration drift, device replacement | Weekly |
| Anomaly validation | False positive / miss rate between ingest and batch | Nightly |

**Retraining discipline — confirmed-normal dataset only:**

The Isolation Forest retrains exclusively on Silver records where:
- `ingest_anomaly = false` — not flagged at ingest time
- Record not present in previous batch `gold_anomaly_flags` — not flagged by prior model

This produces a confirmed-normal corpus. Anomalous records are structurally
excluded from training. The model cannot drift toward treating failures as normal
regardless of how degraded the network becomes.

**Gated promotion — candidate vs current:**

Before the weekly retrain promotes a new model to production:
1. Candidate model scored on a held-out validation window (most recent 7 days of confirmed-normal Silver)
2. Precision and recall computed vs current production model on the same window
3. Candidate promoted only if it matches or improves both metrics
4. If candidate underperforms: current model retained, `ANOMALY_MODEL_RETRAIN_REJECTED` metric incremented, engineer alerted
5. Genuine concept drift (PSI > 0.25) requires human review before retraining on the new distribution

### 9.3 Feedback Loop

```
Post-processing → Streaming:
    PSI detects drift → recomputes baseline_mean, baseline_std, iqr_k
    IsolationForest trained on confirmed-normal Silver → distills decision tree rules
    Both written to device_baseline_params (ClickHouse)
    Streaming reads on next micro-batch startup → auto-adapts

Streaming → Post-processing:
    ingest_flags, ingest_z_score written to silver records as annotations
    IsolationForest excludes ingest_anomaly=true records from training corpus
    IsolationForest trains on what both tiers agreed was normal — not on failures
    Learns precursor patterns: feature vectors that precede ingest flags by 30–60 min

Validation loop:
    Nightly: compare ingest_anomaly vs batch flags per record
    Rising false positive rate → tighten thresholds
    Rising miss rate → trigger immediate baseline recompute
    Agreement rate exposed as Grafana metric (pipeline self-health)

Retraining gate:
    Weekly candidate model validated on held-out window before promotion
    Candidate underperforms → current model retained, alert raised
    PSI > 0.25 (genuine concept drift) → human review required before retraining
    Model version + promotion decision logged to MinIO for audit trail
```

---

## 10. Schema Evolution Strategy

The assignment states `interface_stats` may add a `packet_loss` column next week.

**Three-layer defense:**

**Layer 1 — Consumer (read time):**
`CsvConsumer` validates only `REQUIRED_COLUMNS`. Extra columns read into a dict.
No `KeyError` on unknown columns. Extra columns logged.

**Layer 2 — Spark (parse time):**
`from_json` with `PERMISSIVE` mode. Schema inferred or defined with extra
columns landing in `extra_cols` map via:
```python
df = df.withColumn(
    "extra_cols",
    map_from_entries(
        array([
            struct(lit(c), col(c).cast("string"))
            for c in extra_columns
        ])
    )
)
```

**Layer 3 — Iceberg (write time):**
`mergeSchema = true` on all Iceberg writes. When `packet_loss` appears in a
micro-batch, Iceberg automatically adds the column to the table schema.
Old micro-batches read `packet_loss` as `null`. No breaking change.
No manual DDL. No pipeline restart required.

**ADR statement:** Schema evolution is handled end-to-end without pipeline
changes. A new column flows from source through consumer, Spark, and Iceberg
with zero engineer intervention. The REST Catalog records the schema version
transition with a snapshot, enabling time-travel queries to the pre-evolution
state.

---

## 11. Kafka Topic Design

| Topic | Partitions | Retention | Key | Notes |
|---|---|---|---|---|
| `raw.interface-stats` | 6 | 7 days | `device_id` | Keyed to preserve per-device order |
| `raw.syslogs` | 6 | 7 days | `device_id` | |
| `raw.inventory` | 1 | Forever | `device_id` | Compacted — always latest device state |
| `output.anomaly-alerts` | 3 | 7 days | `site_id` | Prometheus consumer group |

Partition count 6 = expected parallelism for 50GB/day at PoC scale.
Keyed by `device_id` ensures all messages for a device go to the same partition
— preserves ordering for IQR and Welford stateful computation.

---

## 12. Iceberg Catalog & Table Design

**REST Catalog:**
- Implementation: Polaris (Apache) or simple REST catalog server
- `uri`: `http://iceberg-rest:8181`
- Backend: MinIO at `http://minio:9000`
- Namespace: `bronze`, `silver`

**Why REST Catalog over Hive Metastore:**
- Engine-agnostic — Spark, DuckDB, Trino, ClickHouse all connect via same REST API
- No Thrift server dependency
- Simpler Docker Compose setup
- Production-ready: swap to AWS Glue or Polaris cloud without client changes

**Partition strategy for `silver.interface_stats`:**
```
Partition spec:
    identity(site_id)           → enables site_id partition pruning
    days(_partition_date)       → enables date range pruning

Sort order within partition:
    device_id ASC
    interface_name ASC
    device_ts ASC
```

For a query `WHERE site_id = 'SITE-A' AND date BETWEEN '2025-01-01' AND '2025-01-07'`:
- Iceberg prunes all non-SITE-A partitions → reads only 1/N site files
- Further prunes to 7 date partitions → reads 7 * (1/N) of total files
- Sorted order enables column statistics skip within files

---

## 13. ClickHouse Table & Materialized View Design

**Why ClickHouse for Gold:**
- Native Iceberg table engine — reads silver directly, no ETL duplication
- Materialized views increment automatically on each new Silver micro-batch
- Gold is always fresh (minutes behind Silver, not hours)
- Grafana ClickHouse plugin — direct sub-second dashboard queries
- `SummingMergeTree` and `ReplacingMergeTree` give idempotent batch-safe writes

**Materialized view pattern:**
```
silver_interface_stats (Iceberg engine — reads from MinIO via REST Catalog)
    │
    └──▶ mv_site_health_hourly (MATERIALIZED VIEW)
             │ increments on INSERT into silver_interface_stats
             ▼
         gold_site_health_hourly (SummingMergeTree)
```

Adding a new Gold metric = one new materialized view SQL file.
Zero Spark job changes. Zero pipeline restarts.

---

## 14. Health Score Formula

```
health_score = 100
    - (pct_interfaces_saturated × 40)     # saturation penalty, max 40pts
    - (LEAST(critical_syslog_count, 4) × 10)  # syslog penalty, max 40pts
    - (pct_interfaces_down × 20)           # availability penalty, max 20pts

Clamped to [0, 100].

is_degraded = health_score < 40
```

**Definitions:**
- `pct_interfaces_saturated`: fraction of interface readings where `effective_util_in > 80` OR `effective_util_out > 80` in the hour window
- `critical_syslog_count`: count of syslog records where `severity < 3` (EMERGENCY=0, ALERT=1, CRITICAL=2) in the hour window, capped at 4 to prevent one noisy device from dominating
- `pct_interfaces_down`: fraction of interface readings where `oper_status != 1` in the hour window

**Rationale:**
- Bandwidth saturation is the primary operational concern — highest weight (40)
- Critical syslogs indicate known failure conditions — medium weight (up to 40)
- Interface downtime directly impacts availability — lower weight (20) because effective_util already zeroes out DOWN interfaces, avoiding double-penalisation

---

## 15. DLQ Reprocessing Logic

**Classification rules:**

| quarantine_reason | Fixable? | Condition for fixable | Resolution |
|---|---|---|---|
| `ORPHANED_DEVICE` | Yes | device_id now in bronze/inventory | Re-validate → silver |
| `NEGATIVE_UTILIZATION` | No | — | PERMANENT |
| `EXCEEDS_MAX_UTILIZATION` | No | firmware bug, value invalid | PERMANENT |
| `NULL_REQUIRED_FIELD` | No | — | PERMANENT |
| `INVALID_TIMESTAMP` | No | — | PERMANENT |
| `STALE_TIMESTAMP` | Maybe | if within acceptable replay window | Re-evaluate |
| `FUTURE_TIMESTAMP` | No | — | PERMANENT |

**Process:**
1. Read `silver.dlq_quarantine` where `reprocessed = false`, ordered by `quarantined_at`
2. For each record: parse `raw_payload`, apply classification rules
3. Fixable: deserialize → run full ValidatorChain → if now passes → write to silver → mark done
4. Permanent: mark `reprocessed=true`, `resolution=PERMANENT`, log for human review
5. All updates to `dlq_quarantine` via Iceberg `MERGE INTO` — atomic, idempotent

---

## 16. Infrastructure — Docker Compose Services

| Service | Image | Purpose |
|---|---|---|
| `kafka` | `confluentinc/cp-kafka` | Message broker |
| `zookeeper` | `confluentinc/cp-zookeeper` | Kafka coordination |
| `minio` | `minio/minio` | On-prem S3 — Iceberg object store |
| `iceberg-rest` | `tabulario/iceberg-rest` | REST Catalog server |
| `clickhouse` | `clickhouse/clickhouse-server` | Gold layer + analytics |
| `prometheus` | `prom/prometheus` | Metrics collection |
| `grafana` | `grafana/grafana` | Dashboards |
| `alertmanager` | `prom/alertmanager` | Alert routing |
| `spark-master` | `bitnami/spark` | Spark master |
| `spark-worker` | `bitnami/spark` | Spark worker(s) |

All services on a single Docker network `network-health-net`.
MinIO bucket `lakehouse` created on startup via `mc` init container.
Iceberg namespaces `bronze`, `silver` created on startup via REST API call.
ClickHouse DDL applied on startup via init SQL files mounted at `/docker-entrypoint-initdb.d/`.

---

## 17. Production Upgrade Path

| PoC component | Production swap-in | Swap effort |
|---|---|---|
| CsvConsumer / JsonlConsumer | MqttConsumer | One class, same ABC |
| Parquet / Iceberg REST Catalog | Polaris cloud / AWS Glue | Change catalog URI |
| MinIO | AWS S3 / on-prem Ceph | Change endpoint config |
| Simple REST Catalog | Apache Polaris (full) | Same REST API |
| FlatlineDetector | ONNX anomaly model served inline | Swap AnomalyDetector implementation |
| IsolationForest distilled rules | ONNX model served inline | Swap distill() output format |
| Device params in ClickHouse | ScyllaDB feature store | Sub-ms latency for embeddings |
| Spark Structured Streaming | Apache Flink | If sub-second latency required; Silver schema unchanged |
| Docker Compose | Kubernetes + Helm | Same images, different orchestration |

---

## 18. File-Level Responsibility Map

| File | Owns | Depends on |
|---|---|---|
| `main.py` | CLI entrypoint, mode dispatch | all pipeline modules |
| `common/config.py` | All env var config, Pydantic Settings | nothing |
| `common/spark_session.py` | SparkSession factory per mode | config |
| `common/logging.py` | Structured JSON logging setup | nothing |
| `consumers/base.py` | SourceConsumer ABC, RawRecord | nothing |
| `consumers/csv_consumer.py` | CSV file tail → Kafka | base, config |
| `consumers/jsonl_consumer.py` | JSONL file tail → Kafka | base, config |
| `consumers/mqtt_consumer.py` | MQTT stub | base |
| `consumers/registry.py` | Map config string → consumer class | all consumers |
| `validators/base.py` | Validator ABC, ValidationResult | nothing |
| `validators/rules.py` | All five validator implementations | base |
| `validators/chain.py` | ValidatorChain composition | base, rules |
| `transforms/effective_util.py` | oper_status override — pure function | nothing |
| `transforms/health_score.py` | Score formula — pure function | nothing |
| `transforms/flatline.py` | Welford variance — pure function | nothing |
| `models/base.py` | AnomalyDetector ABC, AnomalyResult | nothing |
| `models/flatline_detector.py` | Streaming flatline, Spark UDF | base, transforms/flatline |
| `models/isolation_forest.py` | Batch train on confirmed-normal data + gated promotion + detect + distill | base, storage/clickhouse_writer |
| `models/psi_drift_detector.py` | Weekly PSI, param update | base, storage/clickhouse_writer |
| `pipeline/streaming.py` | Main streaming app, all 3 stages | validators, transforms, models, storage |
| `pipeline/bronze_ingest.py` | Spark batch: Kafka → bronze Iceberg | storage/iceberg_writer, catalog |
| `pipeline/batch.py` | SparkSQL backfill, window fns | storage, catalog |
| `pipeline/dlq_reprocessor.py` | DLQ classify → promote → mark | validators, storage |
| `storage/base.py` | StorageWriter ABC | nothing |
| `storage/iceberg_writer.py` | All Iceberg table writes | base, catalog |
| `storage/clickhouse_writer.py` | ClickHouse inserts | config |
| `catalog/rest_catalog.py` | Iceberg REST Catalog connection | config |
| `query/duckdb_engine.py` | DuckDB → Iceberg queries | config |
| `query/fastapi_bridge.py` | HTTP API wrapping DuckDB | duckdb_engine |
| `monitoring/metrics.py` | All Prometheus metric definitions | nothing |
| `monitoring/exporter.py` | HTTP /metrics endpoint | metrics |

---

## 19. Build Order

Write code in this exact sequence. Each layer depends only on layers above it.

```
1. common/config.py                   # foundation — all config in one place
2. common/logging.py                  # structured logging
3. common/spark_session.py            # SparkSession factory

4. consumers/base.py                  # RawRecord + SourceConsumer ABC
5. consumers/csv_consumer.py
6. consumers/jsonl_consumer.py
7. consumers/mqtt_consumer.py         # stub only
8. consumers/registry.py

9. validators/base.py                 # Validator ABC + ValidationResult
10. validators/rules.py               # all five rules
11. validators/chain.py               # ValidatorChain

12. transforms/effective_util.py      # pure functions — no deps
13. transforms/health_score.py
14. transforms/flatline.py            # Welford algorithm

15. models/base.py                    # AnomalyDetector ABC + AnomalyResult
16. models/flatline_detector.py       # streaming flatline
17. models/isolation_forest.py        # batch model + distillation
18. models/psi_drift_detector.py      # drift detection

19. catalog/rest_catalog.py           # Iceberg REST Catalog connection
20. storage/base.py                   # StorageWriter ABC
21. storage/iceberg_writer.py         # Iceberg writes
22. storage/clickhouse_writer.py      # ClickHouse writes

23. pipeline/bronze_ingest.py         # Kafka → bronze (Spark batch)
24. pipeline/streaming.py             # main streaming app
25. pipeline/batch.py                 # backfill + SparkSQL window fns
26. pipeline/dlq_reprocessor.py       # DLQ reprocessing

27. query/duckdb_engine.py
28. query/fastapi_bridge.py

29. monitoring/metrics.py
30. monitoring/exporter.py

31. sql/iceberg/ddl/*.sql             # table definitions
32. sql/clickhouse/ddl/*.sql          # Gold table DDL
33. sql/clickhouse/views/*.sql        # materialized views

34. config/prometheus.yml
35. config/alerting_rules.yml
36. config/grafana/datasources/*.yml
37. config/grafana/dashboards/*.json

38. docker-compose.yml                # wire everything together
39. main.py                           # CLI entrypoint
40. README.md                         # ADR
```

---

*End of implementation plan. All decisions above are final and agreed.
Writing code follows this document exactly.*
