# 📊 Network Health Platform - Service Endpoints & Health Checks

## Overview
All services are running on `localhost` with the following ports and health check procedures.

---

## 🔷 **Data Ingestion Layer**

### Kafka (Message Broker)
- **URL**: `localhost:9092` (external), `kafka:29092` (internal)
- **UI**: http://localhost:8082
- **Health Check**:
  ```bash
  # Check running topics
  docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list
  
  # Check message count
  docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server localhost:29092 --topic raw.interface.stats
  ```
- **What to Monitor**:
  - ✅ Topic `raw.interface.stats` - Interface statistics (27k+ messages)
  - ✅ Topic `raw.syslogs` - System logs (26k+ messages)
  - ✅ Topic `raw.inventory` - Device inventory (180+ messages)
  - ✅ Topic `output.anomaly.alerts` - Anomaly outputs

---

## 🔷 **Storage Layer**

### MinIO (S3-Compatible Object Store)
- **S3 API**: http://localhost:9000
- **Console UI**: http://localhost:9001
- **Credentials**: `minioadmin` / `minioadmin123`
- **Health Check**:
  ```bash
  # Check MinIO is accessible
  curl -f http://localhost:9001/minio/health/live
  
  # List buckets (requires mc CLI)
  docker exec minio mc ls local/
  ```
- **What to Monitor**:
  - ✅ Bucket `lakehouse/` - Main data lake
  - ✅ Subdirectory `lakehouse/bronze/` - Bronze layer data
  - ✅ Subdirectory `lakehouse/silver/` - Silver layer data
  - ✅ Subdirectory `lakehouse/checkpoints/` - Spark checkpoints
  - ✅ Subdirectory `lakehouse/models/` - ML models

### Iceberg REST Catalog
- **URL**: http://localhost:8181
- **API Endpoint**: http://localhost:8181/v1
- **Health Check**:
  ```bash
  # Check catalog config
  curl http://localhost:8181/v1/config
  
  # List namespaces
  curl http://localhost:8181/v1/namespaces
  
  # Check bronze tables
  curl http://localhost:8181/v1/namespaces/bronze/tables
  ```
- **What to Monitor**:
  - ✅ Namespaces: `bronze`, `silver`
  - ✅ Bronze tables: `interface_stats`, `syslogs`, `inventory`
  - ✅ Silver tables: `interface_stats_src`, `syslogs_src`, `anomaly_flags`

---

## 🔷 **Analytics & Querying Layer**

### ClickHouse (OLAP Database)
- **HTTP API**: http://localhost:8123
- **Native Protocol**: `clickhouse:9009` (internal)
- **Health Check**:
  ```bash
  # Check if ClickHouse is responding
  curl http://localhost:8123/ping
  
  # Query via clickhouse-client
  docker exec -it clickhouse clickhouse-client -q "SELECT 1"
  ```
- **Key Databases**:
  - `network_health` - Main analytics database
- **What to Monitor**:
  - ✅ **Bronze Layer**: Tables created via Iceberg integration
  - ✅ **Silver Layer**:
    - `silver_interface_stats_src` - Interface statistics
    - `silver_syslogs_src` - System logs
    - `silver_anomaly_flags` - Detected anomalies
  - ✅ **Gold Layer**:
    - `gold_site_health_hourly` - Hourly aggregations
    - `gold_anomaly_flags` - Final anomaly flags

### Query API
- **URL**: http://localhost:8000
- **Health Check**:
  ```bash
  # Check API health
  curl http://localhost:8000/health
  
  # Check metrics
  curl http://localhost:8000/metrics | grep anomaly
  ```
- **What to Monitor**:
  - ✅ API response time
  - ✅ Anomaly count metrics
  - ✅ Pipeline health metrics

---

## 🔷 **Observability & Monitoring**

### Prometheus (Metrics Scraper)
- **URL**: http://localhost:9090
- **Health Check**:
  ```bash
  # Check Prometheus is ready
  curl http://localhost:9090/-/ready
  ```
- **What to Monitor**:
  - ✅ Query execution times
  - ✅ Error rates from pipeline components
  - ✅ Data ingestion rates
  - ✅ Storage usage

### Grafana (Visualization & Dashboards)
- **URL**: http://localhost:3000
- **Credentials**: `admin` / `admin`
- **Health Check**:
  ```bash
  # Check Grafana API
  curl http://localhost:3000/api/health
  ```
- **Dashboards**:
  - ✅ **Pipeline Dashboard** - Data flow visualization
  - ✅ **Anomaly Detection** - Anomaly metrics
  - ✅ **ClickHouse Metrics** - Database performance
  - ✅ **Kafka Metrics** - Message throughput
- **What to Monitor**:
  - ✅ Data flow through each layer (Bronze → Silver → Gold)
  - ✅ Anomaly counts and types
  - ✅ Pipeline latency
  - ✅ Storage usage trends

---

## 🔷 **Orchestration Layer**

### Airflow (Workflow Orchestration)
- **URL**: http://localhost:8085
- **Credentials**: `admin` / `admin`
- **Health Check**:
  ```bash
  # Check Airflow health
  curl http://localhost:8085/health
  ```
- **What to Monitor**:
  - ✅ DAG status (enabled/disabled)
  - ✅ Task execution history
  - ✅ Failed task logs
  - ✅ Batch job scheduling (anomaly detection models)

### PostgreSQL (Airflow Metadata)
- **Connection**: `postgres:5432` (internal)
- **Database**: `airflow`
- **Credentials**: `airflow` / `airflow`
- **Health Check**:
  ```bash
  docker exec postgres pg_isready -U airflow
  ```

---

## 🔷 **Compute Layer**

### Spark Master
- **UI**: http://localhost:8080
- **Submit Endpoint**: `spark-master:7077` (internal)
- **Health Check**:
  ```bash
  # Check Spark master is responding
  curl http://localhost:8080/
  ```
- **What to Monitor**:
  - ✅ Active applications
  - ✅ Executor health
  - ✅ Memory usage
  - ✅ Job completion rates

### Spark Worker
- **UI**: http://localhost:8081
- **Health Check**:
  ```bash
  # Check worker is healthy
  curl http://localhost:8081/
  ```

---

## 📋 **Quick Health Check Script**

```bash
#!/bin/bash
echo "=== Network Health Platform - Service Status ==="
echo ""

# Kafka
echo "✓ Kafka UI: http://localhost:8082"
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list &>/dev/null && echo "  Status: ✅ Healthy" || echo "  Status: ❌ Not Responding"

# MinIO
echo "✓ MinIO Console: http://localhost:9001"
curl -s http://localhost:9001/minio/health/live &>/dev/null && echo "  Status: ✅ Healthy" || echo "  Status: ❌ Not Responding"

# Iceberg REST
echo "✓ Iceberg REST Catalog: http://localhost:8181/v1/config"
curl -s http://localhost:8181/v1/config &>/dev/null && echo "  Status: ✅ Healthy" || echo "  Status: ❌ Not Responding"

# ClickHouse
echo "✓ ClickHouse: http://localhost:8123/ping"
curl -s http://localhost:8123/ping &>/dev/null && echo "  Status: ✅ Healthy" || echo "  Status: ❌ Not Responding"

# Prometheus
echo "✓ Prometheus: http://localhost:9090/-/ready"
curl -s http://localhost:9090/-/ready &>/dev/null && echo "  Status: ✅ Healthy" || echo "  Status: ❌ Not Responding"

# Grafana
echo "✓ Grafana: http://localhost:3000/api/health"
curl -s http://localhost:3000/api/health &>/dev/null && echo "  Status: ✅ Healthy" || echo "  Status: ❌ Not Responding"

# Airflow
echo "✓ Airflow: http://localhost:8085/health"
curl -s http://localhost:8085/health &>/dev/null && echo "  Status: ✅ Healthy" || echo "  Status: ❌ Not Responding"

# Query API
echo "✓ Query API: http://localhost:8000/health"
curl -s http://localhost:8000/health &>/dev/null && echo "  Status: ✅ Healthy" || echo "  Status: ❌ Not Responding"

echo ""
echo "=== Data Layer Status ==="
docker exec clickhouse clickhouse-client -q "SELECT 'Kafka topics' AS service, COUNT(*) AS msg_count FROM (SELECT 1 FROM system.tables WHERE name LIKE 'silver_%') AS t" 2>/dev/null || echo "ClickHouse: Not responding"
```

---

## 🔍 **Troubleshooting Quick Links**

| Service | Issue | Command |
|---------|-------|---------|
| Kafka | No messages | `docker logs kafka --tail 50` |
| MinIO | Access denied | `docker logs minio --tail 50` |
| ClickHouse | Query fails | `docker logs clickhouse --tail 50` |
| Bronze-Ingest | No data | `docker logs bronze-ingest --tail 50` |
| Streaming | Pipeline stalled | `docker logs streaming --tail 50` |
| Airflow | DAG not running | Check Airflow UI for task logs |

---

## 📞 **Contact Points for Each Layer**

```
┌─────────────────────────────────────────────────────────┐
│         Data Producers (Kafka Topics)                   │
│  raw.interface.stats, raw.syslogs, raw.inventory        │
└─────────────────┬───────────────────────────────────────┘
                  │ Kafka Port 9092 / Kafka UI 8082
┌─────────────────▼───────────────────────────────────────┐
│  Bronze Layer (Iceberg + MinIO)                         │
│  MinIO Console 9001, Iceberg REST 8181                  │
└─────────────────┬───────────────────────────────────────┘
                  │ S3A Protocol (MinIO:9000)
┌─────────────────▼───────────────────────────────────────┐
│  Silver Layer (ClickHouse)                              │
│  ClickHouse API 8123, Query API 8000                    │
└─────────────────┬───────────────────────────────────────┘
                  │ SQL / Arrow Protocol
┌─────────────────▼───────────────────────────────────────┐
│  Gold Layer (Analytics)                                 │
│  Grafana 3000, Prometheus 9090                          │
└─────────────────────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────┐
│  Orchestration (Airflow)                                │
│  Airflow WebUI 8085, PostgreSQL 5432                    │
└─────────────────────────────────────────────────────────┘
```
