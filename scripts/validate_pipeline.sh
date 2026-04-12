#!/bin/bash
#
# Network Health Pipeline Validation Script
#
# Walks the pipeline stage-by-stage and reports pass / warn / fail for each
# check. Focus is on bronze row counts (bronze_ingest has regressed before)
# and the anomaly detection outputs downstream.

set -u

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓${NC} $1"; }
fail() { echo -e "${RED}✗${NC} $1"; }
warn() { echo -e "${YELLOW}⚠${NC} $1"; }
info() { echo -e "${BLUE}ℹ${NC} $1"; }

section() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════════"
    echo " $1"
    echo "═══════════════════════════════════════════════════════════════════"
}

# Returns numeric value or 0 for empty/non-numeric input.
to_int() {
    local v="${1:-0}"
    [[ "$v" =~ ^[0-9]+$ ]] && echo "$v" || echo "0"
}

ch_query() {
    docker exec clickhouse clickhouse-client --query "$1" 2>/dev/null
}

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          Network Health Pipeline Validation Script             ║"
echo "╚════════════════════════════════════════════════════════════════╝"

# ─────────────────────────────────────────────────────────────────────────
section "PHASE 1: Services Running"

services=(
    "zookeeper" "kafka" "minio" "iceberg-rest"
    "spark-master" "spark-worker" "clickhouse"
    "producer-interface" "producer-syslogs" "producer-inventory"
    "bronze-ingest" "streaming" "query-api"
)

for svc in "${services[@]}"; do
    status=$(docker inspect -f '{{.State.Status}}' "$svc" 2>/dev/null || echo "missing")
    case "$status" in
        running) pass "$svc is running" ;;
        missing) fail "$svc container not found" ;;
        *)       fail "$svc is $status" ;;
    esac
done

# ─────────────────────────────────────────────────────────────────────────
section "PHASE 2: Kafka Topics & Messages"

for topic in "raw.interface.stats" "raw.syslogs" "raw.inventory"; do
    offsets=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --bootstrap-server localhost:29092 --topic "$topic" 2>/dev/null || true)

    if [ -z "$offsets" ]; then
        warn "Kafka topic '$topic' is empty or unreachable"
        continue
    fi

    total=$(echo "$offsets" | awk -F: '{sum+=$3} END {print sum+0}')
    if [ "$(to_int "$total")" -gt 0 ]; then
        pass "Kafka topic '$topic' has $total messages"
    else
        warn "Kafka topic '$topic' exists but has 0 messages"
    fi
done

# ─────────────────────────────────────────────────────────────────────────
section "PHASE 3: Bronze Layer (Iceberg) — row counts"
#
# bronze_ingest has failed silently in the past (tables created, no rows
# written). We pull the current snapshot's total-records directly from the
# Iceberg REST catalog loadTable response, which is the authoritative count.

ICEBERG_REST="http://localhost:8181"

if ! curl -s -f "${ICEBERG_REST}/v1/config" >/dev/null; then
    fail "Iceberg REST catalog not reachable at ${ICEBERG_REST}"
else
    pass "Iceberg REST catalog reachable"

    ns_json=$(curl -s "${ICEBERG_REST}/v1/namespaces/bronze/tables" 2>/dev/null)
    bronze_tables=$(echo "$ns_json" | python3 -c \
        'import sys,json; d=json.load(sys.stdin); print(" ".join(i["name"] for i in d.get("identifiers",[])))' \
        2>/dev/null)

    if [ -z "$bronze_tables" ]; then
        fail "Bronze namespace has no tables (bronze_ingest never wrote?)"
    else
        total_bronze_rows=0
        for tbl in $bronze_tables; do
            meta=$(curl -s "${ICEBERG_REST}/v1/namespaces/bronze/tables/${tbl}" 2>/dev/null)
            rows=$(echo "$meta" | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
    m = d.get("metadata", {})
    snap_id = m.get("current-snapshot-id")
    if snap_id is None or snap_id == -1:
        print(0); sys.exit()
    for s in m.get("snapshots", []):
        if s.get("snapshot-id") == snap_id:
            print(int(s.get("summary", {}).get("total-records", 0)))
            sys.exit()
    print(0)
except Exception:
    print(0)
' 2>/dev/null)
            rows=$(to_int "$rows")
            total_bronze_rows=$((total_bronze_rows + rows))
            if [ "$rows" -gt 0 ]; then
                pass "bronze.${tbl}: $rows rows"
            else
                fail "bronze.${tbl}: 0 rows (bronze_ingest not writing!)"
            fi
        done

        echo ""
        if [ "$total_bronze_rows" -eq 0 ]; then
            fail "Bronze layer is empty — check bronze-ingest logs:"
            echo "    docker logs bronze-ingest --tail 100"
        else
            info "Total bronze rows across all tables: $total_bronze_rows"
        fi
    fi
fi

# ─────────────────────────────────────────────────────────────────────────
section "PHASE 4: Silver Layer (ClickHouse mirrors)"

silver_tables=("silver_interface_stats_src" "silver_syslogs_src")

for tbl in "${silver_tables[@]}"; do
    count=$(ch_query "SELECT count() FROM network_health.${tbl}")
    count=$(to_int "$count")
    if [ "$count" -gt 0 ]; then
        pass "silver.${tbl}: $count rows"
    else
        warn "silver.${tbl}: 0 rows (streaming job may not have caught up)"
    fi
done

# ─────────────────────────────────────────────────────────────────────────
section "PHASE 5: Gold Layer (ClickHouse)"

gold_tables=("gold_site_health_hourly" "gold_anomaly_flags")

for tbl in "${gold_tables[@]}"; do
    count=$(ch_query "SELECT count() FROM network_health.${tbl}")
    count=$(to_int "$count")
    if [ "$count" -gt 0 ]; then
        pass "gold.${tbl}: $count rows"
    else
        warn "gold.${tbl}: 0 rows"
    fi
done

# ─────────────────────────────────────────────────────────────────────────
section "PHASE 6: Anomaly Detection"

echo "Ingest-time (Z-score & IQR) on silver_interface_stats_src:"
z_iqr=$(ch_query "
    SELECT
        countIf(ingest_z_score > 2.0),
        countIf(ingest_iqr_score > 1.0),
        countIf((ingest_z_score > 2.0) AND (ingest_iqr_score > 1.0)),
        countIf(ingest_anomaly)
    FROM network_health.silver_interface_stats_src
    FORMAT TSV
")
if [ -n "$z_iqr" ]; then
    read -r z_cnt iqr_cnt both_cnt total_cnt <<<"$z_iqr"
    pass "  Z-score>2: $(to_int "$z_cnt")  |  IQR>1: $(to_int "$iqr_cnt")  |  overlap: $(to_int "$both_cnt")  |  flagged: $(to_int "$total_cnt")"

    z_devices=$(ch_query "
        SELECT arrayStringConcat(groupArray(device_id), ',')
        FROM (
            SELECT DISTINCT device_id
            FROM network_health.silver_interface_stats_src
            WHERE ingest_z_score > 2.0
            ORDER BY device_id
        )
    ")
    iqr_devices=$(ch_query "
        SELECT arrayStringConcat(groupArray(device_id), ',')
        FROM (
            SELECT DISTINCT device_id
            FROM network_health.silver_interface_stats_src
            WHERE ingest_iqr_score > 1.0
            ORDER BY device_id
        )
    ")
    info "  Z-score devices: ${z_devices:-none}"
    info "  IQR devices: ${iqr_devices:-none}"

    if [ "$(to_int "$both_cnt")" -gt 0 ]; then
        warn "  Some records violate both Z-score and IQR"
    fi
else
    warn "  silver_interface_stats_src not queryable"
fi

echo ""
echo "Ingest-time (FLATLINE) on silver_interface_stats_src ingest_flags:"
flat=$(to_int "$(ch_query "SELECT countIf(has(ingest_flags, 'FLATLINE')) FROM network_health.silver_interface_stats_src")")
if [ "$flat" -gt 0 ]; then
    pass "  FLATLINE flagged records: $flat"
else
    warn "  No FLATLINE records flagged yet"
fi

echo ""
echo "Deterministic three-anomaly smoke check (unified streaming path):"
three_check=$(ch_query "
    SELECT
        countIf(device_id='edge-r2' AND ingest_z_score > 2.0),
        countIf(device_id='fw1' AND ingest_iqr_score > 1.0),
        countIf(device_id='edge-r1' AND has(ingest_flags, 'FLATLINE'))
    FROM network_health.silver_interface_stats_src
    FORMAT TSV
")
if [ -n "$three_check" ]; then
    read -r z_dev iqr_dev flat_dev <<<"$three_check"
    z_dev=$(to_int "$z_dev")
    iqr_dev=$(to_int "$iqr_dev")
    flat_dev=$(to_int "$flat_dev")
    info "  edge-r2 HIGH_Z_SCORE rows: $z_dev"
    info "  fw1 IQR_OUTLIER rows: $iqr_dev"
    info "  edge-r1 FLATLINE rows: $flat_dev"
    if [ "$z_dev" -gt 0 ] && [ "$iqr_dev" -gt 0 ] && [ "$flat_dev" -gt 0 ]; then
        pass "  All 3 deterministic anomaly paths observed (z-score, iqr, flatline)"
    else
        warn "  Missing one or more deterministic anomaly paths"
    fi
else
    warn "  Could not query deterministic anomaly smoke check"
fi

echo ""
echo "Batch (Isolation Forest MODEL) on gold_anomaly_flags:"
model=$(to_int "$(ch_query "SELECT count() FROM network_health.gold_anomaly_flags WHERE anomaly_type='MODEL'")")
if [ "$model" -gt 0 ]; then
    pass "  MODEL anomalies: $model"
else
    warn "  No MODEL anomalies yet (nightly Airflow batch)"
fi

echo ""
echo "Syslog burst anomalies on gold_anomaly_flags:"
syslog_burst=$(to_int "$(ch_query "SELECT count() FROM network_health.gold_anomaly_flags WHERE anomaly_type='CRITICAL_SYSLOG_BURST'")")
if [ "$syslog_burst" -gt 0 ]; then
    pass "  CRITICAL_SYSLOG_BURST anomalies: $syslog_burst"
else
    warn "  No CRITICAL_SYSLOG_BURST anomalies yet"
fi

# ─────────────────────────────────────────────────────────────────────────
section "USEFUL URLS"
cat <<EOF
  Grafana      http://localhost:3000       (admin/admin)
  Airflow      http://localhost:8085       (admin/admin)
  Kafka UI     http://localhost:8082
  Prometheus   http://localhost:9090
  Query API    http://localhost:8000
  Spark UI     http://localhost:8080
  MinIO        http://localhost:9001
EOF

# ─────────────────────────────────────────────────────────────────────────
section "DEBUGGING HINTS"
cat <<'EOF'
  docker logs bronze-ingest --tail 100
  docker logs streaming     --tail 100
  docker exec -it clickhouse clickhouse-client
    SELECT * FROM network_health.gold_anomaly_flags ORDER BY ts DESC LIMIT 5;

  Re-run this script after each stage to watch counts climb.
EOF

echo ""
pass "Validation complete"
