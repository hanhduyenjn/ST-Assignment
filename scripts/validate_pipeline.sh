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
section "PHASE 2: Bronze Layer (Iceberg) — row counts"
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
section "PHASE 3: Silver Layer (ClickHouse mirrors & staging)"

silver_tables=("silver_interface_stats_src" "silver_syslogs_src" "silver_flatline_anomaly_flags")

for tbl in "${silver_tables[@]}"; do
    count=$(ch_query "SELECT count() FROM network_health.${tbl}")
    count=$(to_int "$count")
    if [ "$count" -gt 0 ]; then
        pass "silver.${tbl}: $count rows"
    else
        warn "silver.${tbl}: 0 rows (streaming job may not have caught up)"
    fi
done

echo ""
echo "DLQ quarantine status:"
dlq_count=$(ch_query "SELECT count() FROM network_health.silver_dlq_quarantine_src")
dlq_count=$(to_int "$dlq_count")
if [ "$dlq_count" -gt 0 ]; then
    warn "DLQ records quarantined: $dlq_count"
    dlq_reasons=$(ch_query "
        SELECT
            quarantine_reason,
            count() as cnt
        FROM network_health.silver_dlq_quarantine_src
        GROUP BY quarantine_reason
        ORDER BY cnt DESC
        FORMAT TSV
    ")
    echo "$dlq_reasons" | while read -r reason count; do
        info "  ${reason}: $(to_int "$count")"
    done
else
    pass "DLQ is clean: $dlq_count quarantined"
fi

# ─────────────────────────────────────────────────────────────────────────
section "PHASE 5: Baselines & Materialized Views"

echo "Device baseline parameters (ReplacingMergeTree):"
baseline_count=$(ch_query "SELECT count() FROM network_health.device_baseline_params")
baseline_count=$(to_int "$baseline_count")
if [ "$baseline_count" -gt 0 ]; then
    pass "device_baseline_params: $baseline_count rows"
    sources=$(ch_query "
        SELECT DISTINCT source FROM network_health.device_baseline_params
    ")
    info "  Sources: $(echo "$sources" | tr '\n' ',')"
else
    warn "device_baseline_params: empty (weekly batch not run?)"
fi

echo ""
echo "Materialized Views (refreshed every 1 minute):"
for mv in "mv_anomaly_summary" "mv_site_health_hourly"; do
    # Check if MV exists by querying the system table
    mv_status=$(ch_query "SELECT name FROM system.tables WHERE database='network_health' AND name='${mv}'" 2>/dev/null || echo "")
    if [ -n "$mv_status" ]; then
        pass "MV ${mv} exists"
    else
        warn "MV ${mv} not found"
    fi
done

# ─────────────────────────────────────────────────────────────────────────
section "PHASE 6: Gold Layer (ClickHouse)"

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

echo ""
echo "Gold layer aggregation quality:"
health_by_site=$(ch_query "
    SELECT
        site_id,
        count() as records,
        min(health_score) as min_score,
        max(health_score) as max_score,
        round(avg(health_score), 2) as avg_score
    FROM network_health.gold_site_health_hourly
    GROUP BY site_id
    ORDER BY site_id
    FORMAT TSV
")
if [ -n "$health_by_site" ]; then
    info "Site health aggregations:"
    echo "$health_by_site" | while read -r site_id records min_score max_score avg_score; do
        info "  ${site_id}: records=$records min=$min_score max=$max_score avg=$avg_score"
    done
else
    warn "No site health aggregations yet"
fi

# ─────────────────────────────────────────────────────────────────────────
section "PHASE 7: Prometheus Metrics (Pipeline Observability)"

PROM_URL="http://localhost:9090"

# Check Prometheus reachability
if curl -s -f "${PROM_URL}/api/v1/query?query=up" >/dev/null 2>&1; then
    pass "Prometheus reachable at ${PROM_URL}"

    echo ""
    echo "Ingest pipeline metrics:"

    # ingest_records_total
    ingest_total=$(curl -s "${PROM_URL}/api/v1/query?query=ingest_records_total" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(float(r['value'][1]) for r in d.get('data',{}).get('result',[])) if d.get('status')=='success' else 0)" 2>/dev/null)
    ingest_total=$(to_int "$ingest_total")
    if [ "$ingest_total" -gt 0 ]; then
        pass "  ingest_records_total: $ingest_total"
    else
        warn "  ingest_records_total: $ingest_total (no ingest yet)"
    fi

    # dlq_records_total
    dlq_total=$(curl -s "${PROM_URL}/api/v1/query?query=dlq_records_total" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(float(r['value'][1]) for r in d.get('data',{}).get('result',[])) if d.get('status')=='success' else 0)" 2>/dev/null)
    dlq_total=$(to_int "$dlq_total")
    if [ "$dlq_total" -eq 0 ]; then
        pass "  dlq_records_total: $dlq_total (no rejections)"
    else
        warn "  dlq_records_total: $dlq_total (check DLQ breakdown)"
    fi

    echo ""
    echo "Anomaly detection metrics:"

    # anomalies_total
    anomalies=$(curl -s "${PROM_URL}/api/v1/query?query=anomalies_total" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(float(r['value'][1]) for r in d.get('data',{}).get('result',[])) if d.get('status')=='success' else 0)" 2>/dev/null)
    anomalies=$(to_int "$anomalies")
    if [ "$anomalies" -gt 0 ]; then
        pass "  anomalies_total: $anomalies"
    else
        warn "  anomalies_total: $anomalies (no anomalies detected yet)"
    fi

    # latest_site_health_score
    health=$(curl -s "${PROM_URL}/api/v1/query?query=latest_site_health_score" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); scores = [float(r['value'][1]) for r in d.get('data',{}).get('result',[])]; print(f'min={min(scores):.0f} max={max(scores):.0f} avg={sum(scores)/len(scores):.1f}') if scores else print('no data')" 2>/dev/null)
    info "  latest_site_health_score: $health"

    echo ""
    echo "Pipeline latency metrics:"

    # stream_batch_duration_seconds
    latency=$(curl -s "${PROM_URL}/api/v1/query?query=stream_batch_duration_seconds_bucket" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('data',{}).get('result',[])) > 0)" 2>/dev/null)
    if [ "$latency" = "True" ]; then
        pass "  stream_batch_duration_seconds: collecting"
    else
        warn "  stream_batch_duration_seconds: no histogram data yet"
    fi
else
    warn "Prometheus not reachable at ${PROM_URL}"
fi

# ─────────────────────────────────────────────────────────────────────────
section "PHASE 8: Anomaly Detection"

echo "Anomaly type counts on gold_anomaly_flags FINAL:"
anomaly_counts=$(ch_query "
    SELECT
        countIf(anomaly_type = 'THRESHOLD_SATURATED'),
        countIf(anomaly_type = 'HIGH_Z_SCORE'),
        countIf(anomaly_type = 'IQR_OUTLIER'),
        countIf(anomaly_type = 'FLATLINE'),
        countIf(anomaly_type = 'CRITICAL_SYSLOG_BURST'),
        countIf(anomaly_type = 'MODEL'),
        count()
    FROM network_health.gold_anomaly_flags FINAL
    FORMAT TSV
")
if [ -n "$anomaly_counts" ]; then
    read -r thr_cnt z_cnt iqr_cnt flat_cnt crit_cnt model_cnt total_cnt <<<"$anomaly_counts"
    while read -r label count; do
        n=$(to_int "$count")
        if [ "$n" -gt 0 ]; then
            pass "  ${label}: $n"
        else
            warn "  ${label}: 0"
        fi
    done <<EOF
THRESHOLD_SATURATED $thr_cnt
HIGH_Z_SCORE $z_cnt
IQR_OUTLIER $iqr_cnt
FLATLINE $flat_cnt
CRITICAL_SYSLOG_BURST $crit_cnt
MODEL $model_cnt
EOF
    info "  total gold anomaly records: $(to_int "$total_cnt")"
else
    warn "  gold_anomaly_flags not queryable"
fi

echo ""
echo "Site degradation alerts (health_score < 40):"
degraded=$(ch_query "
    SELECT
        site_id,
        count() as degraded_windows,
        round(min(health_score), 1) as min_score,
        round(avg(health_score), 1) as avg_score
    FROM network_health.gold_site_health_hourly
    WHERE health_score < 40
    GROUP BY site_id
    ORDER BY min_score ASC
    FORMAT TSV
")
if [ -n "$degraded" ]; then
    warn "  Degraded sites detected:"
    echo "$degraded" | while read -r site_id windows min_score avg_score; do
        warn "    ${site_id}: $windows windows, min=$min_score avg=$avg_score"
    done
else
    pass "  All sites healthy (health_score >= 40)"
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
        SELECT * FROM network_health.gold_anomaly_flags ORDER BY detected_at DESC LIMIT 5;

  Re-run this script after each stage to watch counts climb.
EOF

echo ""
pass "Validation complete"
