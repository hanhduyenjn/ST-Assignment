#!/bin/bash
# Run all long-lived pipeline modes in parallel.
# If any process exits, kill the rest and exit non-zero so Docker restarts the container.

set -euo pipefail

log() { echo "[entrypoint] $*"; }

log "Starting pipeline modes..."

uv run python main.py --mode bronze-ingest &
PID_BRONZE=$!
log "bronze-ingest pid=$PID_BRONZE"

uv run python main.py --mode streaming &
PID_STREAMING=$!
log "streaming pid=$PID_STREAMING"

uv run python main.py --mode metrics-exporter --port "${PROMETHEUS_PORT:-8000}" &
PID_METRICS=$!
log "metrics-exporter pid=$PID_METRICS"

# Wait for any child to exit; exit with its code so Docker restarts.
wait -n
EXIT_CODE=$?
log "A pipeline process exited with code $EXIT_CODE — shutting down."
kill $PID_BRONZE $PID_STREAMING $PID_METRICS 2>/dev/null || true
exit $EXIT_CODE
