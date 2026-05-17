#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$BENCH_ROOT/results"
COMPOSE_FILE="$BENCH_ROOT/docker-compose.yml"
NETWORK_NAME="basilisk-bench-net"

mkdir -p "$RESULTS_DIR"

cleanup() {
  docker compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" up -d --build

until docker run --rm --network "$NETWORK_NAME" curlimages/curl:8.8.0 \
  -fsS http://basilisk:8080/bench/ping >/dev/null; do
  sleep 1
done

run_case() {
  local label="$1"
  local target="$2"

  echo "Running round-trip benchmark for ${label} (${target})"
  docker run --rm --network "$NETWORK_NAME" \
    -v "$BENCH_ROOT/k6:/scripts:ro" \
    -v "$RESULTS_DIR:/results" \
    -e TARGET_BASE_URL="$target" \
    -e BENCH_VUS="${BENCH_VUS:-25}" \
    -e BENCH_DURATION="${BENCH_DURATION:-20s}" \
    -e BENCH_PAYLOAD_BYTES="${BENCH_PAYLOAD_BYTES:-8192}" \
    -e BENCH_POST_RATIO="${BENCH_POST_RATIO:-0.7}" \
    grafana/k6:0.53.0 run /scripts/roundtrip_bench.js --summary-export "/results/${label}-roundtrip-summary.json"

  echo "Saved summary to $RESULTS_DIR/${label}-roundtrip-summary.json"
}

run_case "basilisk-direct" "http://basilisk:8080"
run_case "nginx-front" "http://nginx-front"
run_case "haproxy-front" "http://haproxy-front"

echo "Round-trip benchmark pipeline completed. Raw summaries are in $RESULTS_DIR"
