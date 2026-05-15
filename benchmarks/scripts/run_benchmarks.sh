#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$BENCH_ROOT/.." && pwd)"
RESULTS_DIR="$BENCH_ROOT/results"
COMPOSE_FILE="$BENCH_ROOT/docker-compose.yml"
NETWORK_NAME="basilisk-bench-net"
export DOCKER_CONFIG="${DOCKER_CONFIG:-$BENCH_ROOT/.docker-config}"

mkdir -p "$RESULTS_DIR"
mkdir -p "$DOCKER_CONFIG"

if [ ! -f "$DOCKER_CONFIG/config.json" ]; then
  printf '{}\n' > "$DOCKER_CONFIG/config.json"
fi

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
  local output="$RESULTS_DIR/${label}-summary.json"

  echo "Running benchmark for ${label} (${target})"
  docker run --rm --network "$NETWORK_NAME" \
    -v "$BENCH_ROOT/k6:/scripts:ro" \
    -v "$RESULTS_DIR:/results" \
    -e TARGET_BASE_URL="$target" \
    -e BENCH_VUS="${BENCH_VUS:-25}" \
    -e BENCH_DURATION="${BENCH_DURATION:-20s}" \
    grafana/k6:0.53.0 run /scripts/http_bench.js --summary-export "/results/${label}-summary.json"

  echo "Saved summary to $output"
}

run_case "basilisk-direct" "http://basilisk:8080"
run_case "nginx-front" "http://nginx-front"
run_case "haproxy-front" "http://haproxy-front"

echo "Benchmark pipeline completed from $REPO_ROOT. Raw summaries are in $RESULTS_DIR"
