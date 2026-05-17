#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$BENCH_ROOT/results"
COMPOSE_FILE="$BENCH_ROOT/docker-compose.yml"
NETWORK_NAME="basilisk-bench-net"
READY_TIMEOUT_SEC="${BENCH_READY_TIMEOUT_SEC:-120}"
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

service_is_running() {
  local service="$1"
  docker compose -f "$COMPOSE_FILE" ps --status running --services | grep -Fx "$service" >/dev/null
}

wait_for_url() {
  local url="$1"
  local label="$2"
  local required_service="$3"
  local start_ts
  start_ts="$(date +%s)"

  while true; do
    if ! service_is_running "$required_service"; then
      echo "Service '$required_service' is not running while waiting for ${label} (${url})" >&2
      docker compose -f "$COMPOSE_FILE" ps >&2 || true
      docker compose -f "$COMPOSE_FILE" logs --tail=120 basilisk tiny-rust-client-service >&2 || true
      exit 1
    fi

    if docker run --rm --network "$NETWORK_NAME" curlimages/curl:8.8.0 \
      -fsS "$url" >/dev/null 2>&1; then
      break
    fi

    if [ $(( $(date +%s) - start_ts )) -ge "$READY_TIMEOUT_SEC" ]; then
      echo "Timed out waiting for ${label} at ${url} after ${READY_TIMEOUT_SEC}s" >&2
      docker compose -f "$COMPOSE_FILE" ps >&2 || true
      docker compose -f "$COMPOSE_FILE" logs --tail=120 basilisk tiny-rust-client-service >&2 || true
      exit 1
    fi

    sleep 1
  done
}

# Wait for gateway control plane first, then for /bench route availability.
wait_for_url "http://basilisk:8080/registry/services" "basilisk gateway" "basilisk"
wait_for_url "http://basilisk:8080/bench/ping" "bench route" "tiny-rust-client-service"

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
