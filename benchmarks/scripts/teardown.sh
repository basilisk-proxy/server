#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export DOCKER_CONFIG="${DOCKER_CONFIG:-$BENCH_ROOT/.docker-config}"

docker compose -f "$BENCH_ROOT/docker-compose.yml" down --remove-orphans
