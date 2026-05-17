# Benchmarks

This folder sets up a reproducible benchmark pipeline for Basilisk from inside the `proxy-server` repository, using a tiny Rust service built on the sibling `rust-client` crate.

## What gets started

- `basilisk`: the local `proxy-server` build from `../Dockerfile`
- `tiny-rust-client-service`: tiny Rust service that auto-registers on `/bench`
- `nginx-front`: minimal NGINX reverse proxy in front of Basilisk
- `haproxy-front`: minimal HAProxy reverse proxy in front of Basilisk

## Prerequisites

- Docker Engine
- Docker Compose v2 (`docker compose`)

## Run the benchmark pipeline

```bash
cd /home/the-infinite/Projects/Nemesys\ LLC/basilisk/proxy-server
./benchmarks/scripts/run_benchmarks.sh
```

Customize load settings if needed:

```bash
cd /home/the-infinite/Projects/Nemesys\ LLC/basilisk/proxy-server
BENCH_VUS=50 BENCH_DURATION=30s ./benchmarks/scripts/run_benchmarks.sh
```

Raw outputs are written to `benchmarks/results/*.json`.

## Run round-trip payload benchmark (request and response body path)

This profile mixes GET and POST traffic and validates full body echo round-trips:

```bash
cd /home/the-infinite/Projects/Nemesys\ LLC/basilisk/proxy-server
./benchmarks/scripts/run_roundtrip_benchmarks.sh
```

Tune the payload-heavy profile if needed:

```bash
cd /home/the-infinite/Projects/Nemesys\ LLC/basilisk/proxy-server
BENCH_VUS=50 BENCH_DURATION=30s BENCH_PAYLOAD_BYTES=32768 BENCH_POST_RATIO=0.8 ./benchmarks/scripts/run_roundtrip_benchmarks.sh
```

Round-trip summaries are written to:

- `benchmarks/results/basilisk-direct-roundtrip-summary.json`
- `benchmarks/results/nginx-front-roundtrip-summary.json`
- `benchmarks/results/haproxy-front-roundtrip-summary.json`

Generate a compact Markdown comparison table from these JSON outputs:

```bash
cd /home/the-infinite/Projects/Nemesys\ LLC/basilisk/proxy-server
python3 benchmarks/scripts/summarize_roundtrip_results.py
```

The Markdown report is rendered as a fixed-width monospace table where all columns use equal widths for readability.

Write the report to a file:

```bash
cd /home/the-infinite/Projects/Nemesys\ LLC/basilisk/proxy-server
python3 benchmarks/scripts/summarize_roundtrip_results.py --output benchmarks/results/roundtrip-report.md
```

Generate CI-friendly JSON output and fail when any p95 regression exceeds a threshold:

```bash
cd /home/the-infinite/Projects/Nemesys\ LLC/basilisk/proxy-server
python3 benchmarks/scripts/summarize_roundtrip_results.py \
  --format json \
  --max-p95-regression-percent 15 \
  --output benchmarks/results/roundtrip-report.json
```

The CI workflow (`.github/workflows/ci.yml`) runs this validation on fixtures and posts a sticky pull request comment with the generated Markdown report.

## Tear down

```bash
cd /home/the-infinite/Projects/Nemesys\ LLC/basilisk/proxy-server
./benchmarks/scripts/teardown.sh
```

## Tiny service local compile check

```bash
cd /home/the-infinite/Projects/Nemesys\ LLC/basilisk/proxy-server
cargo check --manifest-path benchmarks/tiny-rust-client-service/Cargo.toml
```
