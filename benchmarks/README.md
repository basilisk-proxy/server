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
