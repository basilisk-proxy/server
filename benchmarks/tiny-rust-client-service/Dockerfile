FROM rust:1.88-bookworm AS builder

WORKDIR /workspace
COPY ../rust-client/Cargo.toml ../rust-client/Cargo.lock ./rust-client/
COPY ../rust-client/src ./rust-client/src
COPY ../proxy-server/benchmarks/tiny-rust-client-service/Cargo.toml ./proxy-server/benchmarks/tiny-rust-client-service/
COPY ../proxy-server/benchmarks/tiny-rust-client-service/src ./proxy-server/benchmarks/tiny-rust-client-service/src

RUN CARGO_TARGET_DIR=/workspace/target \
    cargo build --release --manifest-path proxy-server/benchmarks/tiny-rust-client-service/Cargo.toml

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /workspace/target/release/tiny-rust-client-service /usr/local/bin/tiny-rust-client-service

EXPOSE 7001
ENTRYPOINT ["/usr/local/bin/tiny-rust-client-service"]
