FROM rust:1.94 AS builder

WORKDIR /src
COPY src ./src
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /src/target/release/basilisk /usr/local/bin/basilisk

# Ideally, you are to maintain this volume mount externally but uncomment this for simpler set-ups.
# COPY ./basilisk.lua /etc/basilisk/basilisk.lua

EXPOSE 8080 5090
ENTRYPOINT ["/usr/local/bin/basilisk"]
CMD ["/etc/basilisk/basilisk.lua"]
