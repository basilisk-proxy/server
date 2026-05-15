FROM rust:1.94 AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /src/target/release/basilisk /usr/local/bin/basilisk
COPY ./basilisk.lua /etc/basilisk/basilisk.lua

EXPOSE 8080 5090
ENTRYPOINT ["/usr/local/bin/basilisk"]
CMD ["/etc/basilisk/basilisk.lua"]
