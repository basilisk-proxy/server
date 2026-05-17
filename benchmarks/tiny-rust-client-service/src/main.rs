use anyhow::{Context, Result};
use axum::{Router, body::Bytes, routing::get, routing::post};
use rust_client::{BasiliskClient, BasiliskClientConfig};
use std::{env, net::SocketAddr};
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG").unwrap_or_else(|_| "tiny_rust_client_service=info".to_string()),
        )
        .init();

    let gateway_base_url = env_var("BASILISK_GATEWAY_BASE_URL", "http://basilisk:8080");
    let bus_host = env_var("BASILISK_BUS_HOST", "basilisk");
    let bus_port = parse_env::<u16>("BASILISK_BUS_PORT", 5090)?;
    let registration_token = env_var("BASILISK_REGISTRATION_TOKEN", "secret-token");

    let service_id = env_var("BENCH_SERVICE_ID", "bench-service");
    let fingerprint = env_var("BENCH_SERVICE_FINGERPRINT", "bench-service-v1");
    let path_prefix = env_var("BENCH_PATH_PREFIX", "/bench");
    let bind_host = env_var("BENCH_BIND_HOST", "0.0.0.0");
    let bind_port = parse_env::<u16>("BENCH_BIND_PORT", 7001)?;
    let advertised_host = env_var("BENCH_ADVERTISED_HOST", "tiny-rust-client-service");
    let weight = parse_env::<i32>("BENCH_WEIGHT", 1)?;

    let basilisk_client = connect_with_retry(BasiliskClientConfig {
        gateway_base_url,
        bus_host,
        bus_port,
        service_id: service_id.clone(),
        fingerprint,
        path_prefixes: vec![path_prefix],
        scheme: "http".to_string(),
        host: advertised_host,
        port: bind_port,
        weight,
        registration_auth_type: "token".to_string(),
        registration_token,
    })
    .await?;

    info!(
        service_id = %service_id,
        instance_id = %basilisk_client.instance_id,
        "tiny benchmark service registered with basilisk"
    );

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/bench/ping", get(|| async { "pong" }))
        .route("/bench/echo", post(|body: Bytes| async move { body }))
        .with_state(basilisk_client);

    let listener = tokio::net::TcpListener::bind(SocketAddr::from((
        bind_host
            .parse::<std::net::IpAddr>()
            .with_context(|| format!("invalid BENCH_BIND_HOST value: {bind_host}"))?,
        bind_port,
    )))
    .await?;

    info!(address = %listener.local_addr()?, "tiny benchmark service listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn connect_with_retry(config: BasiliskClientConfig) -> Result<BasiliskClient> {
    let mut attempt = 0_u32;
    let max_attempts = 30_u32;

    loop {
        attempt += 1;
        match BasiliskClient::connect(config.clone()).await {
            Ok(client) => return Ok(client),
            Err(err) if attempt < max_attempts => {
                warn!(
                    attempt,
                    max_attempts,
                    error = %err,
                    "basilisk registration/connect failed; retrying"
                );
                sleep(Duration::from_millis(500)).await;
            }
            Err(err) => {
                return Err(err).context("unable to connect tiny service to basilisk");
            }
        }
    }
}

fn env_var(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_env<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr + Copy + ToString,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = env::var(name).unwrap_or_else(|_| default.to_string());
    value
        .parse::<T>()
        .with_context(|| format!("invalid {name} value: {value}"))
}
