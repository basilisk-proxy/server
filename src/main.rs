use axum::{
    Router,
    routing::{any, delete, get, post},
};
use basilisk::gateway::{
    AppState,
    proxy::{ProxyHandler, new_upstream_http_client},
    routes,
};
use basilisk::lua_config::load_config_and_runtime;
use basilisk::observability::RuntimeTelemetry;
use basilisk::registry::{ServiceRegistry, run_maintenance};
use basilisk::service_bus::connection_manager::ConnectionManager;
use basilisk::service_bus::contracts::BASILISK_METRICS_DISTRIBUTION_TOPIC;
use basilisk::service_bus::server::run_server;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args();
    let program = args.next().unwrap_or_else(|| "basilisk".to_string());
    let lua_entrypoint = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("Usage: {} <path-to-basilisk.lua>", program))?;
    if args.next().is_some() {
        return Err(anyhow::anyhow!("Usage: {} <path-to-basilisk.lua>", program));
    }

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());
    let telemetry = Arc::new(RuntimeTelemetry::new());

    // Load configuration exclusively from Lua.
    let (config, lua_runtime, cache) = load_config_and_runtime(
        &lua_entrypoint,
        Arc::clone(&registry),
        Arc::clone(&connection_manager),
    )?;

    // Initialize tracing using RUST_LOG when present, otherwise Lua config fallback.
    let (env_filter, filter_source) = match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(filter) => (filter, "RUST_LOG"),
        Err(_) => (
            tracing_subscriber::EnvFilter::new(config.observability.log_level.clone()),
            "lua_config.observability.log_level",
        ),
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!(
        filter_source,
        configured_log_level = %config.observability.log_level,
        lua_entrypoint = %lua_entrypoint,
        "gateway tracing initialized"
    );

    // Start Service Bus TCP server
    let sb_config = config.clone();
    let sb_cm = Arc::clone(&connection_manager);
    let sb_reg = Arc::clone(&registry);
    tokio::spawn(async move {
        if let Err(e) = run_server(sb_config, sb_cm, sb_reg).await {
            tracing::error!("Service Bus server error: {}", e);
        }
    });

    if config.service_bus.monitoring_enabled {
        let (metrics_tx, mut metrics_rx) = mpsc::unbounded_channel();
        connection_manager
            .subscribe_internal(BASILISK_METRICS_DISTRIBUTION_TOPIC.to_string(), metrics_tx);
        let telemetry_for_metrics = Arc::clone(&telemetry);
        let registry_for_metrics = Arc::clone(&registry);

        tokio::spawn(async move {
            while let Some(event) = metrics_rx.recv().await {
                registry_for_metrics
                    .record_metrics_heartbeat(&event.service_id, &event.instance_id);
                telemetry_for_metrics.ingest_distribution_event(&event);
            }
        });
    }

    // Start Registry Maintenance task
    let main_config = config.clone();
    let main_reg = Arc::clone(&registry);
    let main_cm = Arc::clone(&connection_manager);
    tokio::spawn(async move {
        run_maintenance(main_config, main_reg, main_cm).await;
    });

    // Setup HTTP Gateway
    let gateway_addr = SocketAddr::from(([0, 0, 0, 0], config.server.port));
    let state = Arc::new(AppState {
        config: config.clone(),
        gateway_addr,
        registry: Arc::clone(&registry),
        connection_manager: Arc::clone(&connection_manager),
        proxy_handler: ProxyHandler::new(),
        lua_runtime,
        cache,
        upstream_client: new_upstream_http_client(),
        telemetry,
    });

    let app = Router::new()
        // Registry APIs
        .route("/registry/register", post(routes::register))
        .route(
            "/registry/services/{service_id}/instances/{instance_id}",
            delete(routes::deregister),
        )
        .route("/registry/services", get(routes::get_all_services))
        .route("/registry/services/{service_id}", get(routes::get_service))
        .route(
            "/registry/metrics/runtime",
            get(routes::get_runtime_metrics),
        )
        .fallback(any(ProxyHandler::handle_proxy))
        .with_state(state);

    tracing::info!("HTTP Gateway listening on {}", gateway_addr);

    let listener = tokio::net::TcpListener::bind(&gateway_addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}
