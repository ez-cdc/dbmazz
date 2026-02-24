pub mod handlers;

use std::sync::Arc;
use std::time::Instant;

use axum::{
    routing::{get, post},
    Router,
};
use tokio::sync::RwLock;
use tracing::info;

use crate::grpc::state::SharedState;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceSetupConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SinkSetupConfig {
    pub host: String,
    pub fe_http_port: u16,
    pub fe_mysql_port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}

pub struct HttpAppState {
    pub engine_state: RwLock<Option<Arc<SharedState>>>,
    pub start_time: Instant,
    pub source_config: RwLock<Option<SourceSetupConfig>>,
    pub sink_config: RwLock<Option<SinkSetupConfig>>,
}

pub async fn start_http_server(
    port: u16,
    initial_engine: Option<Arc<SharedState>>,
) -> anyhow::Result<()> {
    let state = Arc::new(HttpAppState {
        engine_state: RwLock::new(initial_engine),
        start_time: Instant::now(),
        source_config: RwLock::new(None),
        sink_config: RwLock::new(None),
    });

    let app = Router::new()
        // Dashboard + status
        .route("/", get(handlers::index))
        .route("/healthz", get(handlers::healthz))
        .route("/status", get(handlers::status))
        .route("/metrics/prometheus", get(handlers::prometheus))
        // Replication control
        .route("/pause", post(handlers::pause))
        .route("/resume", post(handlers::resume))
        .route("/drain-stop", post(handlers::drain_stop))
        // Setup endpoints
        .route("/api/datasources/test", post(handlers::test_connection))
        .route("/api/tables/discover", post(handlers::discover_tables))
        .route("/api/replication/start", post(handlers::start_replication))
        .route("/api/replication/stop", post(handlers::stop_replication))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("HTTP API listening on http://0.0.0.0:{}", port);
    axum::serve(listener, app).await?;

    Ok(())
}
