pub mod handlers;
pub mod traffic;

use std::sync::Arc;
use tokio::sync::RwLock;

use axum::{routing::{get, post}, Router};
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::grpc::state::SharedState;

#[derive(Clone, Copy, PartialEq, serde::Serialize)]
pub enum DemoPhase {
    Setup,
    Replicating,
    Stopped,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceDemoConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SinkDemoConfig {
    pub host: String,
    pub fe_http_port: u16,
    pub fe_mysql_port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}

pub struct DemoState {
    pub engine_state: RwLock<Option<Arc<SharedState>>>,
    pub traffic_handle: RwLock<Option<Arc<traffic::TrafficGenerator>>>,
    pub phase: RwLock<DemoPhase>,
    pub source_config: RwLock<Option<SourceDemoConfig>>,
    pub sink_config: RwLock<Option<SinkDemoConfig>>,
    pub selected_tables: RwLock<Vec<String>>,
}

pub async fn run() -> anyhow::Result<()> {
    let state = Arc::new(DemoState {
        engine_state: RwLock::new(None),
        traffic_handle: RwLock::new(None),
        phase: RwLock::new(DemoPhase::Setup),
        source_config: RwLock::new(None),
        sink_config: RwLock::new(None),
        selected_tables: RwLock::new(Vec::new()),
    });

    let app = Router::new()
        .route("/", get(handlers::index))
        .route("/api/datasources/test", post(handlers::test_connection))
        .route("/api/tables/discover", post(handlers::discover_tables))
        .route("/api/replication/start", post(handlers::start_replication))
        .route("/api/replication/stop", post(handlers::stop_replication))
        .route("/api/replication/pause", post(handlers::pause_replication))
        .route("/api/replication/resume", post(handlers::resume_replication))
        .route("/api/traffic/start", post(handlers::start_traffic))
        .route("/api/traffic/stop", post(handlers::stop_traffic))
        .route("/api/status", get(handlers::status))
        .route("/api/metrics/stream", get(handlers::metrics_stream))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let port = std::env::var("DEMO_PORT").unwrap_or_else(|_| "3000".to_string());

    info!(
        r#"
  ╔═══════════════════════════════════════╗
  ║        EZ-CDC Demo Mode               ║
  ║   http://localhost:{}                ║
  ╚═══════════════════════════════════════╝
"#,
        port
    );

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
