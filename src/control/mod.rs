mod cpu_metrics;
mod handlers;
pub mod state;

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use tracing::info;

pub use state::{CdcConfig, CdcState, SharedState, Stage};

pub async fn start_control_server(port: u16, shared_state: Arc<SharedState>) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/api/v1/health", get(handlers::health))
        .route("/api/v1/cdc/pause", post(handlers::pause))
        .route("/api/v1/cdc/resume", post(handlers::resume))
        .route("/api/v1/cdc/drain", post(handlers::drain))
        .route("/api/v1/cdc/stop", post(handlers::stop))
        .route("/api/v1/cdc/reload", post(handlers::reload_config))
        .route("/api/v1/cdc/snapshot/start", post(handlers::snapshot_start))
        .route("/api/v1/cdc/snapshot/pause", post(handlers::snapshot_pause))
        .route(
            "/api/v1/cdc/snapshot/resume",
            post(handlers::snapshot_resume),
        )
        .route("/api/v1/cdc/status", get(handlers::status))
        .route("/api/v1/cdc/metrics", get(handlers::metrics))
        .with_state(shared_state);

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
