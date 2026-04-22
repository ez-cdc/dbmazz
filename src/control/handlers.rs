use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

use super::state::{CdcState, MetricsSample, SharedState, Stage};

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub stage: &'static str,
    pub stage_detail: String,
    pub error_detail: String,
}

#[derive(Serialize)]
pub struct ControlResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub state: &'static str,
    pub current_lsn: u64,
    pub confirmed_lsn: u64,
    pub pending_events: u64,
    pub slot_name: String,
    pub tables: Vec<String>,
    pub snapshot_active: bool,
    pub snapshot_chunks_total: u64,
    pub snapshot_chunks_done: u64,
    pub snapshot_rows_synced: u64,
    pub table_progress: Vec<TableProgressEntry>,
    pub snapshot_paused: bool,
}

#[derive(Serialize)]
pub struct TableProgressEntry {
    pub table_name: String,
    pub chunks_total: u64,
    pub chunks_done: u64,
    pub rows_synced: u64,
}

#[derive(Deserialize, Default)]
pub struct StopRequest {
    #[serde(default)]
    pub skip_slot_cleanup: bool,
}

#[derive(Deserialize, Default)]
pub struct ReloadConfigRequest {
    #[serde(default)]
    pub flush_size: u64,
    #[serde(default)]
    pub flush_interval_ms: u64,
    #[serde(default)]
    pub tables: Vec<String>,
}

#[derive(Deserialize)]
pub struct MetricsQuery {
    #[serde(default = "default_samples")]
    pub samples: u32,
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
}

fn default_samples() -> u32 {
    1
}
fn default_interval_ms() -> u64 {
    1000
}

pub async fn health(State(state): State<Arc<SharedState>>) -> Json<HealthResponse> {
    let cdc_state = state.state();
    let (stage, stage_detail) = state.stage().await;
    let error_detail = state.setup_error().await.unwrap_or_default();

    let status = if !error_detail.is_empty() {
        "not_serving"
    } else {
        match cdc_state {
            CdcState::Running | CdcState::Paused | CdcState::Draining => "serving",
            CdcState::Stopped => "not_serving",
        }
    };

    Json(HealthResponse {
        status,
        stage: stage_str(stage),
        stage_detail,
        error_detail,
    })
}

pub async fn pause(State(state): State<Arc<SharedState>>) -> Json<ControlResponse> {
    if state.compare_and_set_state(CdcState::Running, CdcState::Paused) {
        state.set_snapshot_paused(true);
        Json(ControlResponse {
            success: true,
            message: "CDC paused successfully".into(),
        })
    } else {
        let current = state.state();
        Json(ControlResponse {
            success: false,
            message: match current {
                CdcState::Paused => "CDC is already paused".into(),
                other => format!("Cannot pause CDC in state: {other:?}"),
            },
        })
    }
}

pub async fn resume(State(state): State<Arc<SharedState>>) -> Json<ControlResponse> {
    if state.compare_and_set_state(CdcState::Paused, CdcState::Running) {
        state.set_snapshot_paused(false);
        Json(ControlResponse {
            success: true,
            message: "CDC resumed successfully".into(),
        })
    } else {
        let current = state.state();
        Json(ControlResponse {
            success: false,
            message: match current {
                CdcState::Running => "CDC is already running".into(),
                other => format!("Cannot resume CDC in state: {other:?}"),
            },
        })
    }
}

pub async fn drain(State(state): State<Arc<SharedState>>) -> Json<ControlResponse> {
    let current = state.state();
    match current {
        CdcState::Running | CdcState::Paused => {
            state.set_state(CdcState::Draining);
            let _ = state.shutdown_tx.send(true);
            Json(ControlResponse {
                success: true,
                message: "CDC is draining and will stop".into(),
            })
        }
        CdcState::Draining => Json(ControlResponse {
            success: false,
            message: "CDC is already draining".into(),
        }),
        CdcState::Stopped => Json(ControlResponse {
            success: false,
            message: "CDC is already stopped".into(),
        }),
    }
}

pub async fn stop(
    State(state): State<Arc<SharedState>>,
    body: Option<Json<StopRequest>>,
) -> Json<ControlResponse> {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let current = state.state();
    if current == CdcState::Stopped {
        return Json(ControlResponse {
            success: false,
            message: "CDC is already stopped".into(),
        });
    }

    state.set_skip_slot_cleanup(req.skip_slot_cleanup);
    state.set_state(CdcState::Stopped);
    let _ = state.shutdown_tx.send(true);

    let message = if req.skip_slot_cleanup {
        "CDC stopped (slot preserved for restart)"
    } else {
        "CDC stopped (slot will be cleaned up)"
    };
    Json(ControlResponse {
        success: true,
        message: message.into(),
    })
}

pub async fn reload_config(
    State(state): State<Arc<SharedState>>,
    Json(req): Json<ReloadConfigRequest>,
) -> Json<ControlResponse> {
    let mut config = state.config.write().await;
    let mut changes = Vec::new();

    if req.flush_size > 0 {
        config.flush_size = req.flush_size as usize;
        changes.push(format!("flush_size={}", req.flush_size));
    }
    if req.flush_interval_ms > 0 {
        config.flush_interval_ms = req.flush_interval_ms;
        changes.push(format!("flush_interval_ms={}", req.flush_interval_ms));
    }
    if !req.tables.is_empty() {
        config.tables = req.tables.clone();
        changes.push(format!("tables={:?}", req.tables));
    }

    if changes.is_empty() {
        Json(ControlResponse {
            success: false,
            message: "No configuration changes provided (use 0 / empty to keep current values)"
                .into(),
        })
    } else {
        Json(ControlResponse {
            success: true,
            message: format!("Configuration reloaded: {}", changes.join(", ")),
        })
    }
}

pub async fn snapshot_start(State(state): State<Arc<SharedState>>) -> Json<ControlResponse> {
    state.trigger_snapshot();
    Json(ControlResponse {
        success: true,
        message: "Snapshot triggered".into(),
    })
}

pub async fn snapshot_pause(State(state): State<Arc<SharedState>>) -> Json<ControlResponse> {
    if !state.is_snapshot_active() {
        return Json(ControlResponse {
            success: true,
            message: "No snapshot running".into(),
        });
    }
    state.set_snapshot_paused(true);
    Json(ControlResponse {
        success: true,
        message: "Snapshot paused".into(),
    })
}

pub async fn snapshot_resume(State(state): State<Arc<SharedState>>) -> Json<ControlResponse> {
    state.set_snapshot_paused(false);
    Json(ControlResponse {
        success: true,
        message: "Snapshot resumed".into(),
    })
}

pub async fn status(State(state): State<Arc<SharedState>>) -> Json<StatusResponse> {
    let cdc_state = state.state();
    let (stage, _) = state.stage().await;
    let config = state.config.read().await;
    let snapshot_active = stage == Stage::Snapshot;

    let table_progress = if snapshot_active {
        state
            .get_table_progress()
            .await
            .into_iter()
            .map(|(table_name, p)| TableProgressEntry {
                table_name,
                chunks_total: p.chunks_total,
                chunks_done: p.chunks_done,
                rows_synced: p.rows_synced,
            })
            .collect()
    } else {
        Vec::new()
    };

    Json(StatusResponse {
        state: state_str(cdc_state),
        current_lsn: state.current_lsn(),
        confirmed_lsn: state.confirmed_lsn(),
        pending_events: state.pending_events(),
        slot_name: config.slot_name.clone(),
        tables: config.tables.clone(),
        snapshot_active,
        snapshot_chunks_total: state.snapshot_chunks_total(),
        snapshot_chunks_done: state.snapshot_chunks_done(),
        snapshot_rows_synced: state.snapshot_rows_synced(),
        table_progress,
        snapshot_paused: state.is_snapshot_paused(),
    })
}

pub async fn metrics(
    State(state): State<Arc<SharedState>>,
    Query(q): Query<MetricsQuery>,
) -> Result<Json<Vec<MetricsSample>>, (StatusCode, String)> {
    if q.interval_ms == 0 {
        return Err((StatusCode::BAD_REQUEST, "interval_ms must be > 0".into()));
    }
    if q.samples == 0 || q.samples > 60 {
        return Err((
            StatusCode::BAD_REQUEST,
            "samples must be between 1 and 60".into(),
        ));
    }

    // Samples are produced asynchronously by the background sampler; the
    // handler returns the most recent snapshot. `samples` still controls
    // the response length for callers that expect an array.
    let latest = state.latest_metrics.read().await.clone();
    Ok(Json(vec![latest; q.samples as usize]))
}

fn state_str(s: CdcState) -> &'static str {
    match s {
        CdcState::Running => "running",
        CdcState::Paused => "paused",
        CdcState::Draining => "draining",
        CdcState::Stopped => "stopped",
    }
}

fn stage_str(s: Stage) -> &'static str {
    match s {
        Stage::Init => "init",
        Stage::Setup => "setup",
        Stage::Cdc => "cdc",
        Stage::Snapshot => "snapshot",
    }
}
