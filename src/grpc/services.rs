use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{interval, Duration};
use tonic::{Request, Response, Status};

use crate::grpc::state::{CdcState, SharedState, Stage};

// Include the generated protobuf code
pub mod dbmazz {
    tonic::include_proto!("dbmazz");
    
    // File descriptor set for reflection
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("descriptor");
}

use dbmazz::{
    health_service_server::{HealthService, HealthServiceServer},
    cdc_control_service_server::{CdcControlService, CdcControlServiceServer},
    cdc_status_service_server::{CdcStatusService, CdcStatusServiceServer},
    cdc_metrics_service_server::{CdcMetricsService, CdcMetricsServiceServer},
    HealthCheckRequest, HealthCheckResponse,
    health_check_response::ServingStatus,
    PauseRequest, ResumeRequest, DrainRequest, StopRequest, ReloadConfigRequest,
    ControlResponse,
    StatusRequest, StatusResponse,
    status_response::CdcState as ProtoCdcState,
    MetricsRequest, MetricsResponse,
};

// ============================================================================
// Health Service
// ============================================================================

pub struct HealthServiceImpl {
    shared_state: Arc<SharedState>,
}

impl HealthServiceImpl {
    pub fn new(shared_state: Arc<SharedState>) -> Self {
        Self { shared_state }
    }
}

#[tonic::async_trait]
impl HealthService for HealthServiceImpl {
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let state = *self.shared_state.state.read().await;
        let (stage, stage_detail) = self.shared_state.get_stage().await;
        
        let status = match state {
            CdcState::Running | CdcState::Paused | CdcState::Draining => ServingStatus::Serving,
            CdcState::Stopped => ServingStatus::NotServing,
        };

        let proto_stage = match stage {
            Stage::Init => 1,    // STAGE_INIT
            Stage::Setup => 2,   // STAGE_SETUP
            Stage::Cdc => 3,     // STAGE_CDC
        };

        Ok(Response::new(HealthCheckResponse {
            status: status as i32,
            stage: proto_stage,
            stage_detail,
        }))
    }
}

pub fn health_service(shared_state: Arc<SharedState>) -> HealthServiceServer<HealthServiceImpl> {
    HealthServiceServer::new(HealthServiceImpl::new(shared_state))
}

// ============================================================================
// Control Service
// ============================================================================

pub struct CdcControlServiceImpl {
    shared_state: Arc<SharedState>,
}

impl CdcControlServiceImpl {
    pub fn new(shared_state: Arc<SharedState>) -> Self {
        Self { shared_state }
    }
}

#[tonic::async_trait]
impl CdcControlService for CdcControlServiceImpl {
    async fn pause(
        &self,
        _request: Request<PauseRequest>,
    ) -> Result<Response<ControlResponse>, Status> {
        let mut state = self.shared_state.state.write().await;
        match *state {
            CdcState::Running => {
                *state = CdcState::Paused;
                Ok(Response::new(ControlResponse {
                    success: true,
                    message: "CDC paused successfully".to_string(),
                }))
            }
            CdcState::Paused => {
                Ok(Response::new(ControlResponse {
                    success: false,
                    message: "CDC is already paused".to_string(),
                }))
            }
            _ => {
                Ok(Response::new(ControlResponse {
                    success: false,
                    message: format!("Cannot pause CDC in state: {:?}", *state),
                }))
            }
        }
    }

    async fn resume(
        &self,
        _request: Request<ResumeRequest>,
    ) -> Result<Response<ControlResponse>, Status> {
        let mut state = self.shared_state.state.write().await;
        match *state {
            CdcState::Paused => {
                *state = CdcState::Running;
                Ok(Response::new(ControlResponse {
                    success: true,
                    message: "CDC resumed successfully".to_string(),
                }))
            }
            CdcState::Running => {
                Ok(Response::new(ControlResponse {
                    success: false,
                    message: "CDC is already running".to_string(),
                }))
            }
            _ => {
                Ok(Response::new(ControlResponse {
                    success: false,
                    message: format!("Cannot resume CDC in state: {:?}", *state),
                }))
            }
        }
    }

    async fn drain_and_stop(
        &self,
        _request: Request<DrainRequest>,
    ) -> Result<Response<ControlResponse>, Status> {
        let mut state = self.shared_state.state.write().await;
        match *state {
            CdcState::Running | CdcState::Paused => {
                *state = CdcState::Draining;
                // Enviar señal de shutdown
                let _ = self.shared_state.shutdown_tx.send(true);
                Ok(Response::new(ControlResponse {
                    success: true,
                    message: "CDC is draining and will stop".to_string(),
                }))
            }
            CdcState::Draining => {
                Ok(Response::new(ControlResponse {
                    success: false,
                    message: "CDC is already draining".to_string(),
                }))
            }
            CdcState::Stopped => {
                Ok(Response::new(ControlResponse {
                    success: false,
                    message: "CDC is already stopped".to_string(),
                }))
            }
        }
    }

    async fn stop(
        &self,
        _request: Request<StopRequest>,
    ) -> Result<Response<ControlResponse>, Status> {
        let mut state = self.shared_state.state.write().await;
        match *state {
            CdcState::Stopped => {
                Ok(Response::new(ControlResponse {
                    success: false,
                    message: "CDC is already stopped".to_string(),
                }))
            }
            _ => {
                *state = CdcState::Stopped;
                // Enviar señal de shutdown inmediato
                let _ = self.shared_state.shutdown_tx.send(true);
                Ok(Response::new(ControlResponse {
                    success: true,
                    message: "CDC stopped immediately".to_string(),
                }))
            }
        }
    }

    async fn reload_config(
        &self,
        request: Request<ReloadConfigRequest>,
    ) -> Result<Response<ControlResponse>, Status> {
        let req = request.into_inner();
        let mut config = self.shared_state.config.write().await;

        let mut changes = Vec::new();

        // 0 significa "no cambiar"
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
            Ok(Response::new(ControlResponse {
                success: false,
                message: "No configuration changes provided (use 0 to keep current values)".to_string(),
            }))
        } else {
            Ok(Response::new(ControlResponse {
                success: true,
                message: format!("Configuration reloaded: {}", changes.join(", ")),
            }))
        }
    }
}

pub fn control_service(
    shared_state: Arc<SharedState>,
) -> CdcControlServiceServer<CdcControlServiceImpl> {
    CdcControlServiceServer::new(CdcControlServiceImpl::new(shared_state))
}

// ============================================================================
// Status Service
// ============================================================================

pub struct CdcStatusServiceImpl {
    shared_state: Arc<SharedState>,
}

impl CdcStatusServiceImpl {
    pub fn new(shared_state: Arc<SharedState>) -> Self {
        Self { shared_state }
    }
}

#[tonic::async_trait]
impl CdcStatusService for CdcStatusServiceImpl {
    async fn get_status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let state = *self.shared_state.state.read().await;
        let config = self.shared_state.config.read().await;

        let proto_state = match state {
            CdcState::Running => ProtoCdcState::Running,
            CdcState::Paused => ProtoCdcState::Paused,
            CdcState::Draining => ProtoCdcState::Draining,
            CdcState::Stopped => ProtoCdcState::Stopped,
        };

        Ok(Response::new(StatusResponse {
            state: proto_state as i32,
            current_lsn: self.shared_state.get_current_lsn(),
            confirmed_lsn: self.shared_state.get_confirmed_lsn(),
            pending_events: self.shared_state.get_pending_events(),
            slot_name: config.slot_name.clone(),
            tables: config.tables.clone(),
        }))
    }
}

pub fn status_service(
    shared_state: Arc<SharedState>,
) -> CdcStatusServiceServer<CdcStatusServiceImpl> {
    CdcStatusServiceServer::new(CdcStatusServiceImpl::new(shared_state))
}

// ============================================================================
// Metrics Service
// ============================================================================

pub struct CdcMetricsServiceImpl {
    shared_state: Arc<SharedState>,
}

impl CdcMetricsServiceImpl {
    pub fn new(shared_state: Arc<SharedState>) -> Self {
        Self { shared_state }
    }
}

#[tonic::async_trait]
impl CdcMetricsService for CdcMetricsServiceImpl {
    type StreamMetricsStream = tokio_stream::wrappers::ReceiverStream<Result<MetricsResponse, Status>>;

    async fn stream_metrics(
        &self,
        request: Request<MetricsRequest>,
    ) -> Result<Response<Self::StreamMetricsStream>, Status> {
        let interval_ms = request.into_inner().interval_ms;
        if interval_ms == 0 {
            return Err(Status::invalid_argument("interval_ms must be > 0"));
        }

        let shared_state = self.shared_state.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(interval_ms as u64));
            let mut last_events = shared_state.get_events_processed();
            let mut last_time = std::time::Instant::now();

            loop {
                ticker.tick().await;

                let current_events = shared_state.get_events_processed();
                let now = std::time::Instant::now();
                let elapsed = now.duration_since(last_time).as_secs_f64();
                
                let events_per_second = if elapsed > 0.0 {
                    (current_events.saturating_sub(last_events)) as f64 / elapsed
                } else {
                    0.0
                };

                last_events = current_events;
                last_time = now;

                let current_lsn = shared_state.get_current_lsn();
                let confirmed_lsn = shared_state.get_confirmed_lsn();
                let lag_bytes = current_lsn.saturating_sub(confirmed_lsn);

                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                let metrics = MetricsResponse {
                    timestamp,
                    events_per_second,
                    lag_bytes,
                    lag_events: shared_state.get_pending_events(),
                    memory_bytes: shared_state.estimate_memory(),
                    total_events_processed: current_events,
                    total_batches_sent: shared_state.get_batches_sent(),
                };

                if tx.send(Ok(metrics)).await.is_err() {
                    // Client disconnected
                    break;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

pub fn metrics_service(
    shared_state: Arc<SharedState>,
) -> CdcMetricsServiceServer<CdcMetricsServiceImpl> {
    CdcMetricsServiceServer::new(CdcMetricsServiceImpl::new(shared_state))
}

