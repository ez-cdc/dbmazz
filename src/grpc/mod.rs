pub mod state;
mod services;

use tonic::transport::Server;
use tonic_reflection::server::Builder as ReflectionBuilder;
use std::sync::Arc;
use state::SharedState;
use services::{health_service, control_service, status_service, metrics_service};

pub use state::{CdcState, CdcConfig, Stage};

/// Inicia el servidor gRPC en el puerto especificado
pub async fn start_grpc_server(
    port: u16,
    shared_state: Arc<SharedState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", port).parse()?;
    
    println!("🚀 gRPC server listening on {}", addr);

    // Configurar servicio de reflection para que grpcurl funcione sin .proto
    let reflection_service = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(services::dbmazz::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    Server::builder()
        .add_service(reflection_service)
        .add_service(health_service(shared_state.clone()))
        .add_service(control_service(shared_state.clone()))
        .add_service(status_service(shared_state.clone()))
        .add_service(metrics_service(shared_state.clone()))
        .serve(addr)
        .await?;

    Ok(())
}

