mod source;
mod sink;
mod pipeline;
mod state_store;
mod grpc;
mod config;
mod engine;
mod replication;

use anyhow::Result;
use dotenvy::dotenv;

use crate::config::Config;
use crate::engine::CdcEngine;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    dotenv().ok();

    // 1. Cargar configuración
    let config = Config::from_env()?;
    config.print_banner();

    // 2. Crear y ejecutar motor CDC
    let engine = CdcEngine::new(config).await?;
    engine.run().await
}
