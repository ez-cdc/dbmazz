// Copyright 2025
// Licensed under the Elastic License v2.0

mod config;
mod connectors;
mod core;
mod engine;
mod grpc;
mod pipeline;
mod replication;
mod sink;
mod source;
mod state_store;

use anyhow::Result;
use dotenvy::dotenv;

use crate::config::Config;
use crate::engine::CdcEngine;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    dotenv().ok();

    // 1. Load configuration
    let config = Config::from_env()?;
    config.print_banner();

    // 2. Create and run CDC engine
    let engine = CdcEngine::new(config).await?;
    engine.run().await
}
