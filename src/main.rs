// Copyright 2025
// Licensed under the Elastic License v2.0

#![warn(clippy::all)]

mod config;
mod connectors;
mod control;
mod core;
mod engine;
mod pipeline;
mod replication;
mod source;
mod state_store;
mod utils;

use anyhow::Result;
use dotenvy::dotenv;

use crate::config::Config;
use crate::engine::CdcEngine;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    dotenv().ok();

    let config = Config::from_env()?;
    config.print_banner();
    let engine = CdcEngine::new(config).await?;
    engine.run().await
}
