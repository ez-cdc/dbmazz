// Copyright 2025
// Licensed under the Elastic License v2.0

#![warn(clippy::all)]

use anyhow::Result;
use dotenvy::dotenv;

use dbmazz::config::Config;
use dbmazz::engine::CdcEngine;

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
