// Copyright 2025
// Licensed under the Elastic License v2.0

#![warn(clippy::all)]

mod config;
mod connectors;
mod core;
#[cfg(feature = "demo")]
mod demo;
mod engine;
#[cfg(feature = "http-api")]
mod http_api;
mod grpc;
mod pipeline;
mod replication;
mod sink;
mod source;
mod state_store;
mod utils;

use anyhow::Result;
use dotenvy::dotenv;
#[cfg(feature = "http-api")]
use tracing::{error, info};

use crate::config::Config;
use crate::engine::CdcEngine;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    dotenv().ok();

    #[cfg(feature = "demo")]
    {
        if std::env::var("DEMO_MODE").unwrap_or_default() == "true" {
            return demo::run().await;
        }
    }

    // When http-api is enabled, support two modes:
    //   1. Auto-start: env vars present → start engine + HTTP dashboard
    //   2. Setup mode: env vars missing → start HTTP server only, configure from browser
    #[cfg(feature = "http-api")]
    {
        let http_port: u16 = std::env::var("HTTP_API_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse()
            .unwrap_or(8080);

        match Config::from_env() {
            Ok(config) => {
                config.print_banner();
                let engine = CdcEngine::new(config);
                let shared = engine.shared_state();

                tokio::spawn(async move {
                    if let Err(e) = http_api::start_http_server(http_port, Some(shared)).await {
                        error!("HTTP API server error: {}", e);
                    }
                });

                return engine.run().await;
            }
            Err(_) => {
                info!("No database configuration found — starting in setup mode");
                info!("Open http://0.0.0.0:{} to configure datasources", http_port);
                return http_api::start_http_server(http_port, None)
                    .await
                    .map_err(Into::into);
            }
        }
    }

    // Without http-api feature: require env vars (original behavior)
    #[cfg(not(feature = "http-api"))]
    {
        let config = Config::from_env()?;
        config.print_banner();
        let engine = CdcEngine::new(config);
        engine.run().await
    }
}
