pub mod error;
pub mod postgres;
pub mod starrocks;

use anyhow::Result;
use tracing::info;

use crate::config::{Config, SinkType};
pub use error::SetupError;
pub use postgres::cleanup_postgres_resources;

/// Main manager for the SETUP process
pub struct SetupManager {
    config: Config,
}

impl SetupManager {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Execute complete setup.
    pub async fn run(&self) -> Result<(), SetupError> {
        info!("\n═══════════════════════════════════════");
        info!("        SETUP PHASE");
        info!("═══════════════════════════════════════\n");

        // 1. Setup PostgreSQL source (always — replication slot, publication, etc.)
        let pg_client = postgres::create_postgres_client(&self.config.database_url).await?;
        let pg_setup = postgres::PostgresSetup::new(&pg_client, &self.config);
        pg_setup.run().await?;

        // 2. Setup sink (conditional by sink type)
        match self.config.sink.sink_type {
            SinkType::StarRocks => {
                let pool = starrocks::create_starrocks_pool(&self.config)?;
                let sr_setup = starrocks::StarRocksSetup::new(&pool, &self.config);
                sr_setup.run().await?;
            }
            SinkType::Postgres => {
                // PostgresSink handles its own setup via Sink::setup()
                info!("  [SKIP] Sink setup deferred to PostgresSink::setup()");
            }
        }

        info!("\n═══════════════════════════════════════");
        info!("    [OK] SETUP COMPLETE");
        info!("═══════════════════════════════════════\n");

        Ok(())
    }
}
