pub mod error;
pub mod postgres;
pub mod starrocks;

use anyhow::Result;
use tracing::info;

pub use error::SetupError;
pub use postgres::cleanup_postgres_resources;
use crate::config::Config;

/// Main manager for the SETUP process
pub struct SetupManager {
    config: Config,
}

impl SetupManager {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Execute complete setup
    pub async fn run(&self) -> Result<(), SetupError> {
        info!("\n═══════════════════════════════════════");
        info!("        SETUP PHASE");
        info!("═══════════════════════════════════════\n");

        // 1. Setup PostgreSQL
        self.setup_postgres().await?;

        // 2. Setup StarRocks
        self.setup_starrocks().await?;

        info!("\n═══════════════════════════════════════");
        info!("    [OK] SETUP COMPLETE");
        info!("═══════════════════════════════════════\n");

        Ok(())
    }

    /// PostgreSQL setup
    async fn setup_postgres(&self) -> Result<(), SetupError> {
        let client = postgres::create_postgres_client(&self.config.database_url).await?;
        let pg_setup = postgres::PostgresSetup::new(&client, &self.config);
        pg_setup.run().await
    }

    /// StarRocks setup
    async fn setup_starrocks(&self) -> Result<(), SetupError> {
        let pool = starrocks::create_starrocks_pool(&self.config)?;
        let sr_setup = starrocks::StarRocksSetup::new(&pool, &self.config);
        sr_setup.run().await
    }
}

