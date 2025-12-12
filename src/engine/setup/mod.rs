pub mod error;
pub mod postgres;
pub mod starrocks;

use anyhow::Result;

pub use error::SetupError;
use crate::config::Config;

/// Manager principal del proceso de SETUP
pub struct SetupManager {
    config: Config,
}

impl SetupManager {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Ejecutar todo el setup
    pub async fn run(&self) -> Result<(), SetupError> {
        println!("\n═══════════════════════════════════════");
        println!("        SETUP PHASE");
        println!("═══════════════════════════════════════\n");

        // 1. Setup PostgreSQL
        self.setup_postgres().await?;
        
        // 2. Setup StarRocks
        self.setup_starrocks().await?;

        println!("\n═══════════════════════════════════════");
        println!("    ✅ SETUP COMPLETE");
        println!("═══════════════════════════════════════\n");

        Ok(())
    }

    /// Setup de PostgreSQL
    async fn setup_postgres(&self) -> Result<(), SetupError> {
        let client = postgres::create_postgres_client(&self.config.database_url).await?;
        let pg_setup = postgres::PostgresSetup::new(&client, &self.config);
        pg_setup.run().await
    }

    /// Setup de StarRocks
    async fn setup_starrocks(&self) -> Result<(), SetupError> {
        let pool = starrocks::create_starrocks_pool(&self.config)?;
        let sr_setup = starrocks::StarRocksSetup::new(&pool, &self.config);
        sr_setup.run().await
    }
}

