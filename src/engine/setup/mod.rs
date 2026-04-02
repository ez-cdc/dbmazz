pub mod error;
pub mod postgres;

use anyhow::Result;
use tracing::info;

use crate::config::Config;
pub use error::SetupError;
pub use postgres::cleanup_postgres_resources;

/// Main manager for the SETUP process.
///
/// Handles source-side setup only (replication slot, publication, etc.).
/// Sink setup is handled by each sink's `Sink::setup()` implementation.
pub struct SetupManager {
    config: Config,
}

impl SetupManager {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Execute source setup.
    pub async fn run(&self) -> Result<(), SetupError> {
        info!("\n═══════════════════════════════════════");
        info!("        SETUP PHASE (source)");
        info!("═══════════════════════════════════════\n");

        // Setup PostgreSQL source (replication slot, publication, etc.)
        let pg_client = postgres::create_postgres_client(&self.config.database_url).await?;
        let pg_setup = postgres::PostgresSetup::new(&pg_client, &self.config);
        pg_setup.run().await?;

        info!("\n═══════════════════════════════════════");
        info!("    [OK] SOURCE SETUP COMPLETE");
        info!("═══════════════════════════════════════\n");

        Ok(())
    }
}
