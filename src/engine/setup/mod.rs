pub mod error;
pub mod postgres;
pub mod starrocks;

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use mysql_async::prelude::Queryable;
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

    /// Execute complete setup.
    ///
    /// Optimized for many tables (500+):
    /// - Batch checks StarRocks tables in 1 query
    /// - Only queries PG schemas for missing tables (2 queries, or 0 if all exist)
    /// - Batch checks audit columns in 1 query
    /// - Reuses connections (1 PG client, 1 SR connection)
    pub async fn run(&self) -> Result<(), SetupError> {
        info!("\n═══════════════════════════════════════");
        info!("        SETUP PHASE");
        info!("═══════════════════════════════════════\n");

        // 1. Setup PostgreSQL (reuse client for schema queries later)
        let pg_client = postgres::create_postgres_client(&self.config.database_url).await?;
        let pg_setup = postgres::PostgresSetup::new(&pg_client, &self.config);
        pg_setup.run().await?;

        // 2. Batch check which tables exist in StarRocks (1 query)
        let pool = starrocks::create_starrocks_pool(&self.config)?;
        let existing_sr_tables = self.get_existing_sr_tables(&pool).await?;

        let missing: Vec<String> = self.config.tables.iter()
            .filter(|t| {
                let name = t.split('.').next_back().unwrap_or(t);
                !existing_sr_tables.contains(name)
            })
            .cloned()
            .collect();

        // 3. Only query PG schemas for tables that don't exist in StarRocks (2 queries or 0)
        let pg_schemas = if missing.is_empty() {
            info!("  All {} tables already exist in StarRocks", self.config.tables.len());
            HashMap::new()
        } else {
            info!("  {} of {} tables missing in StarRocks, querying source schemas...",
                missing.len(), self.config.tables.len());
            postgres::get_table_schemas_batch(&pg_client, &missing).await?
        };

        // 4. Run StarRocks setup: create missing + batch ensure audit columns
        let sr_setup = starrocks::StarRocksSetup::new(&pool, &self.config);
        sr_setup.run(&pg_schemas, &existing_sr_tables).await?;

        info!("\n═══════════════════════════════════════");
        info!("    [OK] SETUP COMPLETE");
        info!("═══════════════════════════════════════\n");

        Ok(())
    }

    /// Get all existing table names in the StarRocks database (1 query).
    async fn get_existing_sr_tables(&self, pool: &mysql_async::Pool) -> Result<HashSet<String>, SetupError> {
        let mut conn = pool.get_conn().await.map_err(|e| SetupError::SrConnectionFailed {
            host: self.config.starrocks_url.clone(),
            error: e.to_string(),
        })?;
        let rows: Vec<(String,)> = conn
            .exec(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                (&self.config.starrocks_db,),
            )
            .await
            .map_err(|e| SetupError::SrConnectionFailed {
                host: self.config.starrocks_url.clone(),
                error: e.to_string(),
            })?;
        Ok(rows.into_iter().map(|(n,)| n).collect())
    }
}
