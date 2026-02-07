// Copyright 2025
// Licensed under the Elastic License v2.0

//! PostgreSQL CDC setup and teardown utilities
//!
//! This module provides functions for:
//! - Creating and verifying publications
//! - Creating and verifying replication slots
//! - Setting REPLICA IDENTITY on tables
//! - Cleanup of PostgreSQL resources
//!
//! # Prerequisites
//! The PostgreSQL user must have:
//! - REPLICATION role or be superuser
//! - CREATE privilege on the database (for replication slots)
//! - Ownership or ALTER privilege on tables (for REPLICA IDENTITY)

use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};
use tracing::{error, info, warn};

use crate::utils::validate_sql_identifier;

/// PostgreSQL setup manager for CDC
pub struct PostgresSetup<'a> {
    client: &'a Client,
    tables: &'a [String],
    slot_name: &'a str,
    publication_name: &'a str,
}

impl<'a> PostgresSetup<'a> {
    /// Create a new PostgresSetup instance
    pub fn new(
        client: &'a Client,
        tables: &'a [String],
        slot_name: &'a str,
        publication_name: &'a str,
    ) -> Self {
        Self {
            client,
            tables,
            slot_name,
            publication_name,
        }
    }

    /// Execute complete PostgreSQL setup
    pub async fn run(&self) -> Result<()> {
        info!("PostgreSQL Setup:");

        // 1. Verify that tables exist
        self.verify_tables_exist().await?;

        // 2. Configure REPLICA IDENTITY FULL
        self.ensure_replica_identity().await?;

        // 3. Create/verify Publication
        self.ensure_publication().await?;

        // 4. Create/verify Replication Slot
        self.ensure_replication_slot().await?;

        info!("[OK] PostgreSQL setup complete");
        Ok(())
    }

    /// Verify that all tables exist
    async fn verify_tables_exist(&self) -> Result<()> {
        for table in self.tables {
            let (schema, table_name) = parse_table_name(table);

            let exists: bool = self
                .client
                .query_one(
                    "SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = $1 AND table_name = $2
                    )",
                    &[&schema, &table_name],
                )
                .await
                .context("Failed to check table existence")?
                .get(0);

            if !exists {
                anyhow::bail!("Table '{}' not found in PostgreSQL", table);
            }

            info!("  [OK] Table {} exists", table);
        }
        Ok(())
    }

    /// Configure REPLICA IDENTITY FULL on all tables
    async fn ensure_replica_identity(&self) -> Result<()> {
        for table in self.tables {
            // Validate table name to prevent SQL injection
            validate_sql_identifier(table)
                .with_context(|| format!("Invalid table name: {}", table))?;

            let (schema, table_name) = parse_table_name(table);

            let row = self
                .client
                .query_one(
                    "SELECT c.relreplident
                     FROM pg_class c
                     JOIN pg_namespace n ON c.relnamespace = n.oid
                     WHERE c.relname = $1 AND n.nspname = $2",
                    &[&table_name, &schema],
                )
                .await
                .context("Failed to query replica identity")?;

            let replica_identity: i8 = row.get(0);
            let identity_char = replica_identity as u8 as char;

            if identity_char != 'f' {
                info!("  Setting REPLICA IDENTITY FULL on {}", table);
                self.client
                    .execute(
                        &format!("ALTER TABLE {} REPLICA IDENTITY FULL", table),
                        &[],
                    )
                    .await
                    .with_context(|| {
                        format!("Failed to set REPLICA IDENTITY FULL on {}", table)
                    })?;
                info!("  [OK] REPLICA IDENTITY FULL set on {}", table);
            } else {
                info!("  [OK] {} already has REPLICA IDENTITY FULL", table);
            }
        }
        Ok(())
    }

    /// Create/verify Publication
    async fn ensure_publication(&self) -> Result<()> {
        // Validate publication name to prevent SQL injection
        validate_sql_identifier(self.publication_name)
            .with_context(|| format!("Invalid publication name: {}", self.publication_name))?;

        // Check if publication exists
        let exists: bool = self
            .client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
                &[&self.publication_name],
            )
            .await
            .context("Failed to check publication existence")?
            .get(0);

        if exists {
            info!("  [OK] Publication {} exists", self.publication_name);

            // Verify that it includes all tables
            let missing = self.get_missing_tables_in_publication().await?;

            for table in missing {
                // Validate each table name before adding
                validate_sql_identifier(&table)
                    .with_context(|| format!("Invalid table name: {}", table))?;

                info!(
                    "  Adding {} to publication {}",
                    table, self.publication_name
                );
                self.client
                    .execute(
                        &format!(
                            "ALTER PUBLICATION {} ADD TABLE {}",
                            self.publication_name, table
                        ),
                        &[],
                    )
                    .await
                    .with_context(|| format!("Failed to add {} to publication", table))?;
                info!("  [OK] Table {} added to publication", table);
            }
        } else {
            // Validate all table names before creating publication
            for table in self.tables {
                validate_sql_identifier(table)
                    .with_context(|| format!("Invalid table name: {}", table))?;
            }

            // Create new publication
            info!("  Creating publication {}", self.publication_name);
            let tables = self.tables.join(", ");
            self.client
                .execute(
                    &format!(
                        "CREATE PUBLICATION {} FOR TABLE {}",
                        self.publication_name, tables
                    ),
                    &[],
                )
                .await
                .with_context(|| {
                    format!("Failed to create publication {}", self.publication_name)
                })?;
            info!("  [OK] Publication {} created", self.publication_name);
        }

        Ok(())
    }

    /// Get tables missing from the publication
    async fn get_missing_tables_in_publication(&self) -> Result<Vec<String>> {
        let rows = self
            .client
            .query(
                "SELECT schemaname || '.' || tablename as full_name
                 FROM pg_publication_tables
                 WHERE pubname = $1",
                &[&self.publication_name],
            )
            .await
            .context("Failed to query publication tables")?;

        let existing: Vec<String> = rows.iter().map(|row| row.get(0)).collect();

        let missing: Vec<String> = self
            .tables
            .iter()
            .filter(|table| {
                let normalized = if table.contains('.') {
                    table.to_string()
                } else {
                    format!("public.{}", table)
                };
                !existing.contains(&normalized) && !existing.contains(table)
            })
            .cloned()
            .collect();

        Ok(missing)
    }

    /// Create/verify Replication Slot
    async fn ensure_replication_slot(&self) -> Result<()> {
        let slot_info = self
            .client
            .query_opt(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.slot_name],
            )
            .await
            .context("Failed to query replication slot")?;

        match slot_info {
            Some(row) => {
                let is_active: bool = row.get(0);
                if is_active {
                    info!(
                        "  [OK] Replication slot {} exists and is active (recovery mode)",
                        self.slot_name
                    );
                } else {
                    // Orphaned slot - drop and recreate
                    info!(
                        "  [WARN] Replication slot {} exists but is inactive, dropping...",
                        self.slot_name
                    );
                    self.client
                        .execute("SELECT pg_drop_replication_slot($1)", &[&self.slot_name])
                        .await
                        .context("Failed to drop orphaned replication slot")?;

                    self.create_replication_slot().await?;
                }
            }
            None => {
                self.create_replication_slot().await?;
            }
        }

        Ok(())
    }

    /// Create a new replication slot
    async fn create_replication_slot(&self) -> Result<()> {
        info!("  Creating replication slot {}", self.slot_name);
        self.client
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&self.slot_name],
            )
            .await
            .with_context(|| format!("Failed to create replication slot {}", self.slot_name))?;
        info!("  [OK] Replication slot {} created", self.slot_name);
        Ok(())
    }
}

/// Parse a table name into schema and table components
fn parse_table_name(table: &str) -> (&str, &str) {
    let parts: Vec<&str> = table.split('.').collect();
    if parts.len() > 1 {
        (parts[0], parts[1])
    } else {
        ("public", parts[0])
    }
}

/// Create a normal PostgreSQL client (non-replication)
pub async fn create_postgres_client(database_url: &str) -> Result<Client> {
    // Remove replication parameter for normal connection
    let clean_url = database_url
        .replace("?replication=database", "")
        .replace("&replication=database", "")
        .replace("replication=database&", "");

    let (client, connection) = tokio_postgres::connect(&clean_url, NoTls)
        .await
        .context("Failed to connect to PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PostgreSQL setup connection error: {}", e);
        }
    });

    Ok(client)
}

/// Cleanup PostgreSQL resources on daemon shutdown
///
/// Drops the replication slot to free the resource. This should be
/// called during graceful shutdown to prevent slot accumulation.
pub async fn cleanup_postgres_resources(database_url: &str, slot_name: &str) -> Result<()> {
    info!("Cleaning up PostgreSQL resources...");

    let client = create_postgres_client(database_url).await?;

    // Check if slot exists
    let slot_info = client
        .query_opt(
            "SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .context("Failed to query replication slot")?;

    match slot_info {
        Some(row) => {
            let is_active: bool = row.get(0);
            let active_pid: Option<i32> = row.get(1);

            info!("  Dropping replication slot: {}", slot_name);

            // If slot is active, first terminate the backend
            if is_active {
                if let Some(pid) = active_pid {
                    info!(
                        "  [WARN] Slot is active (pid={}), terminating backend...",
                        pid
                    );
                    let terminated: bool = client
                        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
                        .await
                        .map(|row| row.get(0))
                        .unwrap_or(false);

                    if terminated {
                        info!("  [OK] Backend terminated");
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    } else {
                        info!("  [WARN] Could not terminate backend");
                    }
                }
            }

            // Try to drop the slot with retry
            let mut retries = 3;
            let mut last_error = None;

            while retries > 0 {
                match client
                    .execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
                    .await
                {
                    Ok(_) => {
                        info!("  [OK] Replication slot {} dropped", slot_name);
                        return Ok(());
                    }
                    Err(e) => {
                        last_error = Some(e.to_string());
                        retries -= 1;
                        if retries > 0 {
                            info!(
                                "  [WARN] Drop failed, retrying in 1s... ({} retries left)",
                                retries
                            );
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            }

            anyhow::bail!(
                "Failed to drop replication slot after retries: {}",
                last_error.unwrap_or_default()
            );
        }
        None => {
            info!(
                "  [INFO] Replication slot {} not found (already dropped?)",
                slot_name
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_table_name() {
        let (schema, table) = parse_table_name("public.orders");
        assert_eq!(schema, "public");
        assert_eq!(table, "orders");

        let (schema, table) = parse_table_name("orders");
        assert_eq!(schema, "public");
        assert_eq!(table, "orders");

        let (schema, table) = parse_table_name("myschema.mytable");
        assert_eq!(schema, "myschema");
        assert_eq!(table, "mytable");
    }
}
