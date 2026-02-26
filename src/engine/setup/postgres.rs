use anyhow::Result;
use tokio_postgres::{Client, NoTls};
use tracing::{info, warn};

use super::error::SetupError;
use crate::config::Config;
use crate::utils::validate_sql_identifier;

/// Extract a detailed error message from a tokio_postgres error.
/// tokio_postgres::Error::Display only prints the error kind (e.g. "db error")
/// without the actual PostgreSQL message. This function extracts the full detail.
fn pg_error_message(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        let mut msg = format!("{}: {}", db_err.severity(), db_err.message());
        if let Some(detail) = db_err.detail() {
            msg.push_str(&format!(" DETAIL: {}", detail));
        }
        if let Some(hint) = db_err.hint() {
            msg.push_str(&format!(" HINT: {}", hint));
        }
        msg
    } else {
        e.to_string()
    }
}

pub struct PostgresSetup<'a> {
    client: &'a Client,
    config: &'a Config,
}

impl<'a> PostgresSetup<'a> {
    pub fn new(client: &'a Client, config: &'a Config) -> Self {
        Self { client, config }
    }

    /// Execute complete PostgreSQL setup
    pub async fn run(&self) -> Result<(), SetupError> {
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
    async fn verify_tables_exist(&self) -> Result<(), SetupError> {
        for table in &self.config.tables {
            let parts: Vec<&str> = table.split('.').collect();
            let schema = if parts.len() > 1 { parts[0] } else { "public" };
            let table_name = if parts.len() > 1 { parts[1] } else { parts[0] };

            let exists: bool = self.client
                .query_one(
                    "SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = $1 AND table_name = $2
                    )",
                    &[&schema, &table_name],
                )
                .await
                .map_err(|e| SetupError::PgConnectionFailed {
                    host: "PostgreSQL".to_string(),
                    error: pg_error_message(&e),
                })?
                .get(0);

            if !exists {
                return Err(SetupError::PgTableNotFound {
                    table: table.clone(),
                });
            }

            info!("  [OK] Table {} exists", table);
        }
        Ok(())
    }

    /// Configure REPLICA IDENTITY FULL on all tables
    async fn ensure_replica_identity(&self) -> Result<(), SetupError> {
        for table in &self.config.tables {
            // Validate table name to prevent SQL injection
            validate_sql_identifier(table).map_err(|e| SetupError::PgConnectionFailed {
                host: "PostgreSQL".to_string(),
                error: format!("Invalid table name: {}", e),
            })?;

            let parts: Vec<&str> = table.split('.').collect();
            let schema = if parts.len() > 1 { parts[0] } else { "public" };
            let table_name = if parts.len() > 1 { parts[1] } else { parts[0] };

            // Consultar estado actual
            let row = self.client
                .query_one(
                    "SELECT c.relreplident
                     FROM pg_class c
                     JOIN pg_namespace n ON c.relnamespace = n.oid
                     WHERE c.relname = $1 AND n.nspname = $2",
                    &[&table_name, &schema],
                )
                .await
                .map_err(|e| SetupError::PgConnectionFailed {
                    host: "PostgreSQL".to_string(),
                    error: pg_error_message(&e),
                })?;

            let replica_identity: i8 = row.get(0);
            let identity_char = replica_identity as u8 as char;

            // If not FULL, configure it
            if identity_char != 'f' {
                info!("  Setting REPLICA IDENTITY FULL on {}", table);
                self.client
                    .execute(
                        &format!("ALTER TABLE {} REPLICA IDENTITY FULL", table),
                        &[],
                    )
                    .await
                    .map_err(|e| SetupError::PgReplicaIdentityFailed {
                        table: table.clone(),
                        error: pg_error_message(&e),
                    })?;
                info!("  [OK] REPLICA IDENTITY FULL set on {}", table);
            } else {
                info!("  [OK] {} already has REPLICA IDENTITY FULL", table);
            }
        }
        Ok(())
    }

    /// Create/verify Publication
    async fn ensure_publication(&self) -> Result<(), SetupError> {
        let pub_name = &self.config.publication_name;

        // Validate publication name to prevent SQL injection
        validate_sql_identifier(pub_name).map_err(|e| SetupError::PgPublicationFailed {
            name: pub_name.clone(),
            error: format!("Invalid publication name: {}", e),
        })?;

        // Check if it exists
        let exists: bool = self.client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
                &[&pub_name],
            )
            .await
            .map_err(|e| SetupError::PgPublicationFailed {
                name: pub_name.clone(),
                error: pg_error_message(&e),
            })?
            .get(0);

        if exists {
            info!("  [OK] Publication {} exists", pub_name);

            // Verify that it includes all tables
            let missing = self.get_missing_tables_in_publication(pub_name).await?;

            for table in missing {
                // Validate each table name before adding
                validate_sql_identifier(&table).map_err(|e| SetupError::PgPublicationFailed {
                    name: pub_name.clone(),
                    error: format!("Invalid table name '{}': {}", table, e),
                })?;

                info!("  Adding {} to publication {}", table, pub_name);
                self.client
                    .execute(
                        &format!("ALTER PUBLICATION {} ADD TABLE {}", pub_name, table),
                        &[],
                    )
                    .await
                    .map_err(|e| SetupError::PgPublicationFailed {
                        name: pub_name.clone(),
                        error: pg_error_message(&e),
                    })?;
                info!("  [OK] Table {} added to publication", table);
            }
        } else {
            // Validate all table names before creating publication
            for table in &self.config.tables {
                validate_sql_identifier(table).map_err(|e| SetupError::PgPublicationFailed {
                    name: pub_name.clone(),
                    error: format!("Invalid table name '{}': {}", table, e),
                })?;
            }

            // Create new publication
            info!("  Creating publication {}", pub_name);
            let tables = self.config.tables.join(", ");
            self.client
                .execute(
                    &format!("CREATE PUBLICATION {} FOR TABLE {}", pub_name, tables),
                    &[],
                )
                .await
                .map_err(|e| SetupError::PgPublicationFailed {
                    name: pub_name.clone(),
                    error: pg_error_message(&e),
                })?;
            info!("  [OK] Publication {} created", pub_name);
        }

        Ok(())
    }

    /// Get tables missing from the publication
    async fn get_missing_tables_in_publication(
        &self,
        pub_name: &str,
    ) -> Result<Vec<String>, SetupError> {
        let rows = self.client
            .query(
                "SELECT schemaname || '.' || tablename as full_name
                 FROM pg_publication_tables 
                 WHERE pubname = $1",
                &[&pub_name],
            )
            .await
            .map_err(|e| SetupError::PgPublicationFailed {
                name: pub_name.to_string(),
                error: pg_error_message(&e),
            })?;

        let existing: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
        
        let missing: Vec<String> = self.config.tables
            .iter()
            .filter(|table| {
                // Normalize names for comparison
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
    async fn ensure_replication_slot(&self) -> Result<(), SetupError> {
        let slot_name = &self.config.slot_name;

        // Check if it exists and if it's active
        let slot_info = self.client
            .query_opt(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name],
            )
            .await
            .map_err(|e| SetupError::PgSlotFailed {
                name: slot_name.clone(),
                error: pg_error_message(&e),
            })?;

        match slot_info {
            Some(row) => {
                let is_active: bool = row.get(0);
                if is_active {
                    // Slot exists and is active - another process is using it
                    // This is recovery mode (same daemon restarting)
                    info!("  [OK] Replication slot {} exists and is active (recovery mode)", slot_name);
                } else {
                    // Slot exists but is NOT active - orphaned from a previous run
                    // Drop it and recreate to ensure clean state
                    warn!("  Replication slot {} exists but is inactive (orphaned), dropping...", slot_name);
                    self.client
                        .execute(
                            "SELECT pg_drop_replication_slot($1)",
                            &[&slot_name],
                        )
                        .await
                        .map_err(|e| SetupError::PgSlotFailed {
                            name: slot_name.clone(),
                            error: format!("failed to drop orphaned slot: {}", pg_error_message(&e)),
                        })?;
                    info!("  Creating replication slot {}", slot_name);
                    self.client
                        .execute(
                            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                            &[&slot_name],
                        )
                        .await
                        .map_err(|e| SetupError::PgSlotFailed {
                            name: slot_name.clone(),
                            error: pg_error_message(&e),
                        })?;
                    info!("  [OK] Replication slot {} created", slot_name);

                    // Clear stale snapshot state — the old slot's progress is invalid
                    self.clear_snapshot_state(slot_name).await;
                }
            }
            None => {
                // Slot doesn't exist - create it
                info!("  Creating replication slot {}", slot_name);
                self.client
                    .execute(
                        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                        &[&slot_name],
                    )
                    .await
                    .map_err(|e| SetupError::PgSlotFailed {
                        name: slot_name.clone(),
                        error: pg_error_message(&e),
                    })?;
                info!("  [OK] Replication slot {} created", slot_name);

                // Clear stale snapshot state — new slot means fresh start
                self.clear_snapshot_state(slot_name).await;
            }
        }

        Ok(())
    }

    /// Clear snapshot state for a slot when the slot is (re)created.
    /// A new replication slot means the snapshot must run from scratch;
    /// stale COMPLETE records would cause the snapshot worker to skip all chunks.
    async fn clear_snapshot_state(&self, slot_name: &str) {
        match self.client
            .execute(
                "DELETE FROM dbmazz_snapshot_state WHERE slot_name = $1",
                &[&slot_name],
            )
            .await
        {
            Ok(n) if n > 0 => {
                info!("  [OK] Cleared {} stale snapshot state rows for slot {}", n, slot_name);
            }
            Ok(_) => {} // No rows to clear
            Err(_) => {} // Table may not exist yet, that's fine
        }
    }
}

/// Helper to create normal PostgreSQL client (non-replication)
pub async fn create_postgres_client(database_url: &str) -> Result<Client, SetupError> {
    // Remove replication parameter for normal connection
    let clean_url = database_url
        .replace("?replication=database", "")
        .replace("&replication=database", "")
        .replace("replication=database&", "");

    let (client, connection) = tokio_postgres::connect(&clean_url, NoTls)
        .await
        .map_err(|e| SetupError::PgConnectionFailed {
            host: "PostgreSQL".to_string(),
            error: pg_error_message(&e),
        })?;

    // Spawn connection in background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            info!("PostgreSQL setup connection error: {}", e);
        }
    });

    Ok(client)
}

/// Cleanup PostgreSQL resources on daemon shutdown
/// Drops the replication slot to free the resource
pub async fn cleanup_postgres_resources(database_url: &str, slot_name: &str) -> Result<(), SetupError> {
    info!("Cleaning up PostgreSQL resources...");

    let client = create_postgres_client(database_url).await?;

    // Check if slot exists and get info
    let slot_info = client
        .query_opt(
            "SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|e| SetupError::PgSlotFailed {
            name: slot_name.to_string(),
            error: pg_error_message(&e),
        })?;

    match slot_info {
        Some(row) => {
            let is_active: bool = row.get(0);
            let active_pid: Option<i32> = row.get(1);

            info!("  Dropping replication slot: {}", slot_name);

            // If slot is active, first terminate the backend that holds it
            if is_active {
                if let Some(pid) = active_pid {
                    warn!("  Slot is active (pid={}), terminating backend...", pid);
                    let terminated: bool = client
                        .query_one(
                            "SELECT pg_terminate_backend($1)",
                            &[&pid],
                        )
                        .await
                        .map(|row| row.get(0))
                        .unwrap_or(false);

                    if terminated {
                        info!("  [OK] Backend terminated");
                        // Wait a moment for PostgreSQL to release the slot
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    } else {
                        warn!("  Could not terminate backend, will retry drop anyway");
                    }
                }
            }

            // Try to drop the slot with retry
            let mut retries = 3;
            let mut last_error = None;

            while retries > 0 {
                match client
                    .execute(
                        "SELECT pg_drop_replication_slot($1)",
                        &[&slot_name],
                    )
                    .await
                {
                    Ok(_) => {
                        info!("  [OK] Replication slot {} dropped", slot_name);
                        return Ok(());
                    }
                    Err(e) => {
                        last_error = Some(pg_error_message(&e));
                        retries -= 1;
                        if retries > 0 {
                            warn!("  Drop failed, retrying in 1s... ({} retries left)", retries);
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            }

            // If all retries failed
            Err(SetupError::PgSlotFailed {
                name: slot_name.to_string(),
                error: format!("failed to drop slot after retries: {}", last_error.unwrap_or_default()),
            })
        }
        None => {
            info!("  [INFO] Replication slot {} not found (already dropped?)", slot_name);
            Ok(())
        }
    }
}
