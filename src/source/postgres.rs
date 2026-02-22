use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls, Config, CopyBothDuplex};
use bytes::{Bytes, BytesMut, BufMut};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

/// PostgreSQL epoch: 2000-01-01 00:00:00 UTC
/// Difference from Unix epoch in microseconds
const PG_EPOCH_OFFSET_USEC: i64 = 946_684_800_000_000;

/// Generates timestamp in PostgreSQL format (microseconds since 2000-01-01)
pub fn pg_timestamp() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let usec = now.as_micros() as i64;
    usec - PG_EPOCH_OFFSET_USEC
}

/// Builds StandbyStatusUpdate message to confirm LSN to PostgreSQL
///
/// Message format (34 bytes total):
/// - tag: 'r' (1 byte)
/// - walWritePos: u64 - received LSN
/// - walFlushPos: u64 - LSN confirmed to local disk
/// - walApplyPos: u64 - LSN applied to destination (sink)
/// - timestamp: i64 - microseconds since 2000-01-01
/// - reply: u8 - 0 = reply not requested
pub fn build_standby_status_update(lsn: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(34);
    buf.put_u8(b'r');           // StandbyStatusUpdate tag
    buf.put_u64(lsn);           // walWritePos
    buf.put_u64(lsn);           // walFlushPos (same as write)
    buf.put_u64(lsn);           // walApplyPos (confirmed to sink)
    buf.put_i64(pg_timestamp()); // timestamp
    buf.put_u8(0);              // reply not requested
    buf.freeze()
}

pub struct PostgresSource {
    client: Client,
    slot_name: String,
    publication_name: String,
}

impl PostgresSource {
    pub async fn new(
        pg_config: &str,
        slot_name: String,
        publication_name: String,
    ) -> Result<Self> {
        // Clean URL of replication parameters if they exist
        let clean_url = pg_config
            .replace("?replication=database", "")
            .replace("&replication=database", "")
            .replace("replication=database&", "");

        // Step 1: Create replication slot on normal connection (without replication mode)
        {
            let (slot_client, slot_connection) = tokio_postgres::connect(&clean_url, NoTls).await?;
            let slot_handle = tokio::spawn(async move {
                if let Err(e) = slot_connection.await {
                    error!("Slot connection error: {}", e);
                }
            });

            // Try to create the slot (ignore if it already exists)
            let _ = slot_client
                .simple_query(&format!(
                    "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
                    slot_name
                ))
                .await; // Ignore errors (slot may already exist)
            
            drop(slot_client);
            let _ = slot_handle.await;
        }

        // Step 2: Create replication connection
        let mut config: Config = clean_url.parse()?;

        // The Materialize fork has this method
        config.replication_mode(tokio_postgres::config::ReplicationMode::Logical);
        
        let (client, connection) = config.connect(NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Replication connection error: {}", e);
            }
        });

        Ok(Self {
            client,
            slot_name,
            publication_name,
        })
    }

    pub async fn start_replication(&self) -> Result<CopyBothDuplex<Bytes>> {
        self.start_replication_from(0).await
    }

    pub async fn start_replication_from(&self, start_lsn: u64) -> Result<CopyBothDuplex<Bytes>> {
        // Convert LSN to PostgreSQL format (X/Y)
        let lsn_str = if start_lsn == 0 {
            "0/0".to_string()
        } else {
            format!("{:X}/{:X}", start_lsn >> 32, start_lsn & 0xFFFFFFFF)
        };
        
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
            self.slot_name, lsn_str, self.publication_name
        );

        info!("Starting replication from LSN: {}", lsn_str);

        let stream = self
            .client
            .copy_both_simple(&query)
            .await
            .context("Failed to start replication")?;

        Ok(stream)
    }

    /// Validates that tables have REPLICA IDENTITY FULL
    ///
    /// This is critical for StarRocks/ClickHouse because they need all columns
    /// (including partition columns) to perform soft delete INSERTs.
    ///
    /// With REPLICA IDENTITY DEFAULT, only the PK is received in DELETEs, which
    /// is insufficient for partitioned tables.
    pub async fn validate_replica_identity(&self, tables: &[String]) -> Result<()> {
        // Create a normal connection (not replication) for queries
        let clean_url = self.clean_url();
        let (client, connection) = tokio_postgres::connect(&clean_url, NoTls).await?;
        
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Validation connection error: {}", e);
            }
        });

        for table in tables {
            // Parse schema.table if qualified
            let parts: Vec<&str> = table.split('.').collect();
            let table_name = if parts.len() > 1 { parts[1] } else { parts[0] };
            
            let row = client
                .query_one(
                    "SELECT c.relreplident, c.relname 
                     FROM pg_class c 
                     JOIN pg_namespace n ON c.relnamespace = n.oid 
                     WHERE c.relname = $1 AND n.nspname = COALESCE($2, 'public')",
                    &[&table_name, &if parts.len() > 1 { parts[0] } else { "public" }],
                )
                .await
                .with_context(|| format!("Failed to query replica identity for table '{}'", table))?;

            let replica_identity: i8 = row.get(0);
            let relname: String = row.get(1);
            let replica_char = replica_identity as u8 as char;

            match replica_char {
                'f' => {
                    info!("Table '{}' has REPLICA IDENTITY FULL", relname);
                }
                'd' => {
                    warn!("Table '{}' has REPLICA IDENTITY DEFAULT", relname);
                    warn!("    This may cause issues with soft deletes in StarRocks.");
                    warn!("    Run: ALTER TABLE {} REPLICA IDENTITY FULL;", table);
                    // Don't fail, just warn - let the user decide
                }
                'n' => {
                    return Err(anyhow::anyhow!(
                        "Table '{}' has REPLICA IDENTITY NOTHING. \
                        This is not supported for CDC. \
                        Run: ALTER TABLE {} REPLICA IDENTITY FULL;",
                        relname, table
                    ));
                }
                'i' => {
                    info!("Table '{}' has REPLICA IDENTITY INDEX", relname);
                    info!("    Note: For full soft delete support, consider REPLICA IDENTITY FULL");
                }
                _ => {
                    warn!("Unknown REPLICA IDENTITY '{}' for table '{}'", replica_char, relname);
                }
            }
        }

        Ok(())
    }

    /// Gets the clean URL without replication parameters
    fn clean_url(&self) -> String {
        // This function assumes PostgresSource was created with a valid URL
        // In a real scenario, you should store the original URL
        // For now, this is a placeholder that would need the URL from env
        std::env::var("SOURCE_URL")
            .unwrap_or_default()
            .replace("?replication=database", "")
            .replace("&replication=database", "")
            .replace("replication=database&", "")
    }
}
