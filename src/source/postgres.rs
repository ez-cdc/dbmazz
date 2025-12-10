use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls, Config, CopyBothDuplex};
use bytes::{Bytes, BytesMut, BufMut};
use std::time::{SystemTime, UNIX_EPOCH};

/// PostgreSQL epoch: 2000-01-01 00:00:00 UTC
/// Difference from Unix epoch in microseconds
const PG_EPOCH_OFFSET_USEC: i64 = 946_684_800_000_000;

/// Genera timestamp en formato PostgreSQL (microsegundos desde 2000-01-01)
pub fn pg_timestamp() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let usec = now.as_micros() as i64;
    usec - PG_EPOCH_OFFSET_USEC
}

/// Construye mensaje StandbyStatusUpdate para confirmar LSN a PostgreSQL
/// 
/// Formato del mensaje (34 bytes total):
/// - tag: 'r' (1 byte)
/// - walWritePos: u64 - LSN recibido
/// - walFlushPos: u64 - LSN confirmado en disco local
/// - walApplyPos: u64 - LSN aplicado al destino (sink)
/// - timestamp: i64 - microsegundos desde 2000-01-01
/// - reply: u8 - 0 = no necesita respuesta
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
        // Limpiar URL de parámetros de replicación si existen
        let clean_url = pg_config
            .replace("?replication=database", "")
            .replace("&replication=database", "")
            .replace("replication=database&", "");
        
        // Paso 1: Crear slot de replicación en conexión normal (sin modo replicación)
        {
            let (slot_client, slot_connection) = tokio_postgres::connect(&clean_url, NoTls).await?;
            let slot_handle = tokio::spawn(async move {
                if let Err(e) = slot_connection.await {
                    eprintln!("Slot connection error: {}", e);
                }
            });

            // Intentar crear el slot (ignorar si ya existe)
            let _ = slot_client
                .simple_query(&format!(
                    "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
                    slot_name
                ))
                .await; // Ignorar errores (slot puede ya existir)
            
            drop(slot_client);
            let _ = slot_handle.await;
        }
        
        // Paso 2: Crear conexión de replicación
        let mut config: Config = clean_url.parse()?;
        
        // ✅ El fork de Materialize SÍ tiene este método
        config.replication_mode(tokio_postgres::config::ReplicationMode::Logical);
        
        let (client, connection) = config.connect(NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Replication connection error: {}", e);
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
        // Convertir LSN a formato PostgreSQL (X/Y)
        let lsn_str = if start_lsn == 0 {
            "0/0".to_string()
        } else {
            format!("{:X}/{:X}", start_lsn >> 32, start_lsn & 0xFFFFFFFF)
        };
        
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
            self.slot_name, lsn_str, self.publication_name
        );

        println!("Starting replication from LSN: {}", lsn_str);

        let stream = self
            .client
            .copy_both_simple(&query)
            .await
            .context("Failed to start replication")?;

        Ok(stream)
    }

    /// Valida que las tablas tengan REPLICA IDENTITY FULL
    /// 
    /// Esto es crítico para StarRocks/ClickHouse porque necesitan todas las columnas
    /// (incluyendo columnas de partición) para hacer INSERTs de soft deletes.
    /// 
    /// Con REPLICA IDENTITY DEFAULT, solo se recibe la PK en DELETEs, lo cual
    /// es insuficiente para tablas particionadas.
    pub async fn validate_replica_identity(&self, tables: &[String]) -> Result<()> {
        // Crear una conexión normal (no de replicación) para consultas
        let clean_url = self.get_clean_url();
        let (client, connection) = tokio_postgres::connect(&clean_url, NoTls).await?;
        
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Validation connection error: {}", e);
            }
        });

        for table in tables {
            // Parsear schema.table si está calificado
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
                    println!("✅ Table '{}' has REPLICA IDENTITY FULL", relname);
                }
                'd' => {
                    eprintln!("⚠️  WARNING: Table '{}' has REPLICA IDENTITY DEFAULT", relname);
                    eprintln!("    This may cause issues with soft deletes in StarRocks.");
                    eprintln!("    Run: ALTER TABLE {} REPLICA IDENTITY FULL;", table);
                    // No fallar, solo advertir - dejar que el usuario decida
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
                    println!("ℹ️  Table '{}' has REPLICA IDENTITY INDEX", relname);
                    eprintln!("    Note: For full soft delete support, consider REPLICA IDENTITY FULL");
                }
                _ => {
                    eprintln!("⚠️  Unknown REPLICA IDENTITY '{}' for table '{}'", replica_char, relname);
                }
            }
        }

        Ok(())
    }

    /// Obtiene la URL limpia sin parámetros de replicación
    fn get_clean_url(&self) -> String {
        // Esta función asume que PostgresSource fue creado con una URL válida
        // En un escenario real, deberías almacenar la URL original
        // Por ahora, esto es un placeholder que necesitaría la URL del env
        std::env::var("DATABASE_URL")
            .unwrap_or_default()
            .replace("?replication=database", "")
            .replace("&replication=database", "")
            .replace("replication=database&", "")
    }
}
