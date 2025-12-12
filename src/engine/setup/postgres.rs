use anyhow::Result;
use tokio_postgres::{Client, NoTls};

use super::error::SetupError;
use crate::config::Config;

pub struct PostgresSetup<'a> {
    client: &'a Client,
    config: &'a Config,
}

impl<'a> PostgresSetup<'a> {
    pub fn new(client: &'a Client, config: &'a Config) -> Self {
        Self { client, config }
    }

    /// Ejecutar todo el setup de PostgreSQL
    pub async fn run(&self) -> Result<(), SetupError> {
        println!("🔧 PostgreSQL Setup:");
        
        // 1. Verificar que las tablas existen
        self.verify_tables_exist().await?;
        
        // 2. Configurar REPLICA IDENTITY FULL
        self.ensure_replica_identity().await?;
        
        // 3. Crear/verificar Publication
        self.ensure_publication().await?;
        
        // 4. Crear/verificar Replication Slot
        self.ensure_replication_slot().await?;
        
        println!("✅ PostgreSQL setup complete");
        Ok(())
    }

    /// Verificar que todas las tablas existen
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
                    error: e.to_string(),
                })?
                .get(0);

            if !exists {
                return Err(SetupError::PgTableNotFound {
                    table: table.clone(),
                });
            }
            
            println!("  ✓ Table {} exists", table);
        }
        Ok(())
    }

    /// Configurar REPLICA IDENTITY FULL en todas las tablas
    async fn ensure_replica_identity(&self) -> Result<(), SetupError> {
        for table in &self.config.tables {
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
                    error: e.to_string(),
                })?;

            let replica_identity: i8 = row.get(0);
            let identity_char = replica_identity as u8 as char;

            // Si no es FULL, configurar
            if identity_char != 'f' {
                println!("  🔧 Setting REPLICA IDENTITY FULL on {}", table);
                self.client
                    .execute(
                        &format!("ALTER TABLE {} REPLICA IDENTITY FULL", table),
                        &[],
                    )
                    .await
                    .map_err(|e| SetupError::PgReplicaIdentityFailed {
                        table: table.clone(),
                        error: e.to_string(),
                    })?;
                println!("  ✅ REPLICA IDENTITY FULL set on {}", table);
            } else {
                println!("  ✓ {} already has REPLICA IDENTITY FULL", table);
            }
        }
        Ok(())
    }

    /// Crear/verificar Publication
    async fn ensure_publication(&self) -> Result<(), SetupError> {
        let pub_name = &self.config.publication_name;

        // Verificar si existe
        let exists: bool = self.client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
                &[&pub_name],
            )
            .await
            .map_err(|e| SetupError::PgPublicationFailed {
                name: pub_name.clone(),
                error: e.to_string(),
            })?
            .get(0);

        if exists {
            println!("  ✓ Publication {} exists", pub_name);
            
            // Verificar que incluye todas las tablas
            let missing = self.get_missing_tables_in_publication(pub_name).await?;
            
            for table in missing {
                println!("  🔧 Adding {} to publication {}", table, pub_name);
                self.client
                    .execute(
                        &format!("ALTER PUBLICATION {} ADD TABLE {}", pub_name, table),
                        &[],
                    )
                    .await
                    .map_err(|e| SetupError::PgPublicationFailed {
                        name: pub_name.clone(),
                        error: e.to_string(),
                    })?;
                println!("  ✅ Table {} added to publication", table);
            }
        } else {
            // Crear nueva publication
            println!("  🔧 Creating publication {}", pub_name);
            let tables = self.config.tables.join(", ");
            self.client
                .execute(
                    &format!("CREATE PUBLICATION {} FOR TABLE {}", pub_name, tables),
                    &[],
                )
                .await
                .map_err(|e| SetupError::PgPublicationFailed {
                    name: pub_name.clone(),
                    error: e.to_string(),
                })?;
            println!("  ✅ Publication {} created", pub_name);
        }

        Ok(())
    }

    /// Obtener tablas que faltan en la publication
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
                error: e.to_string(),
            })?;

        let existing: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
        
        let missing: Vec<String> = self.config.tables
            .iter()
            .filter(|table| {
                // Normalizar nombres para comparación
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

    /// Crear/verificar Replication Slot
    async fn ensure_replication_slot(&self) -> Result<(), SetupError> {
        let slot_name = &self.config.slot_name;

        // Verificar si existe
        let exists: bool = self.client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                &[&slot_name],
            )
            .await
            .map_err(|e| SetupError::PgSlotFailed {
                name: slot_name.clone(),
                error: e.to_string(),
            })?
            .get(0);

        if exists {
            println!("  ✓ Replication slot {} exists (recovery mode)", slot_name);
        } else {
            println!("  🔧 Creating replication slot {}", slot_name);
            self.client
                .execute(
                    "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                    &[&slot_name],
                )
                .await
                .map_err(|e| SetupError::PgSlotFailed {
                    name: slot_name.clone(),
                    error: e.to_string(),
                })?;
            println!("  ✅ Replication slot {} created", slot_name);
        }

        Ok(())
    }
}

/// Helper para crear cliente PostgreSQL normal (no replicación)
pub async fn create_postgres_client(database_url: &str) -> Result<Client, SetupError> {
    // Remover parámetro de replicación para conexión normal
    let clean_url = database_url
        .replace("?replication=database", "")
        .replace("&replication=database", "")
        .replace("replication=database&", "");
    
    let (client, connection) = tokio_postgres::connect(&clean_url, NoTls)
        .await
        .map_err(|e| SetupError::PgConnectionFailed {
            host: clean_url.clone(),
            error: e.to_string(),
        })?;

    // Spawn connection en background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL setup connection error: {}", e);
        }
    });

    Ok(client)
}
