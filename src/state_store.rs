use tokio_postgres::{Client, NoTls};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct StateStore {
    client: Arc<Mutex<Client>>,
}

impl StateStore {
    pub async fn new(database_url: &str) -> Result<Self> {
        // Crear conexión regular (no replicación) para checkpoints
        let clean_url = database_url
            .replace("?replication=database", "")
            .replace("&replication=database", "")
            .replace("replication=database&", "");
        
        let (client, connection) = tokio_postgres::connect(&clean_url, NoTls).await?;
        
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("StateStore connection error: {}", e);
            }
        });
        
        // Crear tabla de checkpoints
        client.execute(
            "CREATE TABLE IF NOT EXISTS dbmazz_checkpoints (
                slot_name TEXT PRIMARY KEY,
                lsn BIGINT NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )", &[]
        ).await?;
        
        Ok(Self { client: Arc::new(Mutex::new(client)) })
    }

    pub async fn save_checkpoint(&self, slot: &str, lsn: u64) -> Result<()> {
        let client = self.client.lock().await;
        client.execute(
            "INSERT INTO dbmazz_checkpoints (slot_name, lsn) VALUES ($1, $2)
             ON CONFLICT (slot_name) DO UPDATE SET lsn = $2, updated_at = NOW()",
            &[&slot, &(lsn as i64)]
        ).await?;
        Ok(())
    }

    pub async fn load_checkpoint(&self, slot: &str) -> Result<Option<u64>> {
        let client = self.client.lock().await;
        let row = client.query_opt(
            "SELECT lsn FROM dbmazz_checkpoints WHERE slot_name = $1",
            &[&slot]
        ).await?;
        
        Ok(row.map(|r| r.get::<_, i64>(0) as u64))
    }
}

