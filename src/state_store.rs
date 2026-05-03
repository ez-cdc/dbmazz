// Copyright 2025
// Licensed under the Elastic License v2.0

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use tracing::error;

#[cfg(feature = "mysql-source")]
use anyhow::Context;
#[cfg(feature = "mysql-source")]
use mysql_async::prelude::Queryable;
#[cfg(feature = "mysql-source")]
use tracing::info;

/// Backend for LSN / checkpoint persistence.
///
/// Detects the database scheme from the URL:
/// - `postgres://` → PostgreSQL with `lsn BIGINT`
/// - `mysql://` → MySQL with `position TEXT` (for future GTID support)
#[derive(Clone)]
enum StoreBackend {
    Postgres {
        client: Arc<Mutex<Client>>,
    },
    #[cfg(feature = "mysql-source")]
    Mysql {
        conn: Arc<Mutex<mysql_async::Conn>>,
    },
}

#[derive(Clone)]
pub struct StateStore {
    backend: StoreBackend,
}

impl StateStore {
    /// Create a new state store. URL scheme selects the backend (`postgres://` or `mysql://`).
    pub async fn new(database_url: &str) -> Result<Self> {
        if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
            Self::new_postgres(database_url).await
        } else if database_url.starts_with("mysql://") {
            #[cfg(feature = "mysql-source")]
            {
                Self::new_mysql(database_url).await
            }
            #[cfg(not(feature = "mysql-source"))]
            {
                anyhow::bail!(
                    "MySQL checkpoint store requires the 'mysql-source' feature. \
                     Build with --features mysql-source"
                )
            }
        } else {
            anyhow::bail!(
                "Unsupported state store URL scheme. \
                 Expected 'postgres://', 'postgresql://', or 'mysql://', got: '{}'",
                database_url.split("://").next().unwrap_or(database_url)
            )
        }
    }

    /// PostgreSQL backend (original implementation, unchanged behavior).
    async fn new_postgres(database_url: &str) -> Result<Self> {
        // Remove replication query param so we connect as a regular client
        let clean_url = database_url
            .replace("?replication=database", "")
            .replace("&replication=database", "")
            .replace("replication=database&", "");

        let (client, connection) = tokio_postgres::connect(&clean_url, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("StateStore (PostgreSQL) connection error: {}", e);
            }
        });

        // Create checkpoints table
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS dbmazz_checkpoints (
                slot_name TEXT PRIMARY KEY,
                lsn BIGINT NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )",
                &[],
            )
            .await?;

        Ok(Self {
            backend: StoreBackend::Postgres {
                client: Arc::new(Mutex::new(client)),
            },
        })
    }

    /// MySQL backend — creates a `dbmazz_checkpoints` table with a `position TEXT` column.
    #[cfg(feature = "mysql-source")]
    async fn new_mysql(database_url: &str) -> Result<Self> {
        let opts = build_mysql_opts(database_url)
            .context("StateStore (MySQL): failed to parse MySQL URL")?;

        let mut conn = mysql_async::Conn::new(opts)
            .await
            .context("StateStore (MySQL): failed to connect")?;
        info!("StateStore (MySQL): connected");

        conn.query_drop(
            "CREATE TABLE IF NOT EXISTS dbmazz_checkpoints (
                slot_name VARCHAR(255) PRIMARY KEY,
                position TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        )
        .await
        .context("StateStore (MySQL): failed to create dbmazz_checkpoints table")?;
        info!("StateStore (MySQL): ensured dbmazz_checkpoints table");

        Ok(Self {
            backend: StoreBackend::Mysql {
                conn: Arc::new(Mutex::new(conn)),
            },
        })
    }

    /// Persist a checkpoint position for the given slot.
    pub async fn save_checkpoint(&self, slot: &str, lsn: u64) -> Result<()> {
        match &self.backend {
            StoreBackend::Postgres { client } => {
                let client = client.lock().await;
                client
                    .execute(
                        "INSERT INTO dbmazz_checkpoints (slot_name, lsn)
                         VALUES ($1, $2)
                         ON CONFLICT (slot_name)
                         DO UPDATE SET lsn = $2, updated_at = NOW()",
                        &[&slot, &(lsn as i64)],
                    )
                    .await?;
            }
            #[cfg(feature = "mysql-source")]
            StoreBackend::Mysql { conn } => {
                let mut conn = conn.lock().await;
                conn.exec_drop(
                    "INSERT INTO dbmazz_checkpoints (slot_name, position)
                     VALUES (?, ?)
                     ON DUPLICATE KEY UPDATE position = VALUES(position),
                                             updated_at = CURRENT_TIMESTAMP",
                    (slot, lsn.to_string()),
                )
                .await
                .context("StateStore (MySQL): failed to save checkpoint")?;
            }
        }
        Ok(())
    }

    /// Load the last checkpoint position for the given slot, or `None` if none exists.
    pub async fn load_checkpoint(&self, slot: &str) -> Result<Option<u64>> {
        match &self.backend {
            StoreBackend::Postgres { client } => {
                let client = client.lock().await;
                let row = client
                    .query_opt(
                        "SELECT lsn FROM dbmazz_checkpoints WHERE slot_name = $1",
                        &[&slot],
                    )
                    .await?;

                Ok(row.map(|r| r.get::<_, i64>(0) as u64))
            }
            #[cfg(feature = "mysql-source")]
            StoreBackend::Mysql { conn } => {
                let mut conn = conn.lock().await;
                let row: Option<(String,)> = conn
                    .exec_first(
                        "SELECT position FROM dbmazz_checkpoints WHERE slot_name = ?",
                        (slot,),
                    )
                    .await
                    .context("StateStore (MySQL): failed to load checkpoint")?;

                match row {
                    Some((pos,)) => match pos.parse::<u64>() {
                        Ok(lsn) => Ok(Some(lsn)),
                        // Position might be a GTID string — LSN interface can't represent it.
                        Err(_) => Ok(None),
                    },
                    None => Ok(None),
                }
            }
        }
    }
}

/// Build `mysql_async::Opts` from a `mysql://` URL.
#[cfg(feature = "mysql-source")]
fn build_mysql_opts(url: &str) -> Result<mysql_async::Opts> {
    let parsed = url::Url::parse(url).context("StateStore (MySQL): failed to parse URL")?;

    anyhow::ensure!(
        parsed.scheme() == "mysql",
        "StateStore (MySQL): unsupported scheme '{}'",
        parsed.scheme()
    );

    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(3306);
    let user = if !parsed.username().is_empty() {
        parsed.username()
    } else {
        "root"
    };
    let password = parsed.password().unwrap_or("");
    let database = parsed.path().trim_start_matches('/');

    let ssl_opts = mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true);

    let builder = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .db_name(Some(database))
        .user(Some(user))
        .pass(Some(password))
        .ssl_opts(ssl_opts);

    Ok(mysql_async::Opts::from(builder))
}
