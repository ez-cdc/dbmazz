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

/// Backend for checkpoint persistence.
///
/// Detects the database scheme from the URL:
/// - `postgres://` → PostgreSQL with `lsn BIGINT`
/// - `mysql://`    → MySQL with `(mysql_binlog_file, mysql_gtid_set)`
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

    /// MySQL backend — creates `dbmazz_checkpoints` with the
    /// `(mysql_binlog_file, mysql_gtid_set)` triple used by MySQL CDC
    /// restart.
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
                mysql_binlog_file VARCHAR(512) NOT NULL,
                mysql_binlog_position BIGINT UNSIGNED NOT NULL,
                mysql_gtid_set TEXT NOT NULL,
                status VARCHAR(16) NULL DEFAULT 'ACTIVE',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        )
        .await
        .context("StateStore (MySQL): failed to create dbmazz_checkpoints table")?;
        // Idempotent v1→v2 migration: add `status` column. Older 8.0
        // versions don't support ADD COLUMN IF NOT EXISTS, so probe
        // with SHOW COLUMNS first.
        let has_status: Vec<mysql_async::Row> = conn
            .query("SHOW COLUMNS FROM dbmazz_checkpoints LIKE 'status'")
            .await
            .context("StateStore (MySQL): failed to probe status column")?;
        if has_status.is_empty() {
            conn.query_drop(
                "ALTER TABLE dbmazz_checkpoints \
                 ADD COLUMN status VARCHAR(16) NULL DEFAULT 'ACTIVE'",
            )
            .await
            .context("StateStore (MySQL): failed to add status column")?;
        }
        info!("StateStore (MySQL): ensured dbmazz_checkpoints table (v2)");

        Ok(Self {
            backend: StoreBackend::Mysql {
                conn: Arc::new(Mutex::new(conn)),
            },
        })
    }

    /// Persist a Postgres LSN checkpoint for the given slot.
    ///
    /// MySQL uses `save_mysql_checkpoint` instead — this method is
    /// Postgres-only and rejects calls against the MySQL backend.
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
                Ok(())
            }
            #[cfg(feature = "mysql-source")]
            StoreBackend::Mysql { .. } => {
                anyhow::bail!(
                    "save_checkpoint(slot, lsn) is Postgres-only; \
                     use save_mysql_checkpoint(slot, file, position, gtid_set) for MySQL."
                )
            }
        }
    }

    /// Persist the MySQL binlog checkpoint triple `(file, position, gtid_set)`.
    /// Rejected by the Postgres backend — Postgres uses `save_checkpoint`.
    ///
    /// Always writes `status = 'ACTIVE'`. For a first-run bootstrap
    /// checkpoint (captured from `SHOW MASTER STATUS` before the
    /// snapshot has produced any data), use
    /// `save_mysql_provisional_checkpoint` instead.
    #[cfg(feature = "mysql-source")]
    pub async fn save_mysql_checkpoint(
        &self,
        slot: &str,
        binlog_file: &str,
        position: u64,
        gtid_set: &str,
    ) -> Result<()> {
        self.save_mysql_checkpoint_with_status(slot, binlog_file, position, gtid_set, "ACTIVE")
            .await
    }

    /// Persist a PROVISIONAL bootstrap checkpoint. The row is promoted
    /// to ACTIVE by the next `save_mysql_checkpoint` call at a real
    /// commit boundary. Used by the first-run binlog bootstrap (H5).
    #[cfg(feature = "mysql-source")]
    pub async fn save_mysql_provisional_checkpoint(
        &self,
        slot: &str,
        binlog_file: &str,
        position: u64,
        gtid_set: &str,
    ) -> Result<()> {
        self.save_mysql_checkpoint_with_status(slot, binlog_file, position, gtid_set, "PROVISIONAL")
            .await
    }

    #[cfg(feature = "mysql-source")]
    async fn save_mysql_checkpoint_with_status(
        &self,
        slot: &str,
        binlog_file: &str,
        position: u64,
        gtid_set: &str,
        status: &str,
    ) -> Result<()> {
        match &self.backend {
            StoreBackend::Postgres { .. } => anyhow::bail!(
                "save_mysql_checkpoint called on a Postgres state store; \
                 use save_checkpoint(slot, lsn) for Postgres."
            ),
            StoreBackend::Mysql { conn } => {
                let mut conn = conn.lock().await;
                conn.exec_drop(
                    "INSERT INTO dbmazz_checkpoints \
                       (slot_name, mysql_binlog_file, mysql_binlog_position, \
                        mysql_gtid_set, status) \
                     VALUES (?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE \
                       mysql_binlog_file = VALUES(mysql_binlog_file), \
                       mysql_binlog_position = VALUES(mysql_binlog_position), \
                       mysql_gtid_set = VALUES(mysql_gtid_set), \
                       status = VALUES(status), \
                       updated_at = CURRENT_TIMESTAMP",
                    (slot, binlog_file, position, gtid_set, status),
                )
                .await
                .context("StateStore (MySQL): failed to save mysql checkpoint")?;
                Ok(())
            }
        }
    }

    /// Load the MySQL binlog checkpoint triple, or `None` if not persisted.
    /// Rejected by the Postgres backend.
    #[cfg(feature = "mysql-source")]
    pub async fn load_mysql_checkpoint(&self, slot: &str) -> Result<Option<(String, u64, String)>> {
        match &self.backend {
            StoreBackend::Postgres { .. } => anyhow::bail!(
                "load_mysql_checkpoint called on a Postgres state store; \
                 use load_checkpoint(slot) for Postgres."
            ),
            StoreBackend::Mysql { conn } => {
                let mut conn = conn.lock().await;
                let row: Option<(String, u64, String)> = conn
                    .exec_first(
                        "SELECT mysql_binlog_file, mysql_binlog_position, mysql_gtid_set \
                         FROM dbmazz_checkpoints \
                         WHERE slot_name = ?",
                        (slot,),
                    )
                    .await
                    .context("StateStore (MySQL): failed to load mysql checkpoint")?;
                Ok(row)
            }
        }
    }

    /// Load the last Postgres LSN checkpoint for the given slot.
    /// Postgres-only — MySQL uses `load_mysql_checkpoint`.
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
            StoreBackend::Mysql { .. } => anyhow::bail!(
                "load_checkpoint(slot) is Postgres-only; \
                 use load_mysql_checkpoint(slot) for MySQL."
            ),
        }
    }
}

/// Build `mysql_async::Opts` from a `mysql://` URL.
///
/// Reads `MYSQL_TLS_SKIP_VERIFY` to honour the same opt-out the source
/// connections use. The state store usually runs against the same MySQL
/// instance as the source, so a single env var controls both.
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

    let tls_skip_verify = std::env::var("MYSQL_TLS_SKIP_VERIFY")
        .map(|v| v == "true")
        .unwrap_or(false);
    let ssl_opts = crate::source::mysql::mysql_ssl_opts(tls_skip_verify);

    let builder = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .db_name(Some(database))
        .user(Some(user))
        .pass(Some(password))
        .ssl_opts(ssl_opts);

    Ok(mysql_async::Opts::from(builder))
}
