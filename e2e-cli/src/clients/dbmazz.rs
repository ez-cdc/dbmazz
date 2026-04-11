//! HTTP client for the dbmazz daemon.
//!
//! The daemon exposes `/healthz`, `/status`, `/pause`, `/resume` endpoints
//! when built with `--features http-api`. All test profiles use that build.
//!
//! This client is async (reqwest + tokio) and supports polling helpers
//! (`wait_for_stage`, `wait_healthy`) for the verify runner.

use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

// ── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum DbmazzError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("daemon returned unexpected data: {0}")]
    BadResponse(String),

    #[error("dbmazz setup error: {0}")]
    SetupError(String),

    #[error("timed out waiting for {what} after {elapsed_secs:.0}s: {detail}")]
    Timeout {
        what: String,
        elapsed_secs: f64,
        detail: String,
    },
}

// ── DaemonStatus ────────────────────────────────────────────────────────────

/// Snapshot of the dbmazz daemon state from `/status`.
#[derive(Debug, Clone, Deserialize)]
pub struct DaemonStatus {
    /// Pipeline stage: "setup", "snapshot", "cdc", "paused", "stopped".
    #[serde(default = "default_stage")]
    pub stage: String,

    #[serde(default)]
    pub uptime_secs: u64,

    /// E.g., "0/1A3F8E2"
    #[serde(default = "default_lsn")]
    pub confirmed_lsn: String,

    #[serde(default = "default_lsn")]
    pub current_lsn: String,

    #[serde(default)]
    pub events_total: u64,

    #[serde(default)]
    pub events_per_sec: f64,

    #[serde(default)]
    pub batches_sent: u64,

    #[serde(default)]
    pub replication_lag_ms: u64,

    #[serde(default)]
    pub memory_rss_mb: f64,

    #[serde(default)]
    pub cpu_millicores: u64,

    #[serde(default)]
    pub snapshot_active: bool,

    #[serde(default)]
    pub snapshot_chunks_total: u64,

    #[serde(default)]
    pub snapshot_chunks_done: u64,

    #[serde(default)]
    pub snapshot_rows_synced: u64,

    #[serde(default)]
    pub error_detail: Option<String>,
}

fn default_stage() -> String {
    "unknown".into()
}

fn default_lsn() -> String {
    "0/0".into()
}

// ── Client ──────────────────────────────────────────────────────────────────

/// Async HTTP client for the dbmazz daemon.
pub struct DbmazzClient {
    base_url: String,
    client: reqwest::Client,
}

impl DbmazzClient {
    /// Create a new client.
    ///
    /// `base_url` should be e.g. `http://localhost:8080`. Trailing slash is stripped.
    /// `timeout` is the per-request timeout.
    pub fn new(base_url: &str, timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("failed to build reqwest client");

        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
        }
    }

    /// Create a client with default 5-second timeout.
    pub fn with_defaults(base_url: &str) -> Self {
        Self::new(base_url, Duration::from_secs(5))
    }

    // ── endpoints ───────────────────────────────────────────────────────

    /// Return true if `/healthz` returns 200 and `status == "ok"`.
    pub async fn health(&self) -> bool {
        let url = format!("{}/healthz", self.base_url);
        let resp = match self.client.get(&url).send().await {
            Ok(r) => r,
            Err(_) => return false,
        };
        if !resp.status().is_success() {
            return false;
        }
        let data: serde_json::Value = match resp.json().await {
            Ok(d) => d,
            Err(_) => return false,
        };
        data.get("status")
            .and_then(|v| v.as_str())
            .map(|s| s == "ok")
            .unwrap_or(false)
    }

    /// Fetch `/status` and return a `DaemonStatus`.
    pub async fn status(&self) -> Result<DaemonStatus, DbmazzError> {
        let url = format!("{}/status", self.base_url);
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(DbmazzError::BadResponse(format!(
                "/status returned {}",
                resp.status()
            )));
        }
        let status: DaemonStatus = resp.json().await.map_err(|e| {
            DbmazzError::BadResponse(format!("invalid JSON from /status: {e}"))
        })?;
        Ok(status)
    }

    /// POST `/pause`.
    pub async fn pause(&self) -> Result<(), DbmazzError> {
        let url = format!("{}/pause", self.base_url);
        let resp = self.client.post(&url).send().await?;
        if !resp.status().is_success() {
            return Err(DbmazzError::BadResponse(format!(
                "failed to pause daemon: {}",
                resp.status()
            )));
        }
        Ok(())
    }

    /// POST `/resume`.
    pub async fn resume(&self) -> Result<(), DbmazzError> {
        let url = format!("{}/resume", self.base_url);
        let resp = self.client.post(&url).send().await?;
        if !resp.status().is_success() {
            return Err(DbmazzError::BadResponse(format!(
                "failed to resume daemon: {}",
                resp.status()
            )));
        }
        Ok(())
    }

    // ── higher-level helpers ────────────────────────────────────────────

    /// Poll `/status` until `stage == expected` or timeout expires.
    ///
    /// Returns the final `DaemonStatus` on success. Returns an error on timeout
    /// or on setup errors (`error_detail` set).
    pub async fn wait_for_stage(
        &self,
        expected: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<DaemonStatus, DbmazzError> {
        let start = tokio::time::Instant::now();
        let mut last_stage: Option<String> = None;

        while start.elapsed() < timeout {
            match self.status().await {
                Ok(status) => {
                    // Propagate setup errors immediately.
                    if let Some(ref detail) = status.error_detail {
                        return Err(DbmazzError::SetupError(detail.clone()));
                    }
                    if status.stage == expected {
                        return Ok(status);
                    }
                    last_stage = Some(status.stage.clone());
                }
                Err(_) => {
                    // Daemon may still be starting up -- retry.
                }
            }
            tokio::time::sleep(poll_interval).await;
        }

        let elapsed = start.elapsed().as_secs_f64();
        let detail = match last_stage {
            Some(stage) => format!("last stage: '{stage}'"),
            None => "daemon never responded to /status".to_string(),
        };
        Err(DbmazzError::Timeout {
            what: format!("stage '{expected}'"),
            elapsed_secs: elapsed,
            detail,
        })
    }

    /// Poll `/healthz` until it returns ok, or timeout.
    pub async fn wait_healthy(
        &self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<(), DbmazzError> {
        let start = tokio::time::Instant::now();
        while start.elapsed() < timeout {
            if self.health().await {
                return Ok(());
            }
            tokio::time::sleep(poll_interval).await;
        }
        let elapsed = start.elapsed().as_secs_f64();
        Err(DbmazzError::Timeout {
            what: "healthy".to_string(),
            elapsed_secs: elapsed,
            detail: "daemon did not become healthy".to_string(),
        })
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_daemon_status_full() {
        let json = r#"{
            "stage": "cdc",
            "uptime_secs": 120,
            "confirmed_lsn": "0/1A3F8E2",
            "current_lsn": "0/1A3F900",
            "events_total": 5000,
            "events_per_sec": 42.5,
            "batches_sent": 100,
            "replication_lag_ms": 50,
            "memory_rss_mb": 128.5,
            "cpu_millicores": 250,
            "snapshot_active": false,
            "snapshot_chunks_total": 10,
            "snapshot_chunks_done": 10,
            "snapshot_rows_synced": 50000,
            "error_detail": null
        }"#;

        let status: DaemonStatus = serde_json::from_str(json).expect("parse failed");
        assert_eq!(status.stage, "cdc");
        assert_eq!(status.uptime_secs, 120);
        assert_eq!(status.confirmed_lsn, "0/1A3F8E2");
        assert_eq!(status.current_lsn, "0/1A3F900");
        assert_eq!(status.events_total, 5000);
        assert!((status.events_per_sec - 42.5).abs() < f64::EPSILON);
        assert_eq!(status.batches_sent, 100);
        assert_eq!(status.replication_lag_ms, 50);
        assert!((status.memory_rss_mb - 128.5).abs() < f64::EPSILON);
        assert_eq!(status.cpu_millicores, 250);
        assert!(!status.snapshot_active);
        assert_eq!(status.snapshot_chunks_total, 10);
        assert_eq!(status.snapshot_chunks_done, 10);
        assert_eq!(status.snapshot_rows_synced, 50000);
        assert!(status.error_detail.is_none());
    }

    #[test]
    fn parse_daemon_status_minimal() {
        // The daemon may return a minimal response during setup.
        let json = r#"{"stage": "setup"}"#;
        let status: DaemonStatus = serde_json::from_str(json).expect("parse failed");
        assert_eq!(status.stage, "setup");
        assert_eq!(status.uptime_secs, 0);
        assert_eq!(status.confirmed_lsn, "0/0");
        assert_eq!(status.events_total, 0);
        assert!(!status.snapshot_active);
        assert!(status.error_detail.is_none());
    }

    #[test]
    fn parse_daemon_status_with_error() {
        let json = r#"{
            "stage": "setup",
            "error_detail": "source connection refused"
        }"#;
        let status: DaemonStatus = serde_json::from_str(json).expect("parse failed");
        assert_eq!(status.stage, "setup");
        assert_eq!(
            status.error_detail.as_deref(),
            Some("source connection refused")
        );
    }

    #[test]
    fn parse_daemon_status_empty_object() {
        let json = "{}";
        let status: DaemonStatus = serde_json::from_str(json).expect("parse failed");
        assert_eq!(status.stage, "unknown");
        assert_eq!(status.confirmed_lsn, "0/0");
    }

    #[test]
    fn client_strips_trailing_slash() {
        let client = DbmazzClient::with_defaults("http://localhost:8080/");
        assert_eq!(client.base_url, "http://localhost:8080");
    }
}
