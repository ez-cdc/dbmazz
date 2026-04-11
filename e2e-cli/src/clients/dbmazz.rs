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
///
/// Field names match what the CLI uses internally. `serde(alias)` maps
/// from the actual JSON field names returned by the daemon's HTTP API:
///   - `events_processed` → `events_total`
///   - `events_per_second` → `events_per_sec`
///   - `estimated_memory_bytes` → `memory_bytes` (converted to MB in display)
///   - `state` → used to override `stage` when paused/stopped
#[derive(Debug, Clone, Deserialize)]
pub struct DaemonStatus {
    /// Pipeline stage: "setup", "snapshot", "cdc".
    /// Note: pause/stop is in the `state` field, not `stage`.
    #[serde(default = "default_stage")]
    pub stage: String,

    /// Engine state: "running", "paused", "draining", "stopped".
    #[serde(default)]
    pub state: String,

    #[serde(default)]
    pub uptime_secs: u64,

    #[serde(default = "default_lsn")]
    pub confirmed_lsn: String,

    #[serde(default = "default_lsn")]
    pub current_lsn: String,

    /// Total events processed. Daemon field: `events_processed`.
    #[serde(default, alias = "events_processed")]
    pub events_total: u64,

    /// Current throughput. Daemon field: `events_per_second`.
    #[serde(default, alias = "events_per_second")]
    pub events_per_sec: f64,

    #[serde(default)]
    pub batches_sent: u64,

    #[serde(default)]
    pub replication_lag_ms: u64,

    /// Memory in bytes. Daemon field: `estimated_memory_bytes`.
    #[serde(default, alias = "estimated_memory_bytes")]
    pub memory_bytes: u64,

    #[serde(default)]
    pub error_detail: Option<String>,
}

impl DaemonStatus {
    /// The effective display stage, merging `stage` + `state`.
    /// When state is "paused" or "stopped", that overrides the stage.
    pub fn effective_stage(&self) -> &str {
        match self.state.as_str() {
            "paused" => "paused",
            "stopped" => "stopped",
            "draining" => "stopped",
            _ => &self.stage,
        }
    }

    /// Memory in MB for display.
    pub fn memory_mb(&self) -> f64 {
        self.memory_bytes as f64 / (1024.0 * 1024.0)
    }
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

    /// Test parsing the REAL JSON that the daemon's /status returns.
    #[test]
    fn parse_real_daemon_response() {
        let json = r#"{
            "engine_running": true,
            "state": "running",
            "stage": "cdc",
            "uptime_secs": 120,
            "confirmed_lsn": "0x1A3F8E2",
            "current_lsn": "0x1A3F900",
            "events_processed": 5000,
            "events_per_second": 42,
            "batches_sent": 100,
            "replication_lag_ms": 50,
            "estimated_memory_bytes": 5242880,
            "pending_events": 3
        }"#;

        let status: DaemonStatus = serde_json::from_str(json).expect("parse failed");
        assert_eq!(status.stage, "cdc");
        assert_eq!(status.state, "running");
        assert_eq!(status.effective_stage(), "cdc");
        assert_eq!(status.uptime_secs, 120);
        assert_eq!(status.events_total, 5000); // alias: events_processed
        assert!((status.events_per_sec - 42.0).abs() < f64::EPSILON); // alias: events_per_second
        assert_eq!(status.batches_sent, 100);
        assert_eq!(status.replication_lag_ms, 50);
        assert_eq!(status.memory_bytes, 5242880); // alias: estimated_memory_bytes
        assert!((status.memory_mb() - 5.0).abs() < 0.01);
    }

    #[test]
    fn parse_paused_state() {
        let json = r#"{
            "engine_running": true,
            "state": "paused",
            "stage": "cdc"
        }"#;
        let status: DaemonStatus = serde_json::from_str(json).expect("parse failed");
        assert_eq!(status.stage, "cdc");
        assert_eq!(status.effective_stage(), "paused"); // state overrides stage
    }

    #[test]
    fn parse_daemon_status_minimal() {
        let json = r#"{"stage": "setup"}"#;
        let status: DaemonStatus = serde_json::from_str(json).expect("parse failed");
        assert_eq!(status.stage, "setup");
        assert_eq!(status.uptime_secs, 0);
        assert_eq!(status.confirmed_lsn, "0/0");
        assert_eq!(status.events_total, 0);
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
