//! Manage dbmazz as a local process (not a Docker container).
//!
//! The e2e CLI spawns dbmazz directly with the right env vars for the
//! selected source/sink pair. This is faster than Docker, easier to
//! debug, and the natural choice for local development testing.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;

use tokio::process::{Child, Command};

use crate::config::schema::*;
use crate::paths;

/// Locate the dbmazz binary. Prefers release, falls back to debug.
#[allow(dead_code)]
pub fn find_binary() -> Result<PathBuf, String> {
    let repo_root = paths::REPO_ROOT.to_path_buf();

    let candidates = [
        repo_root.join("target/release/dbmazz"),
        repo_root.join("target/debug/dbmazz"),
    ];

    for bin in &candidates {
        if bin.exists() {
            return Ok(bin.clone());
        }
    }

    Err(
        "dbmazz binary not found. Build it with:\n  \
         cargo build --release"
            .into(),
    )
}

/// Build the environment variables for a dbmazz process from specs + settings.
/// Unlike the Docker approach, URLs are NOT rewritten — dbmazz connects
/// directly to localhost ports.
#[allow(dead_code)]
pub fn build_env(
    source: &SourceSpec,
    sink: &SinkSpec,
    settings: &PipelineSettings,
) -> HashMap<String, String> {
    let mut env = HashMap::new();

    // Source vars.
    match source {
        SourceSpec::Postgres(inner) => {
            let mut source_url = inner.url.clone();
            if !source_url.contains("replication=database") {
                let sep = if source_url.contains('?') { "&" } else { "?" };
                source_url = format!("{source_url}{sep}replication=database");
            }
            env.insert("SOURCE_TYPE".into(), "postgres".into());
            env.insert("SOURCE_URL".into(), source_url);
            env.insert("SOURCE_SLOT_NAME".into(), inner.replication_slot.clone());
            env.insert("SOURCE_PUBLICATION_NAME".into(), inner.publication.clone());
            env.insert("TABLES".into(), inner.tables.join(","));
        }
    }

    // Sink vars.
    match sink {
        SinkSpec::Postgres(pg) => {
            env.insert("SINK_TYPE".into(), "postgres".into());
            env.insert("SINK_URL".into(), pg.url.clone());
            env.insert("SINK_DATABASE".into(), pg.database.clone());
            env.insert("SINK_SCHEMA".into(), pg.schema_name.clone());
        }
        SinkSpec::Starrocks(sr) => {
            env.insert("SINK_TYPE".into(), "starrocks".into());
            env.insert("SINK_URL".into(), sr.url.clone());
            env.insert("SINK_PORT".into(), sr.mysql_port.to_string());
            env.insert("SINK_DATABASE".into(), sr.database.clone());
            env.insert("SINK_USER".into(), sr.user.clone());
            env.insert("SINK_PASSWORD".into(), sr.password.clone());
        }
        SinkSpec::Snowflake(sf) => {
            env.insert("SINK_TYPE".into(), "snowflake".into());
            env.insert("SINK_SNOWFLAKE_ACCOUNT".into(), sf.account.clone());
            env.insert("SINK_USER".into(), sf.user.clone());
            env.insert("SINK_PASSWORD".into(), sf.password.clone());
            env.insert("SINK_DATABASE".into(), sf.database.clone());
            env.insert("SINK_SCHEMA".into(), sf.schema_name.clone());
            env.insert("SINK_SNOWFLAKE_WAREHOUSE".into(), sf.warehouse.clone());
            env.insert(
                "SINK_SNOWFLAKE_SOFT_DELETE".into(),
                if sf.soft_delete { "true" } else { "false" }.into(),
            );
            if let Some(role) = &sf.role {
                env.insert("SINK_SNOWFLAKE_ROLE".into(), role.clone());
            }
            if let Some(key) = &sf.private_key_path {
                env.insert("SINK_SNOWFLAKE_PRIVATE_KEY_PATH".into(), key.clone());
            }
        }
    }

    // Pipeline settings.
    env.insert("FLUSH_SIZE".into(), settings.flush_size.to_string());
    env.insert("FLUSH_INTERVAL_MS".into(), settings.flush_interval_ms.to_string());
    env.insert("DO_SNAPSHOT".into(), if settings.do_snapshot { "true" } else { "false" }.into());
    env.insert("SNAPSHOT_CHUNK_SIZE".into(), settings.snapshot_chunk_size.to_string());
    env.insert("SNAPSHOT_PARALLEL_WORKERS".into(), settings.snapshot_parallel_workers.to_string());
    env.insert("INITIAL_SNAPSHOT_ONLY".into(), if settings.initial_snapshot_only { "true" } else { "false" }.into());
    env.insert("RUST_LOG".into(), settings.rust_log.clone());
    env.insert("SINK_SNOWFLAKE_FLUSH_FILES".into(), settings.snowflake_flush_files.to_string());
    env.insert("SINK_SNOWFLAKE_FLUSH_BYTES".into(), settings.snowflake_flush_bytes.to_string());

    // Ports.
    env.insert("HTTP_API_PORT".into(), "8080".into());
    env.insert("GRPC_PORT".into(), "50051".into());

    env
}

/// A running dbmazz process.
#[allow(dead_code)]
pub struct DaemonProcess {
    child: Child,
}

#[allow(dead_code)]
impl DaemonProcess {
    /// Spawn dbmazz as a local process with the given env vars.
    /// Logs go to a file in the cache dir.
    pub async fn spawn(
        source: &SourceSpec,
        sink: &SinkSpec,
        settings: &PipelineSettings,
        log_file: Option<PathBuf>,
    ) -> Result<Self, String> {
        let bin = find_binary()?;
        let env = build_env(source, sink, settings);

        let mut cmd = Command::new(&bin);
        // Clear inherited env to avoid interference from the parent shell
        // (e.g. stale SOURCE_URL, FLUSH_SIZE, etc. from prior runs).
        // Preserve system essentials (DNS, dylibs, TLS certs).
        cmd.env_clear();
        for key in &["PATH", "HOME", "USER", "LANG", "DYLD_FALLBACK_LIBRARY_PATH", "TMPDIR"] {
            if let Ok(val) = std::env::var(key) {
                cmd.env(key, val);
            }
        }
        cmd.envs(&env);

        // Redirect stdout/stderr to log file if provided, otherwise inherit.
        if let Some(log_path) = log_file {
            if let Some(parent) = log_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let file = std::fs::File::create(&log_path)
                .map_err(|e| format!("create log file {}: {e}", log_path.display()))?;
            let file2 = file.try_clone()
                .map_err(|e| format!("clone log file handle: {e}"))?;
            cmd.stdout(Stdio::from(file));
            cmd.stderr(Stdio::from(file2));
        } else {
            cmd.stdout(Stdio::inherit());
            cmd.stderr(Stdio::inherit());
        }

        let child = cmd
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("failed to spawn dbmazz at {}: {e}", bin.display()))?;

        Ok(Self { child })
    }

    /// Stop the dbmazz process gracefully.
    pub async fn stop(&mut self) -> Result<(), String> {
        // Send SIGTERM first for graceful shutdown.
        #[cfg(unix)]
        {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            if let Some(pid) = self.child.id() {
                let _ = signal::kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
                // Give it a moment to shut down gracefully.
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }

        // Force kill if still running.
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
        Ok(())
    }

    /// Check if the process is still running.
    pub fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}
