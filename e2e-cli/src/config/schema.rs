//! Serde schema for `e2e/ez-cdc.yaml`.
//!
//! Mirrors the Python Pydantic models in `datasources/schema.py`.
//! Every spec discriminates on `type:`. Validation happens post-deserialization
//! via the `validate()` methods.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

// ── Identifier validation ──────────────────────────────────────────────────

/// Datasource names: lowercase alphanumeric + hyphen/underscore, 1-64 chars.
pub fn validate_datasource_name(name: &str) -> Result<(), String> {
    let re = regex::Regex::new(r"^[a-z0-9][a-z0-9_-]{0,63}$").unwrap();
    if !re.is_match(name) {
        return Err(format!(
            "datasource name {name:?} must be lowercase alphanumeric \
             (plus '-' and '_'), 1-64 chars, starting with a letter or digit"
        ));
    }
    Ok(())
}

/// PostgreSQL logical replication slot/publication identifier.
fn validate_pg_ident(value: &str, label: &str) -> Result<(), String> {
    let re = regex::Regex::new(r"^[a-z_][a-z0-9_]{0,62}$").unwrap();
    if !re.is_match(value) {
        return Err(format!(
            "{label} {value:?} must start with a lowercase letter or underscore \
             and contain only lowercase letters, digits, and underscores"
        ));
    }
    Ok(())
}

// ── Source specs ────────────────────────────────────────────────────────────

/// A PostgreSQL source database for CDC.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceSpec {
    /// Discriminator tag — always "postgres".
    #[serde(rename = "type")]
    pub type_tag: SourceType,

    /// PostgreSQL connection URL (without `?replication=database`).
    pub url: String,

    /// Optional SQL seed file inside `e2e/fixtures/` (e.g. "postgres-seed.sql").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<String>,

    /// Logical replication slot name.
    #[serde(default = "default_slot")]
    pub replication_slot: String,

    /// Publication name.
    #[serde(default = "default_publication")]
    pub publication: String,

    /// Tables to replicate (at least one required).
    pub tables: Vec<String>,
}

fn default_slot() -> String {
    "dbmazz_slot".into()
}
fn default_publication() -> String {
    "dbmazz_pub".into()
}

impl PostgresSourceSpec {
    #[allow(dead_code)]
    pub fn validate(&self) -> Result<(), String> {
        validate_pg_ident(&self.replication_slot, "replication_slot")?;
        validate_pg_ident(&self.publication, "publication")?;
        if self.tables.is_empty() {
            return Err("tables cannot be empty".into());
        }
        let mut seen = std::collections::HashSet::new();
        for t in &self.tables {
            let t = t.trim();
            if t.is_empty() {
                return Err("table name cannot be empty".into());
            }
            if !seen.insert(t) {
                return Err(format!("duplicate table {t:?}"));
            }
        }
        Ok(())
    }
}

// ── Sink specs ─────────────────────────────────────────────────────────────

/// A PostgreSQL sink database.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSinkSpec {
    #[serde(rename = "type")]
    pub type_tag: SinkType,

    /// PostgreSQL connection URL.
    pub url: String,

    /// Target database name.
    pub database: String,

    /// Target schema (defaults to "public").
    #[serde(default = "default_pg_schema", rename = "schema")]
    pub schema_name: String,
}

fn default_pg_schema() -> String {
    "public".into()
}

/// A StarRocks sink.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StarRocksSinkSpec {
    #[serde(rename = "type")]
    pub type_tag: SinkType,

    /// StarRocks FE HTTP URL (e.g. `http://host:8030`).
    pub url: String,

    /// StarRocks FE MySQL protocol port.
    #[serde(default = "default_mysql_port")]
    pub mysql_port: u16,

    /// Target database.
    pub database: String,

    #[serde(default = "default_sr_user")]
    pub user: String,

    #[serde(default)]
    pub password: String,
}

fn default_mysql_port() -> u16 {
    9030
}
fn default_sr_user() -> String {
    "root".into()
}

/// A Snowflake sink (cloud-only).
///
/// Custom `Debug` implementation to redact password.
#[allow(dead_code)]
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnowflakeSinkSpec {
    #[serde(rename = "type")]
    pub type_tag: SinkType,

    /// Snowflake account identifier (e.g. "xy12345.us-east-1").
    pub account: String,

    pub user: String,

    /// Password or use JWT key via `private_key_path`.
    pub password: String,

    pub database: String,

    /// Target schema (defaults to "PUBLIC").
    #[serde(default = "default_sf_schema", rename = "schema")]
    pub schema_name: String,

    pub warehouse: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    /// Optional RSA key path for JWT auth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key_path: Option<String>,

    /// If true, DELETEs become `_DBMAZZ_IS_DELETED=true`.
    #[serde(default = "default_true")]
    pub soft_delete: bool,
}

fn default_sf_schema() -> String {
    "PUBLIC".into()
}
fn default_true() -> bool {
    true
}

impl fmt::Debug for SnowflakeSinkSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnowflakeSinkSpec")
            .field("type_tag", &self.type_tag)
            .field("account", &self.account)
            .field("user", &self.user)
            .field("password", &"***")
            .field("database", &self.database)
            .field("schema_name", &self.schema_name)
            .field("warehouse", &self.warehouse)
            .field("role", &self.role)
            .field("private_key_path", &self.private_key_path)
            .field("soft_delete", &self.soft_delete)
            .finish()
    }
}

// ── Type discriminators ────────────────────────────────────────────────────

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Postgres,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SinkType {
    Postgres,
    Starrocks,
    Snowflake,
}

// ── Discriminated unions ───────────────────────────────────────────────────

/// Source specification — currently only PostgreSQL.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SourceSpec {
    Postgres(PostgresSourceInner),
}

/// Inner fields of PostgresSourceSpec (without the `type` tag, since
/// `SourceSpec` already carries it via `#[serde(tag = "type")]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceInner {
    pub url: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<String>,

    #[serde(default = "default_slot")]
    pub replication_slot: String,

    #[serde(default = "default_publication")]
    pub publication: String,

    pub tables: Vec<String>,
}

impl PostgresSourceInner {
    pub fn validate(&self) -> Result<(), String> {
        validate_pg_ident(&self.replication_slot, "replication_slot")?;
        validate_pg_ident(&self.publication, "publication")?;
        if self.tables.is_empty() {
            return Err("tables cannot be empty".into());
        }
        let mut seen = std::collections::HashSet::new();
        for t in &self.tables {
            let t = t.trim();
            if t.is_empty() {
                return Err("table name cannot be empty".into());
            }
            if !seen.insert(t) {
                return Err(format!("duplicate table {t:?}"));
            }
        }
        Ok(())
    }
}

impl SourceSpec {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            SourceSpec::Postgres(inner) => inner.validate(),
        }
    }

    pub fn tables(&self) -> &[String] {
        match self {
            SourceSpec::Postgres(inner) => &inner.tables,
        }
    }

    pub fn url(&self) -> &str {
        match self {
            SourceSpec::Postgres(inner) => &inner.url,
        }
    }
}

/// Sink specification — PostgreSQL, StarRocks, or Snowflake.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SinkSpec {
    Postgres(PostgresSinkInner),
    Starrocks(StarRocksSinkInner),
    Snowflake(SnowflakeSinkInner),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSinkInner {
    pub url: String,
    pub database: String,
    #[serde(default = "default_pg_schema", rename = "schema")]
    pub schema_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StarRocksSinkInner {
    pub url: String,
    #[serde(default = "default_mysql_port")]
    pub mysql_port: u16,
    pub database: String,
    #[serde(default = "default_sr_user")]
    pub user: String,
    #[serde(default)]
    pub password: String,
}

/// Snowflake sink inner — custom Debug to redact password.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnowflakeSinkInner {
    pub account: String,
    pub user: String,
    pub password: String,
    pub database: String,
    #[serde(default = "default_sf_schema", rename = "schema")]
    pub schema_name: String,
    pub warehouse: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key_path: Option<String>,
    #[serde(default = "default_true")]
    pub soft_delete: bool,
}

impl fmt::Debug for SnowflakeSinkInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnowflakeSinkInner")
            .field("account", &self.account)
            .field("user", &self.user)
            .field("password", &"***")
            .field("database", &self.database)
            .field("schema_name", &self.schema_name)
            .field("warehouse", &self.warehouse)
            .field("role", &self.role)
            .field("private_key_path", &self.private_key_path)
            .field("soft_delete", &self.soft_delete)
            .finish()
    }
}

impl SinkSpec {
    pub fn sink_type_name(&self) -> &'static str {
        match self {
            SinkSpec::Postgres(_) => "postgres",
            SinkSpec::Starrocks(_) => "starrocks",
            SinkSpec::Snowflake(_) => "snowflake",
        }
    }

    #[allow(dead_code)]
    pub fn database(&self) -> &str {
        match self {
            SinkSpec::Postgres(s) => &s.database,
            SinkSpec::Starrocks(s) => &s.database,
            SinkSpec::Snowflake(s) => &s.database,
        }
    }

    #[allow(dead_code)]
    pub fn url(&self) -> &str {
        match self {
            SinkSpec::Postgres(s) => &s.url,
            SinkSpec::Starrocks(s) => &s.url,
            SinkSpec::Snowflake(s) => &s.account,
        }
    }
}

// ── Pipeline settings ──────────────────────────────────────────────────────

/// Tuning knobs for the dbmazz daemon.
/// Maps 1:1 to env vars that dbmazz reads from `Config::from_env()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSettings {
    #[serde(default = "default_flush_size")]
    pub flush_size: u32,

    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u32,

    #[serde(default = "default_true")]
    pub do_snapshot: bool,

    #[serde(default = "default_chunk_size")]
    pub snapshot_chunk_size: u32,

    #[serde(default = "default_parallel_workers")]
    pub snapshot_parallel_workers: u32,

    #[serde(default)]
    pub initial_snapshot_only: bool,

    #[serde(default = "default_rust_log")]
    pub rust_log: String,

    #[serde(default = "default_sf_flush_files")]
    pub snowflake_flush_files: u32,

    #[serde(default = "default_sf_flush_bytes")]
    pub snowflake_flush_bytes: u64,
}

fn default_flush_size() -> u32 {
    2000
}
fn default_flush_interval() -> u32 {
    2000
}
fn default_chunk_size() -> u32 {
    10000
}
fn default_parallel_workers() -> u32 {
    2
}
fn default_rust_log() -> String {
    "info".into()
}
fn default_sf_flush_files() -> u32 {
    1
}
fn default_sf_flush_bytes() -> u64 {
    104_857_600
}

impl Default for PipelineSettings {
    fn default() -> Self {
        Self {
            flush_size: default_flush_size(),
            flush_interval_ms: default_flush_interval(),
            do_snapshot: true,
            snapshot_chunk_size: default_chunk_size(),
            snapshot_parallel_workers: default_parallel_workers(),
            initial_snapshot_only: false,
            rust_log: default_rust_log(),
            snowflake_flush_files: default_sf_flush_files(),
            snowflake_flush_bytes: default_sf_flush_bytes(),
        }
    }
}

impl PipelineSettings {
    /// Render as `KEY=value` lines for the `.env` file.
    pub fn to_env_lines(&self) -> Vec<String> {
        vec![
            format!("FLUSH_SIZE={}", self.flush_size),
            format!("FLUSH_INTERVAL_MS={}", self.flush_interval_ms),
            format!("DO_SNAPSHOT={}", if self.do_snapshot { "true" } else { "false" }),
            format!("SNAPSHOT_CHUNK_SIZE={}", self.snapshot_chunk_size),
            format!("SNAPSHOT_PARALLEL_WORKERS={}", self.snapshot_parallel_workers),
            format!(
                "INITIAL_SNAPSHOT_ONLY={}",
                if self.initial_snapshot_only { "true" } else { "false" }
            ),
            format!("RUST_LOG={}", self.rust_log),
            format!("SINK_SNOWFLAKE_FLUSH_FILES={}", self.snowflake_flush_files),
            format!("SINK_SNOWFLAKE_FLUSH_BYTES={}", self.snowflake_flush_bytes),
        ]
    }
}

// ── Top-level container ────────────────────────────────────────────────────

/// Top-level structure of `e2e/ez-cdc.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasourcesFile {
    #[serde(default)]
    pub settings: PipelineSettings,

    #[serde(default)]
    pub sources: HashMap<String, SourceSpec>,

    #[serde(default)]
    pub sinks: HashMap<String, SinkSpec>,
}

impl Default for DatasourcesFile {
    fn default() -> Self {
        Self {
            settings: PipelineSettings::default(),
            sources: HashMap::new(),
            sinks: HashMap::new(),
        }
    }
}

impl DatasourcesFile {
    /// Validate all datasource names and specs.
    pub fn validate(&self) -> Result<(), String> {
        for name in self.sources.keys() {
            validate_datasource_name(name)?;
        }
        for name in self.sinks.keys() {
            validate_datasource_name(name)?;
        }
        for (name, spec) in &self.sources {
            spec.validate().map_err(|e| format!("source {name:?}: {e}"))?;
        }
        Ok(())
    }

    /// True if at least one source AND one sink are configured.
    pub fn has_any(&self) -> bool {
        !self.sources.is_empty() && !self.sinks.is_empty()
    }

    /// True if no sources and no sinks are configured.
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty() && self.sinks.is_empty()
    }

    pub fn list_source_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.sources.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn list_sink_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.sinks.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn get_source(&self, name: &str) -> Result<&SourceSpec, String> {
        self.sources.get(name).ok_or_else(|| {
            let available = self.list_source_names().join(", ");
            let available = if available.is_empty() {
                "(none configured)".to_string()
            } else {
                available
            };
            format!("source datasource {name:?} not found. Available: {available}")
        })
    }

    pub fn get_sink(&self, name: &str) -> Result<&SinkSpec, String> {
        self.sinks.get(name).ok_or_else(|| {
            let available = self.list_sink_names().join(", ");
            let available = if available.is_empty() {
                "(none configured)".to_string()
            } else {
                available
            };
            format!("sink datasource {name:?} not found. Available: {available}")
        })
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_YAML: &str = r#"
settings:
  flush_size: 2000
  flush_interval_ms: 2000
  do_snapshot: true
  snapshot_chunk_size: 10000
  snapshot_parallel_workers: 2
  initial_snapshot_only: false
  rust_log: info
  snowflake_flush_files: 1
  snowflake_flush_bytes: 104857600
sources:
  demo-pg:
    type: postgres
    url: postgres://postgres:postgres@localhost:15432/dbmazz
    seed: postgres-seed.sql
    replication_slot: dbmazz_slot
    publication: dbmazz_pub
    tables:
      - orders
      - order_items
sinks:
  demo-starrocks:
    type: starrocks
    url: http://localhost:18030
    mysql_port: 19030
    database: dbmazz
    user: root
    password: ''
  demo-pg-target:
    type: postgres
    url: postgres://postgres:postgres@localhost:25432/dbmazz_target
    database: dbmazz_target
    schema: public
"#;

    #[test]
    fn deserialize_sample_yaml() {
        let file: DatasourcesFile = serde_yml::from_str(SAMPLE_YAML).unwrap();

        // Settings
        assert_eq!(file.settings.flush_size, 2000);
        assert_eq!(file.settings.flush_interval_ms, 2000);
        assert!(file.settings.do_snapshot);
        assert_eq!(file.settings.snapshot_chunk_size, 10000);

        // Sources
        assert_eq!(file.sources.len(), 1);
        let src = file.get_source("demo-pg").unwrap();
        assert_eq!(src.url(), "postgres://postgres:postgres@localhost:15432/dbmazz");
        assert_eq!(src.tables(), &["orders", "order_items"]);

        // Sinks
        assert_eq!(file.sinks.len(), 2);

        match file.get_sink("demo-starrocks").unwrap() {
            SinkSpec::Starrocks(sr) => {
                assert_eq!(sr.url, "http://localhost:18030");
                assert_eq!(sr.mysql_port, 19030);
                assert_eq!(sr.database, "dbmazz");
                assert_eq!(sr.user, "root");
            }
            other => panic!("expected StarRocks, got {other:?}"),
        }

        match file.get_sink("demo-pg-target").unwrap() {
            SinkSpec::Postgres(pg) => {
                assert_eq!(pg.database, "dbmazz_target");
                assert_eq!(pg.schema_name, "public");
            }
            other => panic!("expected Postgres, got {other:?}"),
        }
    }

    #[test]
    fn validate_names() {
        assert!(validate_datasource_name("demo-pg").is_ok());
        assert!(validate_datasource_name("my_sink_01").is_ok());
        assert!(validate_datasource_name("").is_err());
        assert!(validate_datasource_name("Demo-PG").is_err()); // uppercase
        assert!(validate_datasource_name("-bad").is_err()); // starts with hyphen
    }

    #[test]
    fn validate_source_spec() {
        let file: DatasourcesFile = serde_yml::from_str(SAMPLE_YAML).unwrap();
        assert!(file.validate().is_ok());
    }

    #[test]
    fn pipeline_settings_defaults() {
        let settings = PipelineSettings::default();
        assert_eq!(settings.flush_size, 2000);
        assert_eq!(settings.flush_interval_ms, 2000);
        assert!(settings.do_snapshot);
        assert!(!settings.initial_snapshot_only);
        assert_eq!(settings.rust_log, "info");
    }

    #[test]
    fn pipeline_to_env_lines() {
        let settings = PipelineSettings::default();
        let lines = settings.to_env_lines();
        assert!(lines.contains(&"FLUSH_SIZE=2000".to_string()));
        assert!(lines.contains(&"DO_SNAPSHOT=true".to_string()));
        assert!(lines.contains(&"RUST_LOG=info".to_string()));
    }

    #[test]
    fn datasources_file_helpers() {
        let file: DatasourcesFile = serde_yml::from_str(SAMPLE_YAML).unwrap();
        assert!(file.has_any());
        assert!(!file.is_empty());
        assert_eq!(file.list_source_names(), vec!["demo-pg"]);
        assert_eq!(
            file.list_sink_names(),
            vec!["demo-pg-target", "demo-starrocks"]
        );
        assert!(file.get_source("nonexistent").is_err());
        assert!(file.get_sink("nonexistent").is_err());
    }

    #[test]
    fn snowflake_debug_redacts_password() {
        let sf = SnowflakeSinkInner {
            account: "test".into(),
            user: "user".into(),
            password: "super_secret_123".into(),
            database: "db".into(),
            schema_name: "PUBLIC".into(),
            warehouse: "wh".into(),
            role: None,
            private_key_path: None,
            soft_delete: true,
        };
        let debug = format!("{sf:?}");
        assert!(!debug.contains("super_secret_123"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn roundtrip_serialization() {
        let file: DatasourcesFile = serde_yml::from_str(SAMPLE_YAML).unwrap();
        let yaml = serde_yml::to_string(&file).unwrap();
        let file2: DatasourcesFile = serde_yml::from_str(&yaml).unwrap();
        assert_eq!(file.sources.len(), file2.sources.len());
        assert_eq!(file.sinks.len(), file2.sinks.len());
    }
}
