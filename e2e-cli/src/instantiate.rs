//! Spec -> client/backend instantiation helpers.
//!
//! Builds:
//!   - `PostgresSource` for a `SourceSpec`
//!   - `PostgresTarget` / `StarRocksTarget` / `SnowflakeTarget` for a `SinkSpec`
//!
//! Connection details are always taken from the spec's fields directly.
//! Returns unconnected clients -- the caller must call `.connect()`.

use url::Url;

use crate::clients::source_pg::{PostgresSource, SourceClient};
use crate::clients::targets::postgres::PostgresTarget;
use crate::clients::targets::snowflake::SnowflakeTarget;
use crate::clients::targets::starrocks::StarRocksTarget;
use crate::clients::targets::TargetBackend;
use crate::config::schema::{SinkSpec, SourceSpec};

/// Build a `SourceClient` from a `SourceSpec`.
///
/// Returns an unconnected client; caller must call `.connect()`.
pub fn instantiate_source(spec: &SourceSpec) -> anyhow::Result<Box<dyn SourceClient>> {
    match spec {
        SourceSpec::Postgres(inner) => Ok(Box::new(PostgresSource::new(&inner.url))),
    }
}

/// Build a `TargetBackend` from a `SinkSpec`.
///
/// Returns an unconnected backend; caller must call `.connect()`.
pub fn instantiate_backend(spec: &SinkSpec) -> anyhow::Result<Box<dyn TargetBackend>> {
    match spec {
        SinkSpec::Postgres(inner) => {
            Ok(Box::new(PostgresTarget::new(&inner.url, &inner.schema_name)))
        }
        SinkSpec::Starrocks(inner) => {
            let parsed = Url::parse(&inner.url)?;
            let host = parsed.host_str().unwrap_or("localhost");
            Ok(Box::new(StarRocksTarget::new(
                host,
                inner.mysql_port,
                &inner.user,
                &inner.password,
                &inner.database,
            )))
        }
        SinkSpec::Snowflake(inner) => Ok(Box::new(SnowflakeTarget::new(
            &inner.account,
            &inner.user,
            &inner.database,
            &inner.schema_name,
            &inner.warehouse,
            inner.role.as_deref(),
            inner.soft_delete,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::schema::*;

    #[test]
    fn instantiate_postgres_source() {
        let spec = SourceSpec::Postgres(PostgresSourceInner {
            url: "postgres://localhost:5432/test".to_string(),
            seed: None,
            replication_slot: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            tables: vec!["t1".to_string()],
        });
        let client = instantiate_source(&spec).expect("should create source");
        assert_eq!(client.name(), "postgres");
    }

    #[test]
    fn instantiate_postgres_target() {
        let spec = SinkSpec::Postgres(PostgresSinkInner {
            url: "postgres://localhost:5432/target".to_string(),
            database: "target".to_string(),
            schema_name: "public".to_string(),
        });
        let backend = instantiate_backend(&spec).expect("should create backend");
        assert_eq!(backend.name(), "postgres");
    }

    #[test]
    fn instantiate_starrocks_target() {
        let spec = SinkSpec::Starrocks(StarRocksSinkInner {
            url: "http://localhost:8030".to_string(),
            mysql_port: 9030,
            database: "dbmazz".to_string(),
            user: "root".to_string(),
            password: String::new(),
        });
        let backend = instantiate_backend(&spec).expect("should create backend");
        assert_eq!(backend.name(), "starrocks");
    }

    #[test]
    fn instantiate_snowflake_target() {
        let spec = SinkSpec::Snowflake(SnowflakeSinkInner {
            account: "xy12345.us-east-1".to_string(),
            user: "user".to_string(),
            password: "pass".to_string(),
            database: "db".to_string(),
            schema_name: "PUBLIC".to_string(),
            warehouse: "wh".to_string(),
            role: None,
            private_key_path: None,
            soft_delete: true,
        });
        let backend = instantiate_backend(&spec).expect("should create backend");
        assert_eq!(backend.name(), "snowflake");
    }
}
