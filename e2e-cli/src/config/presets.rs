//! Bundled demo datasources (in-repo dev only).
//!
//! Ready-to-use datasources for the in-repo dev workflow — they point
//! at well-known localhost ports (15432, 18030, 25432) that the
//! project's own docker-compose stack exposes. Used by
//! `ez-cdc datasource init --template demo`. Not useful for standalone
//! installs.

use super::schema::*;
use super::store::DatasourceStore;

/// Which demo sink to include.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DemoSink {
    Postgres,
    StarRocks,
}

/// Build demo datasources with the chosen sink.
pub fn build_demo_datasources(sink: DemoSink) -> DatasourcesFile {
    let mut file = DatasourcesFile::default();

    file.sources.insert(
        "demo-pg".into(),
        SourceSpec::Postgres(PostgresSourceInner {
            url: "postgres://postgres:postgres@localhost:15432/dbmazz".into(),
            seed: Some("postgres-seed.sql".into()),
            replication_slot: "dbmazz_slot".into(),
            publication: "dbmazz_pub".into(),
            tables: vec!["orders".into(), "order_items".into()],
        }),
    );

    match sink {
        DemoSink::StarRocks => {
            file.sinks.insert(
                "demo-starrocks".into(),
                SinkSpec::Starrocks(StarRocksSinkInner {
                    url: "http://localhost:18030".into(),
                    mysql_port: 19030,
                    database: "dbmazz".into(),
                    user: "root".into(),
                    password: String::new(),
                }),
            );
        }
        DemoSink::Postgres => {
            file.sinks.insert(
                "demo-pg-target".into(),
                SinkSpec::Postgres(PostgresSinkInner {
                    url: "postgres://postgres:postgres@localhost:25432/dbmazz_target".into(),
                    database: "dbmazz_target".into(),
                    schema_name: "public".into(),
                }),
            );
        }
    }

    file
}

/// Merge demo datasources into an existing store.
///
/// Existing datasources with the same name are skipped (idempotent).
/// Returns `(added_sources, added_sinks)`.
pub fn merge_demos_into(
    store: &mut DatasourceStore,
    sink: DemoSink,
) -> Result<(Vec<String>, Vec<String>), super::loader::DatasourceError> {
    let demos = build_demo_datasources(sink);

    let mut added_sources = Vec::new();
    for (name, spec) in demos.sources {
        if !store.exists(&name)? {
            store.add_source(name.clone(), spec, false)?;
            added_sources.push(name);
        }
    }

    let mut added_sinks = Vec::new();
    for (name, spec) in demos.sinks {
        if !store.exists(&name)? {
            store.add_sink(name.clone(), spec, false)?;
            added_sinks.push(name);
        }
    }

    Ok((added_sources, added_sinks))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn demo_starrocks_valid() {
        let file = build_demo_datasources(DemoSink::StarRocks);
        assert!(file.has_any());
        assert_eq!(file.sources.len(), 1);
        assert_eq!(file.sinks.len(), 1);
        assert!(file.sinks.contains_key("demo-starrocks"));
        assert!(file.validate().is_ok());
    }

    #[test]
    fn demo_postgres_valid() {
        let file = build_demo_datasources(DemoSink::Postgres);
        assert!(file.has_any());
        assert_eq!(file.sources.len(), 1);
        assert_eq!(file.sinks.len(), 1);
        assert!(file.sinks.contains_key("demo-pg-target"));
        assert!(file.validate().is_ok());
    }

    #[test]
    fn merge_idempotent() {
        let mut store = DatasourceStore::empty(Path::new("/tmp/test.yaml"));

        let (src1, sk1) = merge_demos_into(&mut store, DemoSink::StarRocks).unwrap();
        assert_eq!(src1.len(), 1);
        assert_eq!(sk1.len(), 1);

        let (src2, sk2) = merge_demos_into(&mut store, DemoSink::StarRocks).unwrap();
        assert!(src2.is_empty());
        assert!(sk2.is_empty());
    }
}
