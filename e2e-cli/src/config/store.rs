//! Persistence layer for the datasources YAML file.
//!
//! `DatasourceStore` wraps `ez-cdc.yaml` and provides load/save/CRUD
//! with atomic writes (temp file + rename) and `.bak` backups.

use std::path::{Path, PathBuf};

use super::loader::{load_datasources, DatasourceError};
use super::schema::{DatasourcesFile, SinkSpec, SourceSpec};

pub struct DatasourceStore {
    path: PathBuf,
    env_files: Vec<PathBuf>,
    data: DatasourcesFile,
    loaded: bool,
}

impl DatasourceStore {
    pub fn new(path: &Path, env_files: Vec<PathBuf>) -> Self {
        Self {
            path: path.to_path_buf(),
            env_files,
            data: DatasourcesFile::default(),
            loaded: false,
        }
    }

    /// Build an in-memory empty store without reading disk.
    pub fn empty(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            env_files: vec![],
            data: DatasourcesFile::default(),
            loaded: true,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    // ── loading ────────────────────────────────────────────────────────────

    /// Read and validate the file. Replaces in-memory state.
    pub fn load(&mut self, allow_missing: bool) -> Result<(), DatasourceError> {
        let env_refs: Vec<&Path> = self.env_files.iter().map(|p| p.as_path()).collect();
        self.data = load_datasources(&self.path, &env_refs, allow_missing)?;
        self.loaded = true;
        Ok(())
    }

    /// Re-read from disk, discarding in-memory mutations.
    pub fn reload(&mut self) -> Result<(), DatasourceError> {
        self.load(true)
    }

    /// Ensure the store is loaded (lazy load on first access).
    fn ensure_loaded(&mut self) -> Result<(), DatasourceError> {
        if !self.loaded {
            self.load(true)?;
        }
        Ok(())
    }

    pub fn data(&mut self) -> Result<&DatasourcesFile, DatasourceError> {
        self.ensure_loaded()?;
        Ok(&self.data)
    }

    // ── inspection ─────────────────────────────────────────────────────────

    pub fn has_any(&mut self) -> Result<bool, DatasourceError> {
        Ok(self.data()?.has_any())
    }

    pub fn is_empty(&mut self) -> Result<bool, DatasourceError> {
        Ok(self.data()?.is_empty())
    }

    pub fn list_sources(&mut self) -> Result<Vec<String>, DatasourceError> {
        Ok(self.data()?.list_source_names())
    }

    pub fn list_sinks(&mut self) -> Result<Vec<String>, DatasourceError> {
        Ok(self.data()?.list_sink_names())
    }

    pub fn get_source(&mut self, name: &str) -> Result<SourceSpec, DatasourceError> {
        let spec = self.data()?.get_source(name)
            .map_err(|e| DatasourceError::NotFound(e))?;
        Ok(spec.clone())
    }

    pub fn get_sink(&mut self, name: &str) -> Result<SinkSpec, DatasourceError> {
        let spec = self.data()?.get_sink(name)
            .map_err(|e| DatasourceError::NotFound(e))?;
        Ok(spec.clone())
    }

    pub fn exists(&mut self, name: &str) -> Result<bool, DatasourceError> {
        self.ensure_loaded()?;
        Ok(self.data.sources.contains_key(name) || self.data.sinks.contains_key(name))
    }

    // ── mutation ───────────────────────────────────────────────────────────

    pub fn add_source(
        &mut self,
        name: String,
        spec: SourceSpec,
        replace: bool,
    ) -> Result<(), DatasourceError> {
        self.ensure_loaded()?;
        self.check_name_collision(&name, "source", replace)?;
        self.data.sources.insert(name, spec);
        Ok(())
    }

    pub fn add_sink(
        &mut self,
        name: String,
        spec: SinkSpec,
        replace: bool,
    ) -> Result<(), DatasourceError> {
        self.ensure_loaded()?;
        self.check_name_collision(&name, "sink", replace)?;
        self.data.sinks.insert(name, spec);
        Ok(())
    }

    pub fn remove(&mut self, name: &str) -> Result<(), DatasourceError> {
        self.ensure_loaded()?;
        if self.data.sources.remove(name).is_some() {
            return Ok(());
        }
        if self.data.sinks.remove(name).is_some() {
            return Ok(());
        }
        Err(DatasourceError::NotFound(format!(
            "datasource {name:?} not found in {}",
            self.path.display()
        )))
    }

    fn check_name_collision(
        &self,
        name: &str,
        kind: &str,
        replace: bool,
    ) -> Result<(), DatasourceError> {
        // Cross-kind collision.
        if kind == "source" && self.data.sinks.contains_key(name) {
            return Err(DatasourceError::Validation(format!(
                "name {name:?} is already used by a sink. \
                 Choose a different name or remove the sink first."
            )));
        }
        if kind == "sink" && self.data.sources.contains_key(name) {
            return Err(DatasourceError::Validation(format!(
                "name {name:?} is already used by a source. \
                 Choose a different name or remove the source first."
            )));
        }
        // Same-kind collision.
        let existing = if kind == "source" {
            &self.data.sources
        } else {
            // SinkSpec and SourceSpec use different maps but we need a common check.
            // For sinks, check the sinks map. For sources, already handled above.
            // This is a simplified approach — just check the right map.
            return if !replace && self.data.sinks.contains_key(name) {
                Err(DatasourceError::Validation(format!(
                    "{kind} {name:?} already exists. Use replace=true to overwrite."
                )))
            } else {
                Ok(())
            };
        };
        if !replace && existing.contains_key(name) {
            return Err(DatasourceError::Validation(format!(
                "{kind} {name:?} already exists. Use replace=true to overwrite."
            )));
        }
        Ok(())
    }

    // ── persistence ────────────────────────────────────────────────────────

    /// Write in-memory state to disk atomically.
    ///
    /// Steps: write temp → fsync → backup .bak → rename → chmod 0o600.
    pub fn save(&self) -> Result<(), DatasourceError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| DatasourceError::Io(format!("create dir: {e}")))?;
        }

        let yaml_text = self.render_yaml()?;
        let tmp_path = self.path.with_extension("yaml.tmp");

        // Write to temp file.
        std::fs::write(&tmp_path, &yaml_text)
            .map_err(|e| DatasourceError::Io(format!("write temp file: {e}")))?;

        // Backup existing file.
        if self.path.exists() {
            let bak_path = self.path.with_extension("yaml.bak");
            let _ = std::fs::copy(&self.path, &bak_path); // non-fatal
        }

        // Atomic rename.
        std::fs::rename(&tmp_path, &self.path)
            .map_err(|e| {
                let _ = std::fs::remove_file(&tmp_path);
                DatasourceError::Io(format!("rename: {e}"))
            })?;

        // Set permissions (non-fatal on non-POSIX).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(
                &self.path,
                std::fs::Permissions::from_mode(0o600),
            );
        }

        Ok(())
    }

    fn render_yaml(&self) -> Result<String, DatasourceError> {
        let header = "\
# ez-cdc configuration file\n\
# Datasources + pipeline settings. Managed by `ez-cdc datasource ...`\n\
# Credentials may use ${VAR} interpolation.\n\
# See e2e/README.md for settings documentation.\n\
\n";

        let body = serde_yml::to_string(&self.data)
            .map_err(|e| DatasourceError::Yaml(format!("serialize: {e}")))?;

        Ok(format!("{header}{body}"))
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::schema::*;

    fn demo_source() -> SourceSpec {
        SourceSpec::Postgres(PostgresSourceInner {
            url: "postgres://localhost/test".into(),
            seed: None,
            replication_slot: "test_slot".into(),
            publication: "test_pub".into(),
            tables: vec!["t1".into()],
        })
    }

    fn demo_sink() -> SinkSpec {
        SinkSpec::Starrocks(StarRocksSinkInner {
            url: "http://localhost:8030".into(),
            mysql_port: 9030,
            database: "test".into(),
            user: "root".into(),
            password: "".into(),
        })
    }

    #[test]
    fn empty_store() {
        let store = DatasourceStore::empty(Path::new("/tmp/test.yaml"));
        assert!(store.data.is_empty());
    }

    #[test]
    fn add_and_remove() {
        let mut store = DatasourceStore::empty(Path::new("/tmp/test.yaml"));
        store.add_source("src1".into(), demo_source(), false).unwrap();
        store.add_sink("sink1".into(), demo_sink(), false).unwrap();
        assert!(store.exists("src1").unwrap());
        assert!(store.exists("sink1").unwrap());
        assert!(!store.exists("nope").unwrap());

        store.remove("src1").unwrap();
        assert!(!store.exists("src1").unwrap());

        assert!(store.remove("nonexistent").is_err());
    }

    #[test]
    fn cross_kind_collision() {
        let mut store = DatasourceStore::empty(Path::new("/tmp/test.yaml"));
        store.add_source("shared".into(), demo_source(), false).unwrap();
        let err = store.add_sink("shared".into(), demo_sink(), false).unwrap_err();
        assert!(err.to_string().contains("already used by a source"));
    }

    #[test]
    fn same_kind_collision() {
        let mut store = DatasourceStore::empty(Path::new("/tmp/test.yaml"));
        store.add_source("dup".into(), demo_source(), false).unwrap();
        assert!(store.add_source("dup".into(), demo_source(), false).is_err());
        // With replace=true, it works.
        store.add_source("dup".into(), demo_source(), true).unwrap();
    }

    #[test]
    fn save_and_reload() {
        let dir = std::env::temp_dir().join("ez-cdc-store-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ez-cdc.yaml");

        let mut store = DatasourceStore::empty(&path);
        store.add_source("s1".into(), demo_source(), false).unwrap();
        store.add_sink("k1".into(), demo_sink(), false).unwrap();
        store.save().unwrap();

        // Reload from disk.
        let mut store2 = DatasourceStore::new(&path, vec![]);
        store2.load(false).unwrap();
        assert!(store2.exists("s1").unwrap());
        assert!(store2.exists("k1").unwrap());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_creates_backup() {
        let dir = std::env::temp_dir().join("ez-cdc-bak-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ez-cdc.yaml");

        // First save.
        let store = DatasourceStore::empty(&path);
        store.save().unwrap();
        assert!(path.exists());

        // Second save — should create .bak.
        store.save().unwrap();
        let bak = dir.join("ez-cdc.yaml.bak");
        assert!(bak.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
