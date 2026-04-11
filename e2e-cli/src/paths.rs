use std::path::{Path, PathBuf};

use once_cell::sync::Lazy;

/// Root of the e2e-cli crate (`<repo>/e2e-cli/`).
pub static CLI_DIR: Lazy<PathBuf> = Lazy::new(|| {
    if let Some(manifest) = option_env!("CARGO_MANIFEST_DIR") {
        return PathBuf::from(manifest);
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            let mut p = parent.to_path_buf();
            for _ in 0..5 {
                if p.join("Cargo.toml").exists() && p.join("fixtures").exists() {
                    return p;
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
});

/// Repository root (`<repo>/`), parent of `e2e-cli/`.
pub static REPO_ROOT: Lazy<PathBuf> = Lazy::new(|| {
    CLI_DIR
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf()
});

/// Fixtures directory (`<repo>/e2e-cli/fixtures/`).
pub static FIXTURES_DIR: Lazy<PathBuf> = Lazy::new(|| CLI_DIR.join("fixtures"));

/// Compose cache directory (`<repo>/e2e-cli/.cache/compose/`).
pub static CACHE_DIR: Lazy<PathBuf> = Lazy::new(|| CLI_DIR.join(".cache").join("compose"));

/// PostgreSQL seed SQL file.
pub fn postgres_seed_sql() -> PathBuf {
    FIXTURES_DIR.join("postgres-seed.sql")
}

/// StarRocks init script.
pub fn starrocks_init_sh() -> PathBuf {
    FIXTURES_DIR.join("starrocks-init.sh")
}

/// Default config file path (`<repo>/e2e-cli/ez-cdc.yaml`).
pub fn default_config_path() -> PathBuf {
    CLI_DIR.join("ez-cdc.yaml")
}

/// Path to the dbmazz daemon log file (`<repo>/e2e-cli/.cache/dbmazz.log`).
pub fn dbmazz_log_path() -> PathBuf {
    CLI_DIR.join(".cache").join("dbmazz.log")
}
