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

/// Compose cache directory (legacy — inside CLI_DIR).
///
/// Do NOT use. Resolves to the manifest dir baked at compile time,
/// which is invalid for binaries distributed via `install.sh`. Use
/// [`cache_dir()`] instead.
#[allow(dead_code)]
pub static CACHE_DIR: Lazy<PathBuf> = Lazy::new(|| CLI_DIR.join(".cache").join("compose"));

/// User-local cache directory for ez-cdc generated artifacts
/// (compose files, .env files, etc.).
///
/// Resolves with the XDG Base Directory spec so it works regardless
/// of how the CLI was installed (curl|sh, cargo install, brew, clone
/// + cargo run, etc.):
///
///  1. `$XDG_CACHE_HOME/ez-cdc`
///  2. `$HOME/.cache/ez-cdc`
///  3. fallback to the current directory's `.cache/ez-cdc`
///
/// This replaces the old `CLI_DIR/.cache/...` path, which was broken
/// for binaries whose `CARGO_MANIFEST_DIR` (baked at compile time)
/// points at the CI runner's filesystem — `Permission denied` when
/// the CLI tries to `mkdir -p /home/runner/...`.
pub fn cache_dir() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_CACHE_HOME") {
        if !xdg.is_empty() {
            return PathBuf::from(xdg).join("ez-cdc");
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        if !home.is_empty() {
            return PathBuf::from(home).join(".cache").join("ez-cdc");
        }
    }
    PathBuf::from(".cache").join("ez-cdc")
}

/// PostgreSQL seed SQL file.
#[allow(dead_code)]
pub fn postgres_seed_sql() -> PathBuf {
    FIXTURES_DIR.join("postgres-seed.sql")
}

/// StarRocks init script.
#[allow(dead_code)]
pub fn starrocks_init_sh() -> PathBuf {
    FIXTURES_DIR.join("starrocks-init.sh")
}

/// Default config file path (`<repo>/e2e-cli/ez-cdc.yaml`).
#[allow(dead_code)]
pub fn default_config_path() -> PathBuf {
    CLI_DIR.join("ez-cdc.yaml")
}

/// Path to the dbmazz daemon log file (`<repo>/e2e-cli/.cache/dbmazz.log`).
#[allow(dead_code)]
pub fn dbmazz_log_path() -> PathBuf {
    CLI_DIR.join(".cache").join("dbmazz.log")
}
