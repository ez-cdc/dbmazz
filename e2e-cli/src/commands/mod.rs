pub mod clean;
pub mod datasource;
pub mod logs;
pub mod quickstart;
pub mod status;
pub mod verify_cmd;

use std::path::Path;

use crate::config::schema::{SinkSpec, SourceSpec};
use crate::config::store::DatasourceStore;
use crate::tui::prompts;

/// Resolve a (source_name, source_spec, sink_name, sink_spec) tuple from CLI args.
///
/// - If both `source` and `sink` are provided, use them directly.
/// - If only one source/sink exists, auto-pick it.
/// - If multiple and TTY, prompt the user.
/// - If multiple and not TTY, return an error.
pub fn resolve_pair(
    store: &mut DatasourceStore,
    source: Option<String>,
    sink: Option<String>,
) -> anyhow::Result<(String, SourceSpec, String, SinkSpec)> {
    let src_names = store.list_sources()?;
    let sk_names = store.list_sinks()?;

    if src_names.is_empty() {
        anyhow::bail!(
            "no sources configured. Run `ez-cdc datasource init` for a starter config, then `ez-cdc datasource add` to add one."
        );
    }
    if sk_names.is_empty() {
        anyhow::bail!(
            "no sinks configured. Run `ez-cdc datasource init` for a starter config, then `ez-cdc datasource add` to add one."
        );
    }

    // Resolve source name.
    let src_name = match source {
        Some(name) => {
            if !src_names.contains(&name) {
                anyhow::bail!("source '{}' not found. Available: {}", name, src_names.join(", "));
            }
            name
        }
        None if src_names.len() == 1 => src_names[0].clone(),
        None => {
            if !prompts::is_interactive() {
                anyhow::bail!(
                    "multiple sources available ({}). Specify --source or run interactively.",
                    src_names.join(", ")
                );
            }
            let items: Vec<(String, String, String)> = src_names
                .iter()
                .map(|n| (n.clone(), n.clone(), String::new()))
                .collect();
            prompts::select("Select source:", items)?
        }
    };

    // Resolve sink name.
    let sk_name = match sink {
        Some(name) => {
            if !sk_names.contains(&name) {
                anyhow::bail!("sink '{}' not found. Available: {}", name, sk_names.join(", "));
            }
            name
        }
        None if sk_names.len() == 1 => sk_names[0].clone(),
        None => {
            if !prompts::is_interactive() {
                anyhow::bail!(
                    "multiple sinks available ({}). Specify --sink or run interactively.",
                    sk_names.join(", ")
                );
            }
            let items: Vec<(String, String, String)> = sk_names
                .iter()
                .map(|n| (n.clone(), n.clone(), String::new()))
                .collect();
            prompts::select("Select sink:", items)?
        }
    };

    let src_spec = store.get_source(&src_name)?;
    let sk_spec = store.get_sink(&sk_name)?;
    Ok((src_name, src_spec, sk_name, sk_spec))
}

/// Redact the password in a URL, replacing it with `***`.
pub fn redact_url_password(url_str: &str) -> String {
    match url::Url::parse(url_str) {
        Ok(mut parsed) => {
            if parsed.password().is_some() {
                let _ = parsed.set_password(Some("***"));
            }
            parsed.to_string()
        }
        Err(_) => url_str.to_string(),
    }
}

/// Build a DatasourceStore from a config file path.
pub fn load_store(config_path: &Path) -> anyhow::Result<DatasourceStore> {
    let mut store = DatasourceStore::new(config_path, vec![]);
    store.load(false)?;
    Ok(store)
}

/// Returns the dbmazz image reference to use.
///
/// By default this pins the image to the CLI's own version so a
/// `v1.6.0` CLI always pulls `ghcr.io/ez-cdc/dbmazz:1.6.0`. The
/// workflow patches `e2e-cli/Cargo.toml` with the calculated release
/// version before building, so `CARGO_PKG_VERSION` is always aligned
/// with the image tag that docker-publish has already pushed.
///
/// For local dev without a matching release (e.g. testing a patched
/// daemon), set `DBMAZZ_IMAGE=my-local-image:dev` to override.
pub fn dbmazz_image() -> String {
    std::env::var("DBMAZZ_IMAGE")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("ghcr.io/ez-cdc/dbmazz:{}", env!("CARGO_PKG_VERSION")))
}

/// Check if a Docker image exists locally.
pub fn docker_image_exists(image: &str) -> bool {
    std::process::Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Pull the dbmazz image from its registry.
fn pull_dbmazz_image(image: &str) -> anyhow::Result<()> {
    let status = std::process::Command::new("docker")
        .args(["pull", image])
        .status()
        .map_err(|e| anyhow::anyhow!("failed to run docker pull: {e}"))?;

    if !status.success() {
        anyhow::bail!(
            "docker pull {} failed (exit {:?}).\n  \
             If you are testing a local image, set DBMAZZ_IMAGE to its tag.",
            image,
            status.code()
        );
    }
    Ok(())
}

/// Ensure the dbmazz image is available locally, pulling it if needed.
///
/// Returns `Ok(true)` if a pull was triggered, `Ok(false)` if the image
/// was already present. When `force_pull` is true, always pulls even
/// when the image exists locally.
pub fn ensure_dbmazz_image(force_pull: bool) -> anyhow::Result<bool> {
    let image = dbmazz_image();
    if !force_pull && docker_image_exists(&image) {
        return Ok(false);
    }
    pull_dbmazz_image(&image)?;
    Ok(true)
}

/// Build a DatasourceStore, allowing the file to be missing.
pub fn load_store_or_empty(config_path: &Path) -> anyhow::Result<DatasourceStore> {
    let mut store = DatasourceStore::new(config_path, vec![]);
    store.load(true)?;
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_password_in_postgres_url() {
        let url = "postgres://user:secret@localhost:5432/db";
        let redacted = redact_url_password(url);
        assert!(!redacted.contains("secret"));
        assert!(redacted.contains("***"));
        assert!(redacted.contains("user"));
        assert!(redacted.contains("localhost"));
    }

    #[test]
    fn redact_no_password() {
        let url = "http://localhost:8030";
        let redacted = redact_url_password(url);
        assert_eq!(redacted, "http://localhost:8030/");
    }

    #[test]
    fn redact_invalid_url_passthrough() {
        let url = "not-a-url";
        let redacted = redact_url_password(url);
        assert_eq!(redacted, "not-a-url");
    }
}
