use std::path::Path;

use console::style;

use crate::compose::runner;

/// Tail the logs of the running dbmazz container.
///
/// The CLI only manages the dbmazz container itself — source and sink
/// containers are out of scope (users bring their own databases or
/// run docker-compose separately). `--service` is kept for backwards
/// compatibility but only `dbmazz` is supported.
pub fn run_logs(_config_path: &Path, service: Option<String>, follow: bool, tail: u32) -> anyhow::Result<()> {
    if let Some(name) = service.as_deref() {
        if name != "dbmazz" {
            anyhow::bail!(
                "only the `dbmazz` service is managed by the CLI — run \
                 `docker logs <container>` directly for other services."
            );
        }
    }
    show_dbmazz_docker_logs(follow, tail)
}

/// Find the running (or most recent) dbmazz pair compose in the cache
/// directory and tail its logs.
fn show_dbmazz_docker_logs(follow: bool, tail: u32) -> anyhow::Result<()> {
    let compose_cache = crate::paths::cache_dir().join("compose");

    if !compose_cache.exists() {
        println!(
            "  {} No compose cache found. Run {} or {} first.",
            style("\u{2717}").red(),
            style("ez-cdc quickstart").bold(),
            style("ez-cdc verify").bold(),
        );
        return Ok(());
    }

    // Scan pair directories for a running dbmazz container.
    for entry in std::fs::read_dir(&compose_cache)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() || path.file_name().map_or(true, |n| n == "_infra") {
            continue;
        }
        let compose_file = path.join("compose.yml");
        if compose_file.exists() && runner::is_service_running(&compose_file, "dbmazz") {
            println!(
                "  {} {}",
                style("\u{2192}").dim(),
                style(compose_file.display()).dim(),
            );
            runner::logs(&compose_file, Some("dbmazz"), follow, tail)?;
            return Ok(());
        }
    }

    // No running container — show the most recent pair compose logs
    // (stopped). Useful for post-mortem after a verify run.
    for entry in std::fs::read_dir(&compose_cache)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() || path.file_name().map_or(true, |n| n == "_infra") {
            continue;
        }
        let compose_file = path.join("compose.yml");
        if compose_file.exists() {
            println!(
                "  {} {} {}",
                style("\u{2192}").dim(),
                style("(stopped)").yellow(),
                style(compose_file.display()).dim(),
            );
            runner::logs(&compose_file, Some("dbmazz"), follow, tail)?;
            return Ok(());
        }
    }

    println!(
        "  {} No dbmazz compose found. Run {} first.",
        style("\u{2717}").red(),
        style("ez-cdc quickstart").bold(),
    );
    Ok(())
}
