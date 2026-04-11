use std::path::Path;

use console::style;

use crate::compose::{builder, runner};

pub fn run_logs(_config_path: &Path, service: Option<String>, follow: bool, tail: u32) -> anyhow::Result<()> {
    // `ez-cdc logs dbmazz` — show dbmazz container logs.
    // The dbmazz service lives in a pair compose, not infra. We check all
    // cached pair compose directories for a running dbmazz container.
    if service.as_deref() == Some("dbmazz") {
        return show_dbmazz_docker_logs(follow, tail);
    }

    let infra_path = builder::infra_compose_path();

    if !infra_path.exists() {
        println!(
            "  {} No infra compose found. Run {} first.",
            style("\u{2717}").red(),
            style("ez-cdc up").bold()
        );
        return Ok(());
    }

    runner::logs(&infra_path, service.as_deref(), follow, tail)?;
    Ok(())
}

/// Find the running dbmazz pair compose and show its logs.
fn show_dbmazz_docker_logs(follow: bool, tail: u32) -> anyhow::Result<()> {
    let compose_cache = crate::paths::CLI_DIR.join(".cache").join("compose");

    if !compose_cache.exists() {
        println!(
            "  {} No compose cache found. Run {} first.",
            style("\u{2717}").red(),
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
                style("→").dim(),
                style(compose_file.display()).dim(),
            );
            runner::logs(&compose_file, Some("dbmazz"), follow, tail)?;
            return Ok(());
        }
    }

    // No running container — show the most recent pair compose logs (stopped).
    // This is useful for post-mortem after a verify run.
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
                style("→").dim(),
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
        style("ez-cdc verify").bold(),
    );
    Ok(())
}
