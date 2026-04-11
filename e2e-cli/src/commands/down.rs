use std::path::Path;

use console::style;

use crate::compose::{builder, runner};
use crate::tui::report::print_step;

pub async fn run_down(config_path: &Path, keep_volumes: bool) -> anyhow::Result<()> {
    let infra_path = builder::infra_compose_path();

    if !infra_path.exists() {
        println!(
            "  {} No infra compose found. Nothing to stop.",
            style("\u{2713}").dim()
        );
        return Ok(());
    }

    print_step("Stopping infrastructure containers...");
    runner::down(&infra_path, !keep_volumes, &[])?;

    // Also stop any running dbmazz containers from pair composes.
    let cache_root = infra_path
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or(Path::new("."));

    if cache_root.exists() {
        if let Ok(entries) = std::fs::read_dir(cache_root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && path.file_name().map_or(false, |n| n != "_infra") {
                    let compose = path.join("compose.yml");
                    if compose.exists() && runner::is_running(&compose) {
                        let dir_name = path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy();
                        print_step(&format!("Stopping dbmazz stack: {dir_name}"));
                        let _ = runner::down(&compose, !keep_volumes, &[]);
                    }
                }
            }
        }
    }

    println!();
    println!(
        "  {} Infrastructure stopped.",
        style("\u{2713}").green().bold()
    );
    if keep_volumes {
        println!("    Volumes preserved (--keep-volumes).");
    }
    println!();

    // Suppress unused config_path warning -- config_path is used to stay
    // consistent with other commands but down only needs the cache.
    let _ = config_path;

    Ok(())
}
