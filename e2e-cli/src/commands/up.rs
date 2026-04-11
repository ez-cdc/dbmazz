use std::path::Path;

use console::style;

use crate::commands::{load_store, resolve_pair};
use crate::compose::{builder, runner};
use crate::tui::report::print_step;

pub async fn run_up(config_path: &Path, rebuild: bool) -> anyhow::Result<()> {
    let mut store = load_store(config_path)?;
    let data = store.data()?.clone();

    if !data.has_any() {
        anyhow::bail!(
            "no datasources configured. Run `ez-cdc datasource init` to add demo datasources."
        );
    }

    // Build infra compose only for the configured datasources.
    let infra_path = builder::build_infra_compose(&data.sources, &data.sinks)
        .map_err(|e| anyhow::anyhow!(e))?;

    let services = builder::list_infra_services(&data.sources, &data.sinks);
    print_step("Starting infrastructure containers:");
    for svc in &services {
        println!("    {} {}", style("-").dim(), svc);
    }
    println!();

    runner::up(&infra_path, true, rebuild, false, &[])?;

    println!();
    println!(
        "  {} Infrastructure is ready.",
        style("✓").green().bold()
    );
    println!();

    Ok(())
}
