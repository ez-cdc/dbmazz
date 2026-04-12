use std::path::Path;
use std::time::Duration;

use console::style;

use crate::clients::dbmazz::DbmazzClient;
use crate::commands::{dbmazz_image, ensure_dbmazz_image, load_store, resolve_pair};
use crate::compose::{builder, runner};
use crate::instantiate::instantiate_backend;
use crate::preflight;
use crate::tui::dashboard::QuickstartDashboard;
use crate::tui::report::print_step;

pub async fn run_quickstart(
    config_path: &Path,
    source: Option<String>,
    sink: Option<String>,
    _keep_up: bool,
    rebuild: bool,
) -> anyhow::Result<()> {
    let mut store = load_store(config_path)?;
    let (src_name, src_spec, sk_name, sk_spec) = resolve_pair(&mut store, source, sink)?;
    let data = store.data()?.clone();

    let display_name = format!("{} → {}", src_name, sk_name);
    println!(
        "  {} {} {} {}",
        style("Quickstart:").bold(),
        style(&src_name).cyan(),
        style("→").dim(),
        style(&sk_name).cyan(),
    );
    println!();

    // Step 1: validate that source and sink are reachable.
    print_step("Testing connectivity...");
    if !preflight::check_connectivity(&src_spec, &sk_spec, &src_name, &sk_name).await {
        anyhow::bail!(
            "source or sink unreachable. Make sure your databases are \
             running and the URLs in your ez-cdc.yaml are correct."
        );
    }

    // Step 2: ensure the dbmazz image is available, then start the container.
    let pulled = ensure_dbmazz_image(rebuild)?;
    if pulled {
        println!(
            "    {} dbmazz image pulled ({})",
            style("✓").green(),
            dbmazz_image(),
        );
    }

    let (compose_path, _env_path) = builder::build_compose_for_pair(
        &src_name, &src_spec, &sk_name, &sk_spec, &data.settings,
    )
    .map_err(|e| anyhow::anyhow!(e))?;

    print_step("Starting dbmazz (Docker)...");
    runner::up(
        &compose_path,
        true,   // wait for healthy
        false,  // image already ensured
        true,   // force_recreate
        &["dbmazz"],
    )
    .map_err(|e| anyhow::anyhow!("failed to start dbmazz: {e}"))?;

    // Step 3: wait for dbmazz to become healthy.
    print_step("Waiting for dbmazz to become healthy...");
    let dbmazz = DbmazzClient::with_defaults("http://localhost:8080");
    dbmazz
        .wait_healthy(Duration::from_secs(60), Duration::from_secs(2))
        .await
        .map_err(|e| anyhow::anyhow!("dbmazz did not become healthy: {e}"))?;

    // Step 4: connect to target for the dashboard.
    let mut target = instantiate_backend(&sk_spec)?;
    target.connect().await?;

    let source_dsn = src_spec.url().to_string();
    let tables = src_spec.tables().to_vec();

    // Step 5: launch dashboard.
    let mut dashboard = QuickstartDashboard::new(
        display_name,
        dbmazz,
        target,
        tables,
        source_dsn,
        Some(compose_path.clone()),
    );
    dashboard.run().await?;

    // Step 6: stop dbmazz.
    print_step("Stopping dbmazz...");
    let _ = runner::down(&compose_path, false, &["dbmazz"]);

    Ok(())
}
