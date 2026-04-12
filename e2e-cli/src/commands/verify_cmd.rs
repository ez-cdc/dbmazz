use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use console::style;

use crate::clients::dbmazz::DbmazzClient;
use crate::commands::{dbmazz_image, ensure_dbmazz_image, load_store, resolve_pair};
use crate::compose::{builder, runner};
use crate::preflight;
use crate::tui::report::{print_report_summary, print_step, print_tier_header, report_to_json};
use crate::verify::runner::VerifyRunner;

pub async fn run_verify(
    config_path: &Path,
    source: Option<String>,
    sink: Option<String>,
    _all: bool,
    quick: bool,
    skip: Option<String>,
    json_report: Option<std::path::PathBuf>,
    no_up: bool,
    _keep_up: bool,
    rebuild: bool,
) -> anyhow::Result<()> {
    let mut store = load_store(config_path)?;
    let (src_name, src_spec, sk_name, sk_spec) = resolve_pair(&mut store, source, sink)?;
    let data = store.data()?.clone();

    println!(
        "  {} {} {} {}",
        style("Verify:").bold(),
        style(&src_name).cyan(),
        style("→").dim(),
        style(&sk_name).cyan(),
    );
    println!();

    let skip_ids: HashSet<String> = skip
        .map(|s| s.split(',').map(|id| id.trim().to_string()).collect())
        .unwrap_or_default();

    // Step 1: validate that source and sink are reachable.
    print_step("Testing connectivity...");
    if !preflight::check_connectivity(&src_spec, &sk_spec, &src_name, &sk_name).await {
        anyhow::bail!(
            "source or sink unreachable. Make sure your databases are running \
             (use `ez-cdc up` for Docker-managed ones) and check the connection URLs."
        );
    }

    // Step 2: ensure the dbmazz Docker image exists, then start the container.
    let compose_path = if !no_up {
        // Ensure the official dbmazz image is available locally.
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
            false,  // never build here — image was ensured above
            true,   // force_recreate — fresh daemon each verify
            &["dbmazz"],
        )
        .map_err(|e| anyhow::anyhow!("failed to start dbmazz: {e}"))?;

        println!(
            "    {} logs: {}",
            style("→").dim(),
            style("ez-cdc logs dbmazz").cyan(),
        );

        // Wait for healthy via HTTP.
        print_step("Waiting for dbmazz to become healthy...");
        let client = DbmazzClient::with_defaults("http://localhost:8080");
        client
            .wait_healthy(Duration::from_secs(60), Duration::from_secs(2))
            .await
            .map_err(|e| anyhow::anyhow!("dbmazz did not become healthy: {e}"))?;

        Some(compose_path)
    } else {
        None
    };

    // Step 3: run verify suite.
    print_tier_header(&format!("Verify: {} → {}", src_name, sk_name));

    let mut verify_runner = VerifyRunner::new(
        src_name.clone(), src_spec, sk_name.clone(), sk_spec, quick, skip_ids,
    );
    let report = verify_runner.run().await;

    print_report_summary(&report);

    if let Some(json_path) = json_report {
        let json = report_to_json(&report);
        let json_str = serde_json::to_string_pretty(&json)?;
        std::fs::write(&json_path, &json_str)?;
        println!("  {} JSON report written to {}", style("✓").dim(), json_path.display());
    }

    // Step 4: stop dbmazz container.
    if let Some(ref cp) = compose_path {
        print_step("Stopping dbmazz...");
        let _ = runner::down(cp, false, &["dbmazz"]);
    }

    if !report.ok() {
        anyhow::bail!("verification failed: {} checks failed", report.total_failed());
    }

    Ok(())
}
