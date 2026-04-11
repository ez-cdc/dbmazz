use std::path::Path;

use console::style;

use crate::commands::{load_store, resolve_pair};
use crate::config::schema::SourceSpec;
use crate::instantiate::{instantiate_backend, instantiate_source};
use crate::tui::prompts;
use crate::tui::report::print_step;

pub async fn run_clean(
    config_path: &Path,
    source: Option<String>,
    sink: Option<String>,
    yes: bool,
) -> anyhow::Result<()> {
    let mut store = load_store(config_path)?;
    let (src_name, src_spec, sk_name, sk_spec) = resolve_pair(&mut store, source, sink)?;

    let tables = src_spec.tables().to_vec();

    println!(
        "  {} {} {} {}",
        style("Clean:").bold(),
        style(&src_name).cyan(),
        style("→").dim(),
        style(&sk_name).cyan(),
    );
    println!("  Tables: {}", tables.join(", "));
    println!();

    if !yes {
        if !prompts::is_interactive() {
            anyhow::bail!("use --yes to skip confirmation in non-interactive mode");
        }
        let confirmed = prompts::confirm(
            "This will clean the target AND drop the source replication slot. Continue?",
            false,
        )?;
        if !confirmed {
            println!("  Cancelled.");
            return Ok(());
        }
    }

    // Clean target.
    print_step(&format!("Connecting to target '{}'...", sk_name));
    let mut target = instantiate_backend(&sk_spec)?;
    target.connect().await?;

    print_step("Cleaning target...");
    let actions = target.clean(&tables).await?;
    let _ = target.close().await;

    for action in &actions {
        println!("    {} {}", style("-").dim(), action);
    }

    // Clean source replication slot + publication so dbmazz starts fresh.
    let (slot_name, pub_name) = match &src_spec {
        SourceSpec::Postgres(inner) => (
            inner.replication_slot.clone(),
            inner.publication.clone(),
        ),
    };

    print_step(&format!("Dropping replication slot '{}' from source...", slot_name));
    match drop_source_slot(&src_spec, &slot_name, &pub_name).await {
        Ok(slot_actions) => {
            for action in &slot_actions {
                println!("    {} {}", style("-").dim(), action);
            }
        }
        Err(e) => {
            // Non-fatal — slot may not exist if dbmazz never ran.
            println!(
                "    {} {}",
                style("~").dim(),
                style(format!("slot cleanup skipped: {e}")).dim(),
            );
        }
    }

    let total = actions.len();
    println!();
    println!(
        "  {} Cleaned {} artifacts from '{}'.",
        style("✓").green().bold(),
        total,
        sk_name,
    );
    println!();
    Ok(())
}

/// Drop the replication slot and publication from the source PG.
/// This ensures the next dbmazz run starts fresh (new snapshot, no stale LSN).
async fn drop_source_slot(
    src_spec: &SourceSpec,
    slot_name: &str,
    pub_name: &str,
) -> anyhow::Result<Vec<String>> {
    let mut source = instantiate_source(src_spec)?;
    source.connect().await?;

    let mut actions = Vec::new();

    // Use the raw client to run DDL.
    // The SourceClient trait doesn't expose slot management, so we use
    // a separate connection for this.
    let url = src_spec.url();
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move { let _ = connection.await; });

    // Drop replication slot (if exists).
    let drop_slot = format!(
        "SELECT pg_drop_replication_slot(slot_name) \
         FROM pg_replication_slots WHERE slot_name = '{}'",
        slot_name.replace('\'', "''")
    );
    match client.execute(&drop_slot, &[]).await {
        Ok(n) => {
            if n > 0 {
                actions.push(format!("dropped replication slot '{slot_name}'"));
            } else {
                actions.push(format!("replication slot '{slot_name}' not found (ok)"));
            }
        }
        Err(e) => actions.push(format!("slot drop: {e}")),
    }

    // Drop publication (if exists).
    let drop_pub = format!(
        "DROP PUBLICATION IF EXISTS \"{}\"",
        pub_name.replace('"', "\"\"")
    );
    match client.execute(&drop_pub, &[]).await {
        Ok(_) => actions.push(format!("dropped publication '{pub_name}'")),
        Err(e) => actions.push(format!("publication drop: {e}")),
    }

    // Also clean dbmazz checkpoint + snapshot state tables.
    for tbl in &["dbmazz_checkpoints", "dbmazz_snapshot_state"] {
        let sql = format!("DROP TABLE IF EXISTS {tbl}");
        if client.execute(&sql, &[]).await.is_ok() {
            actions.push(format!("dropped {tbl}"));
        }
    }

    let _ = source.close().await;
    Ok(actions)
}
