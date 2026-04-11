use std::time::Duration;

use comfy_table::{Cell, Color, Table};
use console::style;

use crate::clients::dbmazz::DbmazzClient;

pub async fn run_status() -> anyhow::Result<()> {
    let client = DbmazzClient::new("http://localhost:8080", Duration::from_secs(5));

    if !client.health().await {
        println!(
            "  {} dbmazz daemon is not reachable at {}",
            style("\u{2717}").red().bold(),
            style("http://localhost:8080").dim()
        );
        println!();
        println!(
            "  Is the daemon running? Try {} first.",
            style("ez-cdc up").bold()
        );
        return Ok(());
    }

    let status = client.status().await?;

    // Format uptime.
    let uptime = if status.uptime_secs >= 3600 {
        format!(
            "{}h {}m {}s",
            status.uptime_secs / 3600,
            (status.uptime_secs % 3600) / 60,
            status.uptime_secs % 60
        )
    } else if status.uptime_secs >= 60 {
        format!(
            "{}m {}s",
            status.uptime_secs / 60,
            status.uptime_secs % 60
        )
    } else {
        format!("{}s", status.uptime_secs)
    };

    // Main status table.
    let mut table = Table::new();
    table.set_header(vec![
        Cell::new("Field").fg(Color::Cyan),
        Cell::new("Value").fg(Color::Cyan),
    ]);

    table.add_row(vec!["Stage", &status.stage]);
    table.add_row(vec!["Uptime", &uptime]);
    table.add_row(vec!["Confirmed LSN", &status.confirmed_lsn]);
    table.add_row(vec!["Current LSN", &status.current_lsn]);
    table.add_row(vec![
        "Events total",
        &format_number(status.events_total),
    ]);
    table.add_row(vec![
        "Events/sec",
        &format!("{:.1}", status.events_per_sec),
    ]);
    table.add_row(vec![
        "Batches sent",
        &format_number(status.batches_sent),
    ]);
    table.add_row(vec![
        "Replication lag",
        &format!("{}ms", status.replication_lag_ms),
    ]);
    table.add_row(vec![
        "CPU",
        &format!("{}m", status.cpu_millicores),
    ]);
    table.add_row(vec![
        "Memory (RSS)",
        &format!("{:.1} MB", status.memory_rss_mb),
    ]);

    println!();
    println!("{table}");

    // Snapshot progress if active.
    if status.snapshot_active || status.snapshot_chunks_total > 0 {
        println!();
        let mut snap_table = Table::new();
        snap_table.set_header(vec![
            Cell::new("Snapshot").fg(Color::Cyan),
            Cell::new("Value").fg(Color::Cyan),
        ]);
        snap_table.add_row(vec![
            "Active",
            if status.snapshot_active { "yes" } else { "no" },
        ]);
        snap_table.add_row(vec![
            "Progress",
            &format!(
                "{}/{} chunks",
                status.snapshot_chunks_done, status.snapshot_chunks_total
            ),
        ]);
        snap_table.add_row(vec![
            "Rows synced",
            &format_number(status.snapshot_rows_synced),
        ]);
        if status.snapshot_chunks_total > 0 {
            let pct = (status.snapshot_chunks_done as f64
                / status.snapshot_chunks_total as f64)
                * 100.0;
            snap_table.add_row(vec![
                "Completion",
                &format!("{pct:.1}%"),
            ]);
        }
        println!("{snap_table}");
    }

    // Error if present.
    if let Some(ref err) = status.error_detail {
        println!();
        println!(
            "  {} Error: {}",
            style("\u{2717}").red().bold(),
            style(err).red()
        );
    }

    println!();
    Ok(())
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}
